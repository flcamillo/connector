package main

import (
	"awsconnector/internal"
	"awsconnector/internal/repository"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Define a configuração do Worker.
type WorkerConfig struct {
	// Client AWS SQS para executar operações na fila.
	SqsService internal.SqsService
	// Client AWS S3 para executar operações no bucket.
	S3Service internal.S3Service
	// URL da fila AWS SQS para enviar mensagens.
	QueueUrl string
	// Canal para as menagens recebidas para processamento.
	MessageChan chan *internal.MessageContext
	// Repositório para acessar os dados dos agendamentos.
	UserRepository repository.Client
	// Repositório para acessar os eventos de execução do conector.
	EventRepository repository.Event
	// Total maximo de comandos que podem ser executados em paralelo no
	// conector.
	MaxCommands int
}

// Define a estrutura para o processo responsável por processar as mensagens
// recebidas da fila AWS SQS.
type Worker struct {
	// Configuração gerais.
	config *WorkerConfig
	// Tracer para criar spans de telemetria durante o processo.
	tracer trace.Tracer
	// Canal para sinalizar o encerramento do processo.
	stopChan chan int
	// metricas de requisições
	messageProcessed     metric.Int64Counter
	messageFailed        metric.Int64Counter
	messageWaitHistogram metric.Float64Histogram
}

// Construtor para criar uma nova instância do Worker
func NewWorker(config *WorkerConfig) *Worker {
	worker := &Worker{
		tracer:   otel.Tracer("Worker"),
		config:   config,
		stopChan: make(chan int),
	}
	// configura as metricas
	meter := otel.Meter("Worker.metrics")
	if counter, err := meter.Int64Counter("custom.worker.messages.processed",
		metric.WithDescription("The number of messages processed sucessfully"),
		metric.WithUnit("{messages}")); err == nil {
		worker.messageProcessed = counter
	} else {
		panic(err)
	}
	if counter, err := meter.Int64Counter("custom.worker.messages.failed",
		metric.WithDescription("The number of messages processed with failue"),
		metric.WithUnit("{messages}")); err == nil {
		worker.messageFailed = counter
	} else {
		panic(err)
	}
	if histogram, err := meter.Float64Histogram("custom.worker.messages.wait.duration",
		metric.WithDescription("Message waiting time before being processed by worker."),
		metric.WithUnit("s")); err == nil {
		worker.messageWaitHistogram = histogram
	} else {
		panic(err)
	}
	return worker
}

// Método para sinalizar o encerramento do processo de consumo de mensagens da fila AWS SQS
func (p *Worker) Stop(ctx context.Context) {
	slog.InfoContext(ctx, "stopping Worker...")
	p.stopChan <- 0
}

// Método para iniciar o processo de consumo de mensagens da fila AWS SQS.
func (p *Worker) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "stopping Worker due to context cancellation...")
			return
		case <-p.stopChan:
			return
		default:
			messageContext, ok := <-p.config.MessageChan
			if !ok {
				return
			}
			retry, err := p.processMessage(messageContext.Context, messageContext)
			if err != nil {
				slog.ErrorContext(messageContext.Context, "failed to process message",
					slog.String("message_id", messageContext.Id),
					slog.String("message_body", messageContext.Body),
					slog.Any("error", err),
				)
			}
			// deve atualizar as metricas e remover a mensagem caso a mesma tenha
			// sido processada com sucesso ou caso tenha ocorrido um erro que
			// não seja passível de retry
			if !retry {
				if err != nil {
					p.messageFailed.Add(messageContext.Context, 1)
				} else {
					p.messageProcessed.Add(messageContext.Context, 1)
				}
				p.messageWaitHistogram.Record(messageContext.Context, time.Since(messageContext.Received).Seconds())
				_, err := p.config.SqsService.DeleteMessage(messageContext.Context, &sqs.DeleteMessageInput{
					QueueUrl:      &messageContext.QueueUrl,
					ReceiptHandle: &messageContext.ReceiptHandle,
				})
				if err != nil {
					slog.ErrorContext(messageContext.Context, "failed to delete message",
						slog.String("message_id", messageContext.Id),
						slog.String("message_body", messageContext.Body),
						slog.Any("error", err),
					)
				}
			}
		}
	}
}

// Método para identificar o tipo da mensagem recebida e processá-la de acordo.
// Retorna um booleano indicando se a mensagem deve ser reprocessada.
func (p *Worker) processMessage(ctx context.Context, messageContext *internal.MessageContext) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "processMessage")
	defer span.End()
	// decodifica o corpo da mensagem para a estrutura do evento de agendamento
	schedulerEvent := internal.SchedulerEvent{}
	err = json.Unmarshal([]byte(messageContext.Body), &schedulerEvent)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal message body")
		return false, err
	}
	err = schedulerEvent.Validate()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to validate scheduler event")
		return false, err
	}
	slog.InfoContext(ctx, "command received",
		slog.String("command", schedulerEvent.Command),
		slog.String("mft", schedulerEvent.Mft),
	)
	// identifica o tipo da mensagem e processa de acordo
	switch schedulerEvent.Command {
	case internal.SchedulerEventCommandStart:
		retry, err = p.processStartEvent(ctx, schedulerEvent)
	default:
		slog.WarnContext(ctx, "received unknown command",
			slog.String("command", schedulerEvent.Command),
		)
		return false, nil
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to process command")
		slog.ErrorContext(ctx, "failed to process command",
			slog.String("command", schedulerEvent.Command),
			slog.Any("error", err),
		)
	}
	return retry, err
}

// Método para processar mensagens do tipo "start". Retorna um booleano
// indicando se a mensagem deve ser reprocessada.
func (p *Worker) processStartEvent(ctx context.Context, event internal.SchedulerEvent) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "processStartEvent")
	defer span.End()
	// deve selecionar apenas usuários ativos para inciar a agenda
	activeUsers, err := p.config.UserRepository.FindByMft(ctx, event.Mft, repository.UserStatusActive)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get users from repository by mft and status")
		slog.ErrorContext(ctx, "failed to get users from repository by mft and status",
			slog.String("mft", event.Mft),
			slog.String("status", repository.UserStatusActive),
			slog.Any("error", err),
		)
		return true, err
	}
	span.AddEvent(fmt.Sprintf("found {%d} users for mft {%s}", len(activeUsers), event.Mft))
	// da lista de usuários ativos deve agora selecionar apenas aqueles que
	// podem executar
	var readyForExecution []*repository.UserRecord
	for _, user := range activeUsers {
		ready, err := p.canExecute(ctx, user)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to check if user could execute")
			slog.ErrorContext(ctx, "failed to check if user could execute",
				slog.Any("user", user),
				slog.Any("error", err),
			)
			return true, err
		}
		if ready {
			readyForExecution = append(readyForExecution, user)
		}
	}
	span.AddEvent(fmt.Sprintf("found {%d} users read for execution for mft {%s}", len(readyForExecution), event.Mft))
	// deve executar os processos de recepção ou de envio de arquivos para
	// cada usuário que esta pronto para execução e atualizar a relação
	// de conectores que serão utilizados
	connectorsForExecution := make(map[string]int, 0)
	for _, user := range readyForExecution {
		if user.DownloadEnabled {
			err = p.executeDownload(ctx, user)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "failed to schedule file download")
				slog.ErrorContext(ctx, "failed to schedule file download",
					slog.Any("user", user),
					slog.Any("error", err),
				)
				return true, err
			}
			connectorsForExecution[user.Connector]++
		}
		if user.UploadEnabled {
			err = p.executeUpload(ctx, user)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "failed to schedule file upload")
				slog.ErrorContext(ctx, "failed to schedule file upload",
					slog.Any("user", user),
					slog.Any("error", err),
				)
				return true, err
			}
			connectorsForExecution[user.Connector]++
		}
	}
	// seleciona os comandos que devem ser inicializados de imediato pelo conector
	// respeitando os limites de execução simultanea
	for connector := range connectorsForExecution {
		err = p.executeNextCommand(ctx, connector)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to schedule next command")
			slog.ErrorContext(ctx, "failed to schedule next command",
				slog.String("connector", connector),
				slog.Any("error", err),
			)
			return true, err
		}
	}
	return false, nil
}

// Verifica se o conector possui algum evento em execução e caso não exista
// o considera apto para executar.
func (p *Worker) canExecute(ctx context.Context, user *repository.UserRecord) (bool, error) {
	if !user.DownloadEnabled && !user.UploadEnabled {
		return false, nil
	}
	events, err := p.config.EventRepository.FindByConnectorAndCreated(ctx, user.Connector, time.Now().Add(-time.Duration(user.StartInterval)*time.Minute), time.Now())
	if err != nil {
		return false, err
	}
	for _, event := range events {
		if user.Status == internal.StatusCommandExecuting || event.Status == internal.StatusCommandNotStarted {
			return false, nil
		}
	}
	return true, nil
}

// Envia o evento para o conector.
func (p *Worker) sendEvent(ctx context.Context, event *repository.EventRecord) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = p.config.SqsService.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &p.config.QueueUrl,
		MessageBody: aws.String(string(data)),
	})
	if err != nil {
		return err
	}
	return nil
}

// Envia o comando de listagem do diretório remoto para o conector executar.
func (p *Worker) executeDownload(ctx context.Context, user *repository.UserRecord) error {
	ctx, span := p.tracer.Start(ctx, "executeDownload")
	defer span.End()
	span.SetAttributes(
		attribute.String("event.mft", user.Mft),
		attribute.String("event.mailbox", user.Mailbox),
		attribute.String("event.product", user.Product),
		attribute.String("event.connector", user.Connector),
	)
	slog.InfoContext(ctx, "scheduling a listing of the remote directory",
		slog.String("mft", user.Mft),
		slog.String("mailbox", user.Mailbox),
		slog.String("product", user.Product),
		slog.String("connector", user.Connector),
	)
	event := &repository.EventRecord{
		Mft:                 user.Mft,
		Mailbox:             user.Mailbox,
		Product:             user.Product,
		Connector:           user.Connector,
		LocalPath:           user.LocalPathForDownload,
		RemotePath:          user.RemotePathForDownload,
		FileNameFilter:      user.FileNameFilter,
		RemoveAfterDownload: user.RemoveAfterDownload,
		Command:             internal.ConnectorCommandList,
		Status:              internal.StatusCommandNotStarted,
		ConnectorEventID:    "NA",
	}
	err := p.config.EventRepository.Save(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed save event on repository")
		slog.ErrorContext(ctx, "failed save event on repository",
			slog.Any("event", event),
			slog.Any("error", err),
		)
		return err
	}
	return nil
}

// Lista os arquivos locais e envia para o conector executar.
func (p *Worker) executeUpload(ctx context.Context, user *repository.UserRecord) error {
	ctx, span := p.tracer.Start(ctx, "executeUpload")
	defer span.End()
	span.SetAttributes(
		attribute.String("event.mft", user.Mft),
		attribute.String("event.mailbox", user.Mailbox),
		attribute.String("event.product", user.Product),
		attribute.String("event.connector", user.Connector),
	)
	slog.InfoContext(ctx, "scheduling files to be sent",
		slog.String("mft", user.Mft),
		slog.String("mailbox", user.Mailbox),
		slog.String("product", user.Product),
		slog.String("connector", user.Connector),
	)
	// deve separar da url o bucket e prefix para listar os arquivo
	// o caminho do arquivo deve estar no formato de url (s3://bucket/prefix/)
	_, bucket, prefix, _ := internal.SplitUrlPath(user.LocalPathForUpload)
	if bucket == "" {
		err := fmt.Errorf("missing bucket name")
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to execute upload")
		slog.ErrorContext(ctx, "failed to execute upload",
			slog.String("local_path", user.LocalPathForUpload),
			slog.Any("error", err),
		)
		return err
	}
	// deve identificar todos os arquivos do bucket local para solicitar o envio
	// respeitando a quantidade maxima de arquivos que serão enviados em paralelo
	sends := 0
	maxKeys := int32(1000)
	paginator := s3.NewListObjectsV2Paginator(p.config.S3Service, &s3.ListObjectsV2Input{
		Bucket:  &bucket,
		Prefix:  &prefix,
		MaxKeys: aws.Int32(maxKeys),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to read next page from bucket")
			slog.ErrorContext(ctx, "failed to read next page from bucket",
				slog.String("bucket", bucket),
				slog.String("prefix", prefix),
				slog.Int("max_keys", int(maxKeys)),
				slog.Any("error", err),
			)
			return err
		}
		for _, object := range page.Contents {
			// ignora se o objeto for uma pasta
			if strings.HasSuffix(*object.Key, "/") {
				continue
			}
			event := &repository.EventRecord{
				Mft:                 user.Mft,
				Mailbox:             user.Mailbox,
				Product:             user.Product,
				Connector:           user.Connector,
				LocalPath:           fmt.Sprintf("s3://%s/%s", bucket, *object.Key),
				RemotePath:          user.RemotePathForUpload,
				FileNameFilter:      user.FileNameFilter,
				FileSize:            *object.Size,
				RemoveAfterDownload: user.RemoveAfterDownload,
				Command:             internal.ConnectorCommandUpload,
				Status:              internal.StatusCommandNotStarted,
				ConnectorEventID:    "NA",
			}
			err := p.config.EventRepository.Save(ctx, event)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "failed save event on repository")
				slog.ErrorContext(ctx, "failed save event on repository",
					slog.Any("event", event),
					slog.Any("error", err),
				)
				return err
			}
			sends++
		}
	}
	slog.InfoContext(ctx, "file scheduling for sending completed",
		slog.String("mft", user.Mft),
		slog.String("mailbox", user.Mailbox),
		slog.String("product", user.Product),
		slog.String("connector", user.Connector),
		slog.Int("scheduled_files", sends),
	)
	return nil
}

// Agenda a execução do proximo comando desde que não tenha atingido a
// quantidade maxima de comandos enfileirados no conector.
func (p *Worker) executeNextCommand(ctx context.Context, connector string) error {
	ctx, span := p.tracer.Start(ctx, "executeNextCommand")
	defer span.End()
	events, err := p.config.EventRepository.FindByConnectorAndStatus(ctx, connector, internal.StatusCommandNotStarted)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get event from repository by connector and status")
		slog.ErrorContext(ctx, "failed to get event from repository by connector and status",
			slog.String("connector", connector),
			slog.String("status", internal.StatusCommandNotStarted),
			slog.Any("error", err),
		)
		return err
	}
	if events == nil {
		return nil
	}
	for _, pending := range events {
		// antes de colocar o comando em execução deve validar
		// se não atingiu o maximo de comandos que podem ser
		// enfileirados
		queuedCommands, err := p.queuedCommands(ctx, connector)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to count the number of commands running for the connector")
			slog.ErrorContext(ctx, "failed to count the number of commands running for the connector",
				slog.String("connector", connector),
				slog.Any("error", err),
			)
			return err
		}
		if queuedCommands >= p.config.MaxCommands {
			break
		}
		pending.Status = internal.StatusCommandExecuting
		saved, err := p.config.EventRepository.SaveIfStatusIs(ctx, pending, internal.StatusCommandNotStarted)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed save event on repository by status")
			slog.ErrorContext(ctx, "failed save event on repository by status",
				slog.String("status", internal.StatusCommandNotStarted),
				slog.Any("event", pending),
				slog.Any("error", err),
			)
			return err
		}
		// ignora o registro caso ele não seja salvo pois algum outro
		// processo pode ter enfileirado o registro
		if !saved {
			continue
		}
		err = p.sendEvent(ctx, pending)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed send event to connector")
			slog.ErrorContext(ctx, "failed send event to connector",
				slog.Any("event", pending),
				slog.Any("error", err),
			)
			// se por algum motivo não for possível enfileirar o evento
			// então ajusta o status no repositório
			pending.Status = internal.StatusCommandNotStarted
			err2 := p.config.EventRepository.Save(ctx, pending)
			if err2 != nil {
				slog.ErrorContext(ctx, "failed save event on repository",
					slog.Any("event", pending),
					slog.Any("error", err2),
				)
				err = errors.Join(err, err2)
			}
			return err
		}
	}
	return nil
}

// Consulta a quantidade maxima de comandos enfileirados no conector.
func (p *Worker) queuedCommands(ctx context.Context, connector string) (queued int, err error) {
	notStarted, err := p.config.EventRepository.FindByConnectorAndStatus(ctx, connector, internal.StatusCommandNotStarted)
	if err != nil {
		return 0, err
	}
	executing, err := p.config.EventRepository.FindByConnectorAndStatus(ctx, connector, internal.StatusCommandExecuting)
	if err != nil {
		return 0, err
	}
	return len(notStarted) + len(executing), nil
}
