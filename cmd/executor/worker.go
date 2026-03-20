package main

import (
	"awsconnector/internal"
	"awsconnector/internal/repository"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/transfer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Define a configuração do Worker.
type WorkerConfig struct {
	// Client AWS Transfer para executar operações no conector.
	TransferService internal.TransferService
	// Client AWS SQS para executar operações na fila.
	SqsService internal.SqsService
	// Client AWS S3 para executar operações no bucket.
	S3Service internal.S3Service
	// Canal para as menagens recebidas para processamento.
	MessageChan chan *internal.MessageContext
	// Repositório para acessar os eventos de execução do conector.
	EventRepository repository.Event
	// URL do bucket para arquivos temporário.
	TemporaryFilePath string
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
	// decodifica o corpo da mensagem para a estrutura do registro do repositório
	event := &repository.EventRecord{}
	err = json.Unmarshal([]byte(messageContext.Body), &event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal message body")
		return false, err
	}
	err = event.Validate()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to validate event")
		return false, err
	}
	span.SetAttributes(
		attribute.String("event.mft", event.Mft),
		attribute.String("event.mailbox", event.Mailbox),
		attribute.String("event.product", event.Product),
		attribute.String("event.connector", event.Connector),
	)
	slog.InfoContext(ctx, "command received",
		slog.String("command", event.Command),
		slog.String("mft", event.Mft),
		slog.String("mailbox", event.Mailbox),
		slog.String("product", event.Product),
		slog.String("connector", event.Connector),
	)
	// identifica o tipo da mensagem e processa de acordo
	switch event.Command {
	case internal.ConnectorCommandDelete:
		retry, err = p.processDeleteCommand(ctx, event)
	case internal.ConnectorCommandList:
		retry, err = p.processListCommand(ctx, event)
	case internal.ConnectorCommandUpload:
		retry, err = p.processUploadCommand(ctx, event)
	case internal.ConnectorCommandDownload:
		retry, err = p.processDownloadCommand(ctx, event)
	default:
		slog.WarnContext(ctx, "received unknown command",
			slog.String("command", event.Command),
		)
		return false, nil
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to process command")
		slog.WarnContext(ctx, "failed to process command",
			slog.String("command", event.Command),
			slog.Any("error", err),
		)
	}
	return retry, err
}

// Processa o comando para apagar arquivos no servidor remoto.
func (p *Worker) processDeleteCommand(ctx context.Context, event *repository.EventRecord) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "processDeleteCommand")
	defer span.End()
	slog.InfoContext(ctx, "processing remote delete",
		slog.String("mft", event.Mft),
		slog.String("mailbox", event.Mailbox),
		slog.String("product", event.Product),
		slog.String("connector", event.Connector),
		slog.String("delete_path", event.RemotePath),
	)
	// o comando de delete suporta a exclusão de apenas um arquivo por vez
	// dessa forma, cada arquivo que precisar ser removido precisa de
	// um evento no conector
	output, err := p.config.TransferService.StartRemoteDelete(ctx, &transfer.StartRemoteDeleteInput{
		ConnectorId: &event.Connector,
		DeletePath:  &event.RemotePath,
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to start remote delete")
		slog.ErrorContext(ctx, "failed to start remote delete",
			slog.String("connector", event.Connector),
			slog.String("delete_path", event.RemotePath),
			slog.Any("error", err),
		)
		event.ReturnCode = 1
		event.ReturnMessage = err.Error()
		event.Status = internal.StatusCommandCompleted
		err2 := p.config.EventRepository.Save(ctx, event)
		if err2 != nil {
			slog.ErrorContext(ctx, "failed save event on repository",
				slog.Any("event", event),
				slog.Any("error", err2),
			)
		}
		return true, err
	}
	event.ConnectorEventID = *output.DeleteId
	event.Status = internal.StatusCommandExecuting
	err = p.config.EventRepository.Save(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed save event on repository")
		slog.ErrorContext(ctx, "failed save event on repository",
			slog.Any("event", event),
			slog.Any("error", err),
		)
		return true, err
	}
	return false, nil
}

// Processa o comando para listar os arquivos existentes no servidor remoto.
func (p *Worker) processListCommand(ctx context.Context, event *repository.EventRecord) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "processListCommand")
	defer span.End()
	slog.InfoContext(ctx, "processing directory listing",
		slog.String("mft", event.Mft),
		slog.String("mailbox", event.Mailbox),
		slog.String("product", event.Product),
		slog.String("connector", event.Connector),
		slog.String("remote_path", event.RemotePath),
	)
	// constrói o caminho do arquivo temporário no bucket
	// é esperado que o caminho local esteja no formato: s3://bucket/prefix1/prefix2/
	// o caminho correto para o conector gerar a lista de arquivos encontrados
	// no servidor remoto é: bucket/prefix1/prefix2/
	_, bucket, prefix, _ := internal.SplitUrlPath(p.config.TemporaryFilePath)
	if bucket == "" {
		err = fmt.Errorf("missing bucket name")
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to start directory listing")
		slog.ErrorContext(ctx, "failed to start directory listing",
			slog.String("temporary_path", p.config.TemporaryFilePath),
			slog.Any("error", err),
		)
		return false, err
	}
	if prefix != "" {
		prefix = fmt.Sprintf("/%s", prefix)
	}
	// deve converter as variaveis especiais de ambos os caminhos
	remotePath := internal.ParseVariables(event.RemotePath, "")
	temporaryPath := internal.ParseVariables(fmt.Sprintf("/%s%s", bucket, prefix), "")
	// solicita no comando de listagem com o maximo de arquivos suportados
	// o conector irá gerar um JSON com a relação de arquivos no bucket
	// temporário, e o arquivo terá como nome: <connector_id>-<listing_id>.json
	maxItens := int32(10000)
	output, err := p.config.TransferService.StartDirectoryListing(ctx, &transfer.StartDirectoryListingInput{
		ConnectorId:         &event.Connector,
		RemoteDirectoryPath: &remotePath,
		OutputDirectoryPath: &temporaryPath,
		MaxItems:            aws.Int32(maxItens),
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to start directory listing")
		slog.ErrorContext(ctx, "failed to start directory listing",
			slog.String("connector", event.Connector),
			slog.String("remote_path", remotePath),
			slog.String("temporary_path", temporaryPath),
			slog.Int("max_itens", int(maxItens)),
			slog.Any("error", err),
		)
		event.ReturnCode = 1
		event.ReturnMessage = err.Error()
		event.Status = internal.StatusCommandCompleted
		err2 := p.config.EventRepository.Save(ctx, event)
		if err2 != nil {
			slog.ErrorContext(ctx, "failed save event on repository",
				slog.Any("event", event),
				slog.Any("error", err2),
			)
		}
		return true, err
	}
	event.ConnectorEventID = *output.ListingId
	event.Status = internal.StatusCommandExecuting
	err = p.config.EventRepository.Save(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed save event on repository")
		slog.ErrorContext(ctx, "failed save event on repository",
			slog.Any("event", event),
			slog.Any("error", err),
		)
		return true, err
	}
	return false, nil
}

// Processa o comando para enviar um arquivo para o servidor remoto.
func (p *Worker) processUploadCommand(ctx context.Context, event *repository.EventRecord) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "processUploadCommand")
	defer span.End()
	slog.InfoContext(ctx, "processing upload",
		slog.String("mft", event.Mft),
		slog.String("mailbox", event.Mailbox),
		slog.String("product", event.Product),
		slog.String("connector", event.Connector),
		slog.String("local_path", event.LocalPath),
		slog.String("remote_path", event.RemotePath),
	)
	// constrói o caminho do arquivo do arquivo a ser enviado
	// é esperado que o caminho local esteja no formato:
	// s3://bucket/prefix1/prefix2/file.txt
	// o caminho correto para o conector receber o arquivo para do servidor
	// remoto é: bucket/prefix1/prefix2/file.txt
	_, bucket, prefix, file := internal.SplitUrlPath(event.LocalPath)
	if bucket == "" {
		err = fmt.Errorf("missing bucket name")
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to start upload file")
		slog.ErrorContext(ctx, "failed to start upload file",
			slog.String("local_path", event.LocalPath),
			slog.Any("error", err),
		)
		return false, err
	}
	if prefix != "" {
		prefix = fmt.Sprintf("/%s", prefix)
	}
	if file == "" {
		err = fmt.Errorf("missing file name")
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to start upload file")
		slog.ErrorContext(ctx, "failed to start upload file",
			slog.String("local_path", event.LocalPath),
			slog.Any("error", err),
		)
		return false, err
	}
	// deve converter as variaveis especiais apenas do caminho remoto
	// pois o caminho local é o caminho real do arquivo a ser enviado
	localPath := []string{fmt.Sprintf("/%s%s/%s", bucket, prefix, file)}
	remotePath := internal.ParseVariables(event.RemotePath, file)
	// o conector suporta o envio de mais de um arquivo por vez, porém
	// será feito o envio individual para facilitar o controle do
	// paralelismo
	output, err := p.config.TransferService.StartFileTransfer(ctx, &transfer.StartFileTransferInput{
		ConnectorId:         &event.Connector,
		RemoteDirectoryPath: &remotePath,
		SendFilePaths:       localPath,
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to start file transfer")
		slog.ErrorContext(ctx, "failed to start file transfer",
			slog.String("connector", event.Connector),
			slog.String("remote_path", remotePath),
			slog.Any("local_paths", localPath),
			slog.Any("error", err),
		)
		event.ReturnCode = 1
		event.ReturnMessage = err.Error()
		event.Status = internal.StatusCommandCompleted
		err2 := p.config.EventRepository.Save(ctx, event)
		if err2 != nil {
			slog.ErrorContext(ctx, "failed save event on repository",
				slog.Any("event", event),
				slog.Any("error", err2),
			)
		}
		return true, err
	}
	event.ConnectorEventID = *output.TransferId
	event.Status = internal.StatusCommandExecuting
	err = p.config.EventRepository.Save(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed save event on repository")
		slog.ErrorContext(ctx, "failed save event on repository",
			slog.Any("event", event),
			slog.Any("error", err),
		)
		return true, err
	}
	return false, nil
}

// Processa o comando para receber um arquivo do servidor remoto.
func (p *Worker) processDownloadCommand(ctx context.Context, event *repository.EventRecord) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "processDownloadCommand")
	defer span.End()
	slog.InfoContext(ctx, "processing download",
		slog.String("mft", event.Mft),
		slog.String("mailbox", event.Mailbox),
		slog.String("product", event.Product),
		slog.String("connector", event.Connector),
		slog.String("local_path", event.LocalPath),
		slog.String("remote_path", event.RemotePath),
	)
	// constrói o caminho do arquivo do arquivo a ser recepcionado
	// é esperado que o caminho local esteja no formato:
	// s3://bucket/prefix1/prefix2/file.txt
	// o caminho correto para o conector receber o arquivo para do servidor
	// remoto é: bucket/prefix1/prefix2/file.txt
	_, bucket, prefix, _ := internal.SplitUrlPath(event.LocalPath)
	if bucket == "" {
		err = fmt.Errorf("missing bucket name")
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to start download file")
		slog.ErrorContext(ctx, "failed to start download file",
			slog.String("local_path", event.LocalPath),
			slog.Any("error", err),
		)
		return false, err
	}
	if prefix != "" {
		prefix = fmt.Sprintf("/%s", prefix)
	}
	// deve converter as variaveis especiais apenas do caminho local
	// pois o caminho remoto é o caminho real do arquivo a ser enviado
	localPath := internal.ParseVariables(fmt.Sprintf("/%s%s", bucket, prefix), "")
	// o conector suporta a recepção de mais de um arquivo por vez, porém
	// será feito a recepção individual para facilitar o controle do
	// paralelismo
	output, err := p.config.TransferService.StartFileTransfer(ctx, &transfer.StartFileTransferInput{
		ConnectorId:        &event.Connector,
		LocalDirectoryPath: &localPath,
		RetrieveFilePaths:  []string{event.RemotePath},
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to start file transfer")
		slog.ErrorContext(ctx, "failed to start file transfer",
			slog.String("connector", event.Connector),
			slog.String("local_path", localPath),
			slog.Any("error", err),
		)
		event.ReturnCode = 1
		event.ReturnMessage = err.Error()
		event.Status = internal.StatusCommandCompleted
		err2 := p.config.EventRepository.Save(ctx, event)
		if err2 != nil {
			slog.ErrorContext(ctx, "failed save event on repository",
				slog.Any("event", event),
				slog.Any("error", err2),
			)
		}
		return true, err
	}
	event.ConnectorEventID = *output.TransferId
	event.Status = internal.StatusCommandExecuting
	err = p.config.EventRepository.Save(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed save event on repository")
		slog.ErrorContext(ctx, "failed save event on repository",
			slog.Any("event", event),
			slog.Any("error", err),
		)
		return true, err
	}
	return false, nil
}
