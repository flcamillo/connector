package main

import (
	"awsconnector/internal"
	"awsconnector/internal/repository"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	// Função para cancelar a execução.
	cancel context.CancelFunc
	// metricas de requisições
	messageProcessed     metric.Int64Counter
	messageFailed        metric.Int64Counter
	messageWaitHistogram metric.Float64Histogram
}

// Construtor para criar uma nova instância do Worker
func NewWorker(config *WorkerConfig) *Worker {
	worker := &Worker{
		tracer: otel.Tracer("Worker"),
		config: config,
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
	if p.cancel != nil {
		p.cancel()
	}
}

// Método para iniciar o processo de consumo de mensagens da fila AWS SQS.
func (p *Worker) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	defer func() {
		slog.WarnContext(ctx, "Worker stopped")
	}()
	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "stopping Worker due to context cancellation...")
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
				err := messageContext.Commit()
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
	eventBridgeEvent := &internal.EventBridgeEvent{}
	err = json.Unmarshal([]byte(messageContext.Body), &eventBridgeEvent)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal message body")
		return false, err
	}
	// identifica o tipo da mensagem e processa de acordo
	switch eventBridgeEvent.DetailType {
	case internal.SftpConnectorDirectoryListingCompleted:
		retry, err = p.processDirectoryListingCompleted(ctx, eventBridgeEvent)
	case internal.SftpConnectorDirectoryListingFailed:
		retry, err = p.processDirectoryListingFailed(ctx, eventBridgeEvent)
	case internal.SftpConnectorRemoteDeleteCompleted:
		retry, err = p.processRemoteDeleteCompleted(ctx, eventBridgeEvent)
	case internal.SftpConnectorRemoteDeleteFailed:
		retry, err = p.processRemoteDeleteFailed(ctx, eventBridgeEvent)
	case internal.SftpConnectorFileRetrieveCompleted:
		retry, err = p.processFileRetrieveCompleted(ctx, eventBridgeEvent)
	case internal.SftpConnectorFileRetrieveFailed:
		retry, err = p.processFileRetrieveFailed(ctx, eventBridgeEvent)
	case internal.SftpConnectorFileSendCompleted:
		retry, err = p.processFileSendCompleted(ctx, eventBridgeEvent)
	case internal.SftpConnectorFileSendFailed:
		retry, err = p.processFileSendFailed(ctx, eventBridgeEvent)
	default:
		slog.WarnContext(ctx, "received unknown detail type",
			slog.String("detail_type", eventBridgeEvent.DetailType),
			slog.String("source", eventBridgeEvent.Source),
		)
		return false, nil
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to process command")
		slog.ErrorContext(ctx, "failed to process command",
			slog.String("detail_type", eventBridgeEvent.DetailType),
			slog.String("connector_id", eventBridgeEvent.Detail.ConnectorId),
			slog.String("failure_code", eventBridgeEvent.Detail.FailureCode),
			slog.String("failure_message", eventBridgeEvent.Detail.FailureMessage),
			slog.Any("error", err),
		)
	}
	// tenta executar os comandos enfileirados do conector
	err = p.executeNextCommand(ctx, eventBridgeEvent.Detail.ConnectorId)
	if err != nil {
		slog.ErrorContext(ctx, "failed to schedule next command",
			slog.String("connector_id", eventBridgeEvent.Detail.ConnectorId),
			slog.Any("error", err),
		)
	}
	return retry, err
}

// Processa o resultado do comando de listagem de diretório com sucesso
// solicitando a recepção de todos os arquivos encontrados no servidor remoto.
func (p *Worker) processDirectoryListingCompleted(ctx context.Context, eventBridgeEvent *internal.EventBridgeEvent) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "processDirectoryListingCompleted")
	defer span.End()
	// recupera o evento do repositório
	event, err := p.config.EventRepository.FindByConnectorEventId(ctx, eventBridgeEvent.Detail.ListingId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get event from repository by listing id")
		slog.ErrorContext(ctx, "failed to get event from repository by listing id",
			slog.String("listing_id", eventBridgeEvent.Detail.ListingId),
			slog.Any("error", err),
		)
		return false, err
	}
	if event == nil {
		slog.WarnContext(ctx, "listing id not found",
			slog.String("listing_id", eventBridgeEvent.Detail.ListingId),
		)
		return true, nil
	}
	span.SetAttributes(
		attribute.String("event.mft", event.Mft),
		attribute.String("event.mailbox", event.Mailbox),
		attribute.String("event.product", event.Product),
		attribute.String("event.connector", event.Connector),
	)
	slog.InfoContext(ctx, "processing directory listing completed",
		slog.String("mft", event.Mft),
		slog.String("mailbox", event.Mailbox),
		slog.String("product", event.Product),
		slog.String("connector", event.Connector),
		slog.String("local_path", event.LocalPath),
		slog.String("remote_path", event.RemotePath),
	)
	// identifica todos os arquivos encontrados na listagem do servidor remoto
	listingOutput, err := p.readConnectorListingOutputFile(ctx, eventBridgeEvent.Detail.OutputFileLocation.Bucket, eventBridgeEvent.Detail.OutputFileLocation.Key)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to read listing directory output file")
		slog.ErrorContext(ctx, "failed to read listing directory output file",
			slog.String("bucket", eventBridgeEvent.Detail.OutputFileLocation.Bucket),
			slog.String("key", eventBridgeEvent.Detail.OutputFileLocation.Key),
			slog.Any("error", err),
		)
		// deve também atualizar o evento para informar a falha
		event.Status = internal.StatusCommandCompleted
		event.ReturnCode = 1
		event.ReturnMessage = err.Error()
		err2 := p.config.EventRepository.Save(ctx, event)
		if err2 != nil {
			slog.ErrorContext(ctx, "failed save event on repository",
				slog.Any("event", event),
				slog.Any("error", err),
			)
			err = errors.Join(err, err2)
		}
		return false, err
	}
	// enfileira cada arquivo para realizar a recepção
	for _, file := range listingOutput.Files {
		fileEvent := &repository.EventRecord{
			Mft:                 event.Mft,
			Mailbox:             event.Mailbox,
			Product:             event.Product,
			Connector:           event.Connector,
			LocalPath:           event.LocalPath,
			RemotePath:          file.FilePath,
			FileNameFilter:      event.FileNameFilter,
			FileSize:            int64(file.Size),
			RemoveAfterDownload: event.RemoveAfterDownload,
			Command:             internal.ConnectorCommandDownload,
			Status:              internal.StatusCommandNotStarted,
			ConnectorEventID:    "NA",
		}
		err = p.config.EventRepository.Save(ctx, fileEvent)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed save event on repository")
			slog.ErrorContext(ctx, "failed save event on repository",
				slog.Any("event", fileEvent),
				slog.Any("error", err),
			)
			// deve também atualizar o evento para informar a falha
			event.Status = internal.StatusCommandCompleted
			event.ReturnCode = 1
			event.ReturnMessage = err.Error()
			err2 := p.config.EventRepository.Save(ctx, event)
			if err2 != nil {
				slog.ErrorContext(ctx, "failed save event on repository",
					slog.Any("event", event),
					slog.Any("error", err),
				)
				err = errors.Join(err, err2)
			}
			return false, err
		}
	}
	slog.InfoContext(ctx, "file reception scheduling completed",
		slog.String("mft", event.Mft),
		slog.String("mailbox", event.Mailbox),
		slog.String("product", event.Product),
		slog.String("connector", event.Connector),
		slog.Int("scheduled_files", len(listingOutput.Files)),
	)
	// deve também atualizar o evento agora como concluído com sucesso
	event.Status = internal.StatusCommandCompleted
	event.ReturnCode = 0
	event.ReturnMessage = ""
	err = p.config.EventRepository.Save(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed save event on repository")
		slog.ErrorContext(ctx, "failed save event on repository",
			slog.Any("event", event),
			slog.Any("error", err),
		)
		return false, err
	}
	return false, nil
}

// Lê o arquivo de saída da listagem de diretório do servidor remoto.
func (p *Worker) readConnectorListingOutputFile(ctx context.Context, bucket string, file string) (output *internal.SftpConnectorOutputDirectoryListing, err error) {
	obj, err := p.config.S3Service.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &file,
	})
	if err != nil {
		return nil, err
	}
	defer obj.Body.Close()
	output = &internal.SftpConnectorOutputDirectoryListing{}
	err = json.NewDecoder(obj.Body).Decode(output)
	if err != nil {
		return nil, err
	}
	return output, nil
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

// Processa o resultado do comando de listagem de diretório com falha.
func (p *Worker) processDirectoryListingFailed(ctx context.Context, eventBridgeEvent *internal.EventBridgeEvent) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "processDirectoryListingFailed")
	defer span.End()
	// recupera o evento do repositório
	event, err := p.config.EventRepository.FindByConnectorEventId(ctx, eventBridgeEvent.Detail.ListingId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get event from repository by listing id")
		slog.ErrorContext(ctx, "failed to get event from repository by listing id",
			slog.String("listing_id", eventBridgeEvent.Detail.ListingId),
			slog.Any("error", err),
		)
		return false, err
	}
	if event == nil {
		slog.WarnContext(ctx, "listing id not found",
			slog.String("listing_id", eventBridgeEvent.Detail.ListingId),
		)
		return false, nil
	}
	span.SetAttributes(
		attribute.String("event.mft", event.Mft),
		attribute.String("event.mailbox", event.Mailbox),
		attribute.String("event.product", event.Product),
		attribute.String("event.connector", event.Connector),
	)
	slog.InfoContext(ctx, "processing directory listing failed",
		slog.String("mft", event.Mft),
		slog.String("mailbox", event.Mailbox),
		slog.String("product", event.Product),
		slog.String("connector", event.Connector),
		slog.String("local_path", event.LocalPath),
		slog.String("remote_path", event.RemotePath),
	)
	// deve atualizar o evento com os resultados da execução do conector
	event.Status = internal.StatusCommandCompleted
	event.ReturnCode = 1
	event.ReturnMessage = fmt.Sprintf("%s %s", eventBridgeEvent.Detail.FailureCode, eventBridgeEvent.Detail.FailureMessage)
	err = p.config.EventRepository.Save(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed save event on repository")
		slog.ErrorContext(ctx, "failed save event on repository",
			slog.Any("event", event),
			slog.Any("error", err),
		)
		return false, err
	}
	return false, nil
}

// Processa o resultado do comando de exclusão de arquivo do servidor remoto
// com sucesso.
func (p *Worker) processRemoteDeleteCompleted(ctx context.Context, eventBridgeEvent *internal.EventBridgeEvent) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "processRemoteDeleteCompleted")
	defer span.End()
	// recupera o evento do repositório
	event, err := p.config.EventRepository.FindByConnectorEventId(ctx, eventBridgeEvent.Detail.DeleteId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get event from repository by delete id")
		slog.ErrorContext(ctx, "failed to get event from repository by delete id",
			slog.String("delete_id", eventBridgeEvent.Detail.DeleteId),
			slog.Any("error", err),
		)
		return false, err
	}
	if event == nil {
		slog.WarnContext(ctx, "delete id not found",
			slog.String("delete_id", eventBridgeEvent.Detail.DeleteId),
		)
		return false, nil
	}
	slog.InfoContext(ctx, "processing remote delete completed",
		slog.String("mft", event.Mft),
		slog.String("mailbox", event.Mailbox),
		slog.String("product", event.Product),
		slog.String("connector", event.Connector),
		slog.String("local_path", event.LocalPath),
		slog.String("remote_path", event.RemotePath),
	)
	// deve atualizar o evento com os resultados da execução do conector
	event.Status = internal.StatusCommandCompleted
	event.ReturnCode = 0
	event.ReturnMessage = ""
	err = p.config.EventRepository.Save(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed save event on repository")
		slog.ErrorContext(ctx, "failed save event on repository",
			slog.Any("event", event),
			slog.Any("error", err),
		)
		return false, err
	}
	return false, nil
}

// Processa o resultado do comando de exclusão de arquivo do servidor remoto
// com falha.
func (p *Worker) processRemoteDeleteFailed(ctx context.Context, eventBridgeEvent *internal.EventBridgeEvent) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "processRemoteDeleteFailed")
	defer span.End()
	// recupera o evento do repositório
	event, err := p.config.EventRepository.FindByConnectorEventId(ctx, eventBridgeEvent.Detail.DeleteId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get event from repository by delete id")
		slog.ErrorContext(ctx, "failed to get event from repository by delete id",
			slog.String("delete_id", eventBridgeEvent.Detail.DeleteId),
			slog.Any("error", err),
		)
		return false, err
	}
	if event == nil {
		slog.WarnContext(ctx, "delete id not found",
			slog.String("delete_id", eventBridgeEvent.Detail.DeleteId),
		)
		return false, nil
	}
	span.SetAttributes(
		attribute.String("event.mft", event.Mft),
		attribute.String("event.mailbox", event.Mailbox),
		attribute.String("event.product", event.Product),
		attribute.String("event.connector", event.Connector),
	)
	slog.InfoContext(ctx, "processing remote delete failed",
		slog.String("mft", event.Mft),
		slog.String("mailbox", event.Mailbox),
		slog.String("product", event.Product),
		slog.String("connector", event.Connector),
		slog.String("local_path", event.LocalPath),
		slog.String("remote_path", event.RemotePath),
	)
	// deve atualizar o evento com os resultados da execução do conector
	event.Status = internal.StatusCommandCompleted
	event.ReturnCode = 1
	event.ReturnMessage = fmt.Sprintf("%s %s", eventBridgeEvent.Detail.FailureCode, eventBridgeEvent.Detail.FailureMessage)
	err = p.config.EventRepository.Save(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed save event on repository")
		slog.ErrorContext(ctx, "failed save event on repository",
			slog.Any("event", event),
			slog.Any("error", err),
		)
		return false, err
	}
	return false, nil
}

// Processa o resultado do comando de recepção de arquivo do servidor remoto
// com sucesso solicitando a recepção do próximo arquivo pendente.
func (p *Worker) processFileRetrieveCompleted(ctx context.Context, eventBridgeEvent *internal.EventBridgeEvent) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "processFileRetrieveCompleted")
	defer span.End()
	// recupera o evento do repositório
	event, err := p.config.EventRepository.FindByConnectorEventId(ctx, eventBridgeEvent.Detail.TransferId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get event from repository by file transfer id")
		slog.ErrorContext(ctx, "failed to get event from repository by file transfer id",
			slog.String("file_transfer_id", eventBridgeEvent.Detail.TransferId),
			slog.Any("error", err),
		)
		return false, err
	}
	if event == nil {
		slog.WarnContext(ctx, "file transfer id not found",
			slog.String("file_transfer_id", eventBridgeEvent.Detail.TransferId),
		)
		return false, nil
	}
	span.SetAttributes(
		attribute.String("event.mft", event.Mft),
		attribute.String("event.mailbox", event.Mailbox),
		attribute.String("event.product", event.Product),
		attribute.String("event.connector", event.Connector),
	)
	slog.InfoContext(ctx, "processing download completed",
		slog.String("mft", event.Mft),
		slog.String("mailbox", event.Mailbox),
		slog.String("product", event.Product),
		slog.String("connector", event.Connector),
		slog.String("local_path", event.LocalPath),
		slog.String("remote_path", event.RemotePath),
	)
	// deve atualizar o evento com os resultados da execução do conector
	event.Status = internal.StatusCommandCompleted
	event.ReturnCode = 0
	event.ReturnMessage = ""
	err = p.config.EventRepository.Save(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed save event on repository")
		slog.ErrorContext(ctx, "failed save event on repository",
			slog.Any("event", event),
			slog.Any("error", err),
		)
		return false, err
	}
	if event.RemoveAfterDownload {
		return p.executeRemoteDelete(ctx, event)
	}
	return false, nil
}

// Solicita a exclusão do arquivo remoto do servidor.
func (p *Worker) executeRemoteDelete(ctx context.Context, event *repository.EventRecord) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "executeRemoteDelete")
	defer span.End()
	deleteEvent := &repository.EventRecord{
		Mft:                 event.Mft,
		Mailbox:             event.Mailbox,
		Product:             event.Product,
		Connector:           event.Connector,
		LocalPath:           event.LocalPath,
		RemotePath:          event.RemotePath,
		FileNameFilter:      event.FileNameFilter,
		RemoveAfterDownload: event.RemoveAfterDownload,
		FileSize:            event.FileSize,
		Command:             internal.ConnectorCommandDelete,
		Status:              internal.StatusCommandNotStarted,
		ConnectorEventID:    "NA",
	}
	err = p.config.EventRepository.Save(ctx, deleteEvent)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed save event on repository")
		slog.ErrorContext(ctx, "failed save event on repository",
			slog.Any("event", deleteEvent),
			slog.Any("error", err),
		)
		return false, err
	}
	return false, nil
}

// Processa o resultado do comando de recepção de arquivo do servidor remoto
// com falha.
func (p *Worker) processFileRetrieveFailed(ctx context.Context, eventBridgeEvent *internal.EventBridgeEvent) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "processFileRetrieveFailed")
	defer span.End()
	// recupera o evento do repositório
	event, err := p.config.EventRepository.FindByConnectorEventId(ctx, eventBridgeEvent.Detail.TransferId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get event from repository by file transfer id")
		slog.ErrorContext(ctx, "failed to get event from repository by file transfer id",
			slog.String("file_transfer_id", eventBridgeEvent.Detail.TransferId),
			slog.Any("error", err),
		)
		return false, err
	}
	if event == nil {
		slog.WarnContext(ctx, "file transfer id not found",
			slog.String("file_transfer_id", eventBridgeEvent.Detail.TransferId),
		)
		return false, nil
	}
	span.SetAttributes(
		attribute.String("event.mft", event.Mft),
		attribute.String("event.mailbox", event.Mailbox),
		attribute.String("event.product", event.Product),
		attribute.String("event.connector", event.Connector),
	)
	slog.InfoContext(ctx, "processing download failed",
		slog.String("mft", event.Mft),
		slog.String("mailbox", event.Mailbox),
		slog.String("product", event.Product),
		slog.String("connector", event.Connector),
		slog.String("local_path", event.LocalPath),
		slog.String("remote_path", event.RemotePath),
	)
	// deve atualizar o evento com os resultados da execução do conector
	event.Status = internal.StatusCommandCompleted
	event.ReturnCode = 1
	event.ReturnMessage = fmt.Sprintf("%s %s", eventBridgeEvent.Detail.FailureCode, eventBridgeEvent.Detail.FailureMessage)
	err = p.config.EventRepository.Save(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed save event on repository")
		slog.ErrorContext(ctx, "failed save event on repository",
			slog.Any("event", event),
			slog.Any("error", err),
		)
		return false, err
	}
	return false, nil
}

// Processa o resultado do comando de envio de arquivo do servidor remoto
// com sucesso solicitando o envio do proximo arquivo pendente.
func (p *Worker) processFileSendCompleted(ctx context.Context, eventBridgeEvent *internal.EventBridgeEvent) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "processFileSendCompleted")
	defer span.End()
	// recupera o evento do repositório
	event, err := p.config.EventRepository.FindByConnectorEventId(ctx, eventBridgeEvent.Detail.TransferId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get event from repository by file transfer id")
		slog.ErrorContext(ctx, "failed to get event from repository by file transfer id",
			slog.String("file_transfer_id", eventBridgeEvent.Detail.TransferId),
			slog.Any("error", err),
		)
		return false, err
	}
	if event == nil {
		slog.WarnContext(ctx, "file transfer id not found",
			slog.String("file_transfer_id", eventBridgeEvent.Detail.TransferId),
		)
		return false, nil
	}
	span.SetAttributes(
		attribute.String("event.mft", event.Mft),
		attribute.String("event.mailbox", event.Mailbox),
		attribute.String("event.product", event.Product),
		attribute.String("event.connector", event.Connector),
	)
	slog.InfoContext(ctx, "processing upload completed",
		slog.String("mft", event.Mft),
		slog.String("mailbox", event.Mailbox),
		slog.String("product", event.Product),
		slog.String("connector", event.Connector),
		slog.String("local_path", event.LocalPath),
		slog.String("remote_path", event.RemotePath),
	)
	// deve atualizar o evento com os resultados da execução do conector
	event.Status = internal.StatusCommandCompleted
	event.ReturnCode = 0
	event.ReturnMessage = ""
	err = p.config.EventRepository.Save(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed save event on repository")
		slog.ErrorContext(ctx, "failed save event on repository",
			slog.Any("event", event),
			slog.Any("error", err),
		)
		return false, err
	}
	// deve sempre remover o arquivo local
	return p.executeLocalDelete(ctx, event)
}

// Exclui o arquivo local.
func (p *Worker) executeLocalDelete(ctx context.Context, event *repository.EventRecord) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "executeLocalDelete")
	defer span.End()
	_, bucket, prefix, file := internal.SplitUrlPath(event.LocalPath)
	if prefix != "" {
		file = fmt.Sprintf("%s/%s", prefix, file)
	}
	_, err = p.config.S3Service.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &file,
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to delete local file")
		slog.ErrorContext(ctx, "failed to delete local file",
			slog.String("bucket", bucket),
			slog.String("key", file),
			slog.Any("error", err),
		)
		return false, err
	}
	return false, nil
}

// Processa o resultado do comando de recepção de arquivo do servidor remoto
// com falha.
func (p *Worker) processFileSendFailed(ctx context.Context, eventBridgeEvent *internal.EventBridgeEvent) (retry bool, err error) {
	ctx, span := p.tracer.Start(ctx, "processFileSendFailed")
	defer span.End()
	// recupera o evento do repositório
	event, err := p.config.EventRepository.FindByConnectorEventId(ctx, eventBridgeEvent.Detail.TransferId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get event from repository by file transfer id")
		slog.ErrorContext(ctx, "failed to get event from repository by file transfer id",
			slog.String("file_transfer_id", eventBridgeEvent.Detail.TransferId),
			slog.Any("error", err),
		)
		return false, err
	}
	if event == nil {
		slog.WarnContext(ctx, "file transfer id not found",
			slog.String("file_transfer_id", eventBridgeEvent.Detail.TransferId),
		)
		return false, nil
	}
	span.SetAttributes(
		attribute.String("event.mft", event.Mft),
		attribute.String("event.mailbox", event.Mailbox),
		attribute.String("event.product", event.Product),
		attribute.String("event.connector", event.Connector),
	)
	slog.InfoContext(ctx, "processing upload failed",
		slog.String("mft", event.Mft),
		slog.String("mailbox", event.Mailbox),
		slog.String("product", event.Product),
		slog.String("connector", event.Connector),
		slog.String("local_path", event.LocalPath),
		slog.String("remote_path", event.RemotePath),
	)
	// deve atualizar o evento com os resultados da execução do conector
	event.Status = internal.StatusCommandCompleted
	event.ReturnCode = 1
	event.ReturnMessage = fmt.Sprintf("%s %s", eventBridgeEvent.Detail.FailureCode, eventBridgeEvent.Detail.FailureMessage)
	err = p.config.EventRepository.Save(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed save event on repository")
		slog.ErrorContext(ctx, "failed save event on repository",
			slog.Any("event", event),
			slog.Any("error", err),
		)
		return false, err
	}
	return false, nil
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
