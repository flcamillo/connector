package main

import (
	"awsconnector/internal"
	"awsconnector/internal/repository"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/transfer"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
)

// Esta aplicação receberá eventos de comandos para executar
// no AWS Transfer Family Connector através de uma fila AWS SQS.
// Os comandos representam operações como listar arquivos no servidor remoto,
// receber um arquivo, enviar um arquivo ou excluir um arquivo.
// A aplicação esta instrumentada com OpenTelemetry para coletar métricas,
// traces e logs dos componentes envolidos.
// Para que a telemetria seja coletada corretamente é esperado que seja
// informado via variável de ambiente o nome do serviço OTEL_SERVICE_NAME.
// Para configurar a aplicação também são esperadas as seguintes variáveis
// de ambiente:
// - SQS_QUEUE_URL: URL da fila AWS SQS onde serão recebidos os eventos do
// AWS EventBridge com as ações de inicio ou encerramento dos conectores
// do AWS Transfer Family.
// - S3_TEMPORARY_PATH: URL para o bucket S3 que será usado para gravar
// arquivos temporários gerados pelo AWS Transfer Family Connector.
// A URL precisa estar no formato: "s3://bucket/prefix1/prefix2/"
// - MAX_WORKERS: quantidade de workers para processar as mensagens recebidas
// da fila AWS SQS. O valor padrão é 10.
// - MAX_SEND_FILES: quantidade maxima de arquivos quem podem ser enviados
// em paralelo.
// - EVENT_RECORD_EXPIRATION: duranção em segundos dos eventos na tabela
// de eventos, após isso os registros serão removidos automaticamente.

var (
	// encerramento do OTel SDK
	otelShutdown func(ctx context.Context) error
	// fila AWS SQS onde serão recebidos os eventos do AWS EventBridge das
	// funções executadas pelo AWS Transfer Family Connector
	sqsQueueUrl string
	// quantidade de workers para processar as mensagens recebidas da fila AWS SQS
	maxWorkers int
	// Client AWS SQS para executar operações na fila.
	sqsService internal.SqsService
	// Client AWS S3 para executar operações no bucket.
	s3Service internal.S3Service
	// Client AWS Transfer para executar operações no conector.
	transferService internal.TransferService
	// Client AWS DynamoDB para executar operações no repositório.
	dynamoDBService internal.DynamoDBService
	// Repositório para acessar os eventos de execução do conector.
	eventRepository repository.Event
	// Tempo de expiração em segundos dos registros da tabela de eventos.
	eventRecordExpiration time.Duration
	// Caminho do bucket para os arquivos temporários.
	// Deve estar no padrão: "s3://bucket/prefix1/prefix2/"
	s3TemporaryPath string
)

// Inicializa recursos essenciais da aplicação.
func init() {
	var err error
	ctx := context.Background()
	// agenda uma função para encerrar a aplicação e a telemetria caso ocorra qualquer erro
	defer func() {
		if err != nil {
			if otelShutdown != nil {
				err = otelShutdown(ctx)
				if err != nil {
					fmt.Printf("failed to shutdown OTel SDK, %s", err)
				}
			}
			os.Exit(1)
		}
	}()
	// inicializa o log padrão
	slog.SetDefault(otelslog.NewLogger(os.Getenv("OTEL_SERVICE_NAME")))
	// inicializa a telemetria
	otelShutdown, err = setupOTelSDK(context.Background())
	if err != nil {
		fmt.Printf("failed to setup OTel SDK, %s\n", err)
		return
	}
	// coleta metricas de runtime
	err = runtime.Start(runtime.WithMinimumReadMemStatsInterval(time.Second))
	if err != nil {
		slog.Error("failed to setup runtime metrics",
			slog.Any("error", err),
		)
		return
	}
	// inicializa as configurações da aplicação
	sqsQueueUrl = os.Getenv("SQS_QUEUE_URL")
	if sqsQueueUrl == "" {
		err = fmt.Errorf("SQS_QUEUE_URL environment variable is not set")
		slog.Error(err.Error())
		return
	}
	s3TemporaryPath = os.Getenv("S3_TEMPORARY_PATH")
	if s3TemporaryPath == "" {
		err = fmt.Errorf("S3_TEMPORARY_PATH environment variable is not set")
		slog.Error(err.Error())
		return
	}
	maxWorkers = 10
	if maxWorkersEnv := os.Getenv("MAX_WORKERS"); maxWorkersEnv != "" {
		n, err := strconv.Atoi(maxWorkersEnv)
		if err != nil {
			slog.Warn("failed to parse MAX_WORKERS",
				slog.String("MAX_WORKERS", maxWorkersEnv),
				slog.Any("error", err),
			)
		} else {
			maxWorkers = n
		}
	}
	eventRecordExpiration = internal.EventRecordExpiration
	if eventRecordExpirationEnv := os.Getenv("EVENT_RECORD_EXPIRATION"); eventRecordExpirationEnv != "" {
		n, err := strconv.Atoi(eventRecordExpirationEnv)
		if err != nil {
			slog.Warn("failed to parse EVENT_RECORD_EXPIRATION",
				slog.String("EVENT_RECORD_EXPIRATION", eventRecordExpirationEnv),
				slog.Any("error", err),
			)
		} else {
			eventRecordExpiration = time.Duration(n) * time.Second
		}
	}
	// inicializa o sdk da AWS
	sdkConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		slog.Error("failed to load AWS SDK config",
			slog.Any("error", err),
		)
		return
	}
	otelaws.AppendMiddlewares(&sdkConfig.APIOptions)
	sqsService = sqs.NewFromConfig(sdkConfig)
	s3Service = s3.NewFromConfig(sdkConfig)
	transferService = transfer.NewFromConfig(sdkConfig)
	dynamoDBService = dynamodb.NewFromConfig(sdkConfig)
	// inicializa os repositórios
	eventRepository = repository.NewDynamoDBEvent(&repository.DynamoDBEventConfig{
		DynamoDBService: dynamoDBService,
		Table:           "sftp_client_events",
		TTL:             eventRecordExpiration,
	})
	err = eventRepository.Create(context.Background())
	if err != nil {
		slog.Error("failed to create event repository",
			slog.Any("error", err),
		)
	}
}

// Função principal.
func main() {
	// deve inicializar um context com cancelamento para receber sinais de término
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// inicia o processo de consumo e processamento de mensagens da fila AWS SQS
	errChan := make(chan error, 1)
	messageChan := make(chan *internal.MessageContext, maxWorkers)
	executor := NewExecutor(&ExecutorConfig{
		TransferService:   transferService,
		S3Service:         s3Service,
		SqsService:        sqsService,
		QueueUrl:          sqsQueueUrl,
		MessageChan:       messageChan,
		MaxWorkers:        maxWorkers,
		EventRepository:   eventRepository,
		TemporaryFilePath: s3TemporaryPath,
	})
	errChan <- executor.Start(ctx)
	// aguarda o sinal de término
	select {
	case <-ctx.Done():
		slog.WarnContext(ctx, "received shutdown signal")
	case err := <-errChan:
		if err != nil {
			slog.ErrorContext(ctx, "failed to start Executor",
				slog.Any("error", err),
			)
		}
	}
	// encerra a telemetria
	err := otelShutdown(context.Background())
	if err != nil {
		fmt.Printf("failed to shutdown OTel SDK, %s\n", err)
	}
}
