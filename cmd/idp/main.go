package main

import (
	"awsconnector/internal"
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
)

// Esta aplicação receberá eventos de autenticação do AWS Transfer Family.
// Os eventos recebidos podem ser de dois tipos, autenticação de usuário e
// senha ou antenticação de chave pública SSH.
// A aplicação esta instrumentada com OpenTelemetry para coletar métricas,
// traces e logs dos componentes envolidos.
// Para que a telemetria seja coletada corretamente é esperado que seja
// informado via variável de ambiente o nome do serviço OTEL_SERVICE_NAME.

var (
	// encerramento do OTel SDK
	otelShutdown func(ctx context.Context) error
	// S3 bucket para o usuário enviar arquivos.
	localPathForSend string
	// S3 bucket para o usuário receber arquivos.
	localPathForReceive string
	// IAM Role com os acessos permitidos para o usuário.
	userRoleArn string
	// Prefixo do AWS Secret Manager onde estão os cadastros dos usuários
	// exemplo: sftp
	secretManagerPrefix string
	// Client AWS Secret Manager para ler os usuários cadastrados do
	// AWS Transfer Family.
	secretService internal.SecretService
	// Client AWS S3 para executar operações no bucket.
	s3Service internal.S3Service
)

// Inicializa recursos essenciais da aplicação.
func init() {
	// inicializa o log padrão
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	// inicializa a telemetria
	var err error
	otelShutdown, err = setupOTelSDK(context.Background())
	if err != nil {
		slog.Error("failed to setup OTel SDK",
			slog.Any("error", err),
		)
		os.Exit(1)
	}
	// coleta metricas de runtime
	err = runtime.Start(runtime.WithMinimumReadMemStatsInterval(time.Second))
	if err != nil {
		slog.Error("failed to setup runtime metrics",
			slog.Any("error", err),
		)
		os.Exit(1)
	}
	// inicializa as configurações da aplicação
	localPathForSend = os.Getenv("LOCAL_PATH_FOR_SEND")
	if localPathForSend == "" {
		slog.Error("LOCAL_PATH_FOR_SEND environment variable is not set")
		os.Exit(1)
	}
	localPathForReceive = os.Getenv("LOCAL_PATH_FOR_RECEIVE")
	if localPathForReceive == "" {
		slog.Error("LOCAL_PATH_FOR_RECEIVE environment variable is not set")
		os.Exit(1)
	}
	userRoleArn = os.Getenv("USER_ROLE_ARN")
	if userRoleArn == "" {
		slog.Error("USER_ROLE_ARN environment variable is not set")
		os.Exit(1)
	}
	secretManagerPrefix = os.Getenv("SECRET_MANAGER_PREFIX")
	if secretManagerPrefix == "" {
		slog.Error("SECRET_MANAGER_PREFIX environment variable is not set")
		os.Exit(1)
	}
	// inicializa o sdk da AWS
	sdkConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		slog.Error("failed to load AWS SDK config",
			slog.Any("error", err),
		)
		os.Exit(1)
	}
	otelaws.AppendMiddlewares(&sdkConfig.APIOptions)
	secretService = secretsmanager.NewFromConfig(sdkConfig)
	s3Service = s3.NewFromConfig(sdkConfig)
}

// Função principal.
func main() {
	lambdaHandler := NewLambdaHandler(&LambdaHandlerConfig{
		LocalPathForSend:    localPathForSend,
		LocalPathForReceive: localPathForReceive,
		UserRoleArn:         userRoleArn,
		SecretManagerPrefix: secretManagerPrefix,
		SecretService:       secretService,
		S3Service:           s3Service,
	})
	lambda.Start(lambdaHandler.HandleRequest)
	// encerra a telemetria
	err := otelShutdown(context.Background())
	if err != nil {
		slog.Error("failed to shutdown OTel SDK",
			slog.Any("error", err),
		)
	}
}
