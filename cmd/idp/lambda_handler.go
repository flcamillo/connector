package main

import (
	"awsconnector/internal"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Configuração do LambdaHandler.
type LambdaHandlerConfig struct {
	// S3 bucket para o usuário enviar arquivos.
	LocalPathForSend string
	// S3 bucket para o usuário receber arquivos.
	LocalPathForReceive string
	// IAM Role com os acessos permitidos para o usuário.
	UserRoleArn string
	// Prefixo do AWS Secret Manager onde estão os cadastros dos usuários
	// exemplo: sftp/
	SecretManagerPrefix string
	// Client AWS Secret Manager para ler os usuários cadastrados do
	// AWS Transfer Family.
	SecretService internal.SecretService
	// Client AWS S3 para executar operações no bucket.
	S3Service internal.S3Service
}

// Estrutura do LambdaHandler.
type LambdaHandler struct {
	// configuração do handler
	config *LambdaHandlerConfig
	// configura o tracer
	tracer trace.Tracer
}

// Estrutura da solicitação de autenticação do AWS Transfer Family
// passada ao IDP.
type AwsTransferAutenticationRequest struct {
	UserName string `json:"username"`
	Password string `json:"password"`
	Protocol string `json:"protocol"`
	ServerId string `json:"serverId"`
	SourceIp string `json:"sourceIp"`
}

// Estrutura do diretório virtual do AWS Transfer Family.
type AwsHomeDirectoryDetails struct {
	Entry  string `json:"Entry"`
	Target string `json:"Target"`
}

// Estrutura da resposta que deve ser retornada ao AWS Transfer Family
// para as chamadas de autenticação do IDP.
type AwsTransferAutenticationResponse struct {
	Role                 string   `json:"Role,omitempty"`
	PosixProfile         string   `json:"PosixProfile,omitempty"`
	PublicKeys           []string `json:"PublicKeys,omitempty"`
	Policy               string   `json:"Policy,omitempty"`
	HomeDirectoryType    string   `json:"HomeDirectoryType,omitempty"`
	HomeDirectoryDetails string   `json:"HomeDirectoryDetails,omitempty"`
	HomeDirectory        string   `json:"HomeDirectory,omitempty"`
}

// Estrutura do segredo do usuário no AWS Secret Manager.
type AwsSecretManagerUser struct {
	UserName  string   `json:"Username"`
	Password  string   `json:"Password"`
	PublicKey []string `json:"PublicKey"`
}

// Cria uma nova instância do LambdaHandler.
func NewLambdaHandler(config *LambdaHandlerConfig) *LambdaHandler {
	return &LambdaHandler{
		config: config,
		tracer: otel.Tracer("lambda.handler"),
	}
}

// Identifica o método HTTP da requisição e direciona para o handler apropriado.
func (p *LambdaHandler) HandleRequest(ctx context.Context, event *AwsTransferAutenticationRequest) (*AwsTransferAutenticationResponse, error) {
	ctx, span := p.tracer.Start(ctx, "HandleRequest", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	if event.Password == "" {
		return p.authenticateWithSSHPublickey(ctx, event)
	}
	return p.authenticateWithPassword(ctx, event)
}

// Autentica o usuário com a chave SSH.
func (p *LambdaHandler) authenticateWithSSHPublickey(ctx context.Context, event *AwsTransferAutenticationRequest) (*AwsTransferAutenticationResponse, error) {
	ctx, span := p.tracer.Start(ctx, "authenticateWithSSHPublickey")
	defer span.End()
	slog.InfoContext(ctx, "authenticating user with ssh public key password",
		slog.String("user_name", event.UserName),
		slog.String("protocol", event.Protocol),
		slog.String("server_id", event.ServerId),
		slog.String("source_ip", event.SourceIp),
	)
	credentials, err := p.userCredentials(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get user credentials")
		slog.ErrorContext(ctx, "failed to get user credentials",
			slog.Any("error", err),
		)
		return &AwsTransferAutenticationResponse{}, err
	}
	// retorna a chave ssh para ser validada
	response, err := p.buildResponse(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed build response for user")
		slog.ErrorContext(ctx, "failed build response for user",
			slog.Any("error", err),
		)
		return &AwsTransferAutenticationResponse{}, err
	}
	response.PublicKeys = credentials.PublicKey
	return response, nil
}

// Autentica o usuário com a senha.
func (p *LambdaHandler) authenticateWithPassword(ctx context.Context, event *AwsTransferAutenticationRequest) (*AwsTransferAutenticationResponse, error) {
	ctx, span := p.tracer.Start(ctx, "authenticateWithPassword")
	defer span.End()
	slog.InfoContext(ctx, "authenticating user with password",
		slog.String("user_name", event.UserName),
		slog.String("protocol", event.Protocol),
		slog.String("server_id", event.ServerId),
		slog.String("source_ip", event.SourceIp),
	)
	credentials, err := p.userCredentials(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get user credentials")
		slog.ErrorContext(ctx, "failed to get user credentials",
			slog.Any("error", err),
		)
		return &AwsTransferAutenticationResponse{}, err
	}
	// se a senha não estiver correta deve retornar sempre uma resposta vazia
	if event.Password != credentials.Password {
		slog.WarnContext(ctx, "user password invalid")
		return &AwsTransferAutenticationResponse{}, nil
	}
	response, err := p.buildResponse(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed build response for user")
		slog.ErrorContext(ctx, "failed build response for user",
			slog.Any("error", err),
		)
		return &AwsTransferAutenticationResponse{}, err
	}
	return response, nil
}

// Cria a responsta que será retornada ao servidor do AWS Transfer Family.
// A resposta será criada usando diretórios virtuais para os locais de
// envio e recepção de arquivos.
func (p *LambdaHandler) buildResponse(ctx context.Context, event *AwsTransferAutenticationRequest) (*AwsTransferAutenticationResponse, error) {
	virtualDirectoryDetails := []*AwsHomeDirectoryDetails{
		{
			Entry:  "/upload",
			Target: fmt.Sprintf("/%s/%s", p.config.LocalPathForSend, event.UserName),
		},
		{
			Entry:  "/download",
			Target: fmt.Sprintf("/%s/%s", p.config.LocalPathForReceive, event.UserName),
		},
	}
	err := p.createBucketFolders(ctx, virtualDirectoryDetails)
	if err != nil {
		return nil, err
	}
	virtualDirectoryData, _ := json.Marshal(virtualDirectoryDetails)
	return &AwsTransferAutenticationResponse{
		Role:                 p.config.UserRoleArn,
		HomeDirectoryType:    "LOGICAL",
		HomeDirectoryDetails: string(virtualDirectoryData),
	}, nil
}

// Cria a estrutura de pastas do usuário no bucket.
func (p *LambdaHandler) createBucketFolders(ctx context.Context, directories []*AwsHomeDirectoryDetails) error {
	for _, entry := range directories {
		parts := strings.Split(entry.Target, "/")
		bucket := parts[1]
		folder := fmt.Sprintf("%s/", strings.Join(parts[2:], "/"))
		_, err := p.config.S3Service.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &folder,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Recupera as credenciais do usuário.
func (p *LambdaHandler) userCredentials(ctx context.Context, event *AwsTransferAutenticationRequest) (credentials *AwsSecretManagerUser, err error) {
	secretId := fmt.Sprintf("%s/%s", p.config.SecretManagerPrefix, event.UserName)
	secret, err := p.config.SecretService.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: &secretId,
	})
	if err != nil {
		return nil, err
	}
	credentials = &AwsSecretManagerUser{}
	err = json.Unmarshal([]byte(*secret.SecretString), credentials)
	if err != nil {
		return nil, err
	}
	return credentials, nil
}
