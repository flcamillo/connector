package internal

import (
	"context"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/transfer"
)

// Tipos de eventos enviados para o AWS EventBridge pelo AWS Transfer Family Connector.
const (
	SftpConnectorFileSendCompleted         = "SFTP Connector File Send Completed"
	SftpConnectorFileSendFailed            = "SFTP Connector File Send Failed"
	SftpConnectorFileRetrieveCompleted     = "SFTP Connector File Retrieve Completed"
	SftpConnectorFileRetrieveFailed        = "SFTP Connector File Retrieve Failed"
	SftpConnectorDirectoryListingCompleted = "SFTP Connector Directory Listing Completed"
	SftpConnectorDirectoryListingFailed    = "SFTP Connector Directory Listing Failed"
	SftpConnectorRemoteMoveCompleted       = "SFTP Connector Remote Move Completed"
	SftpConnectorRemoteMoveFailed          = "SFTP Connector Remote Move Failed"
	SftpConnectorRemoteDeleteCompleted     = "SFTP Connector Remote Delete Completed"
	SftpConnectorRemoteDeleteFailed        = "SFTP Connector Remote Delete Failed"
)

// Define os valores para os comandos executados pelo AWS Transfer Family Connector.
const (
	ConnectorCommandList     = "List"
	ConnectorCommandUpload   = "Upload"
	ConnectorCommandDownload = "Download"
	ConnectorCommandDelete   = "Delete"
)

// Tipos de estado que o comando do conector pode estar.
const (
	StatusCommandNotStarted = "Not Started"
	StatusCommandExecuting  = "Executing"
	StatusCommandCompleted  = "Completed"
)

// Define os valores para as operações de agendamento.
const (
	SchedulerEventCommandStart = "Start"
)

// Define os valores padrão de configuração.
const (
	// Duranção dos registros na tabela de eventos.
	EventRecordExpiration = 24 * time.Hour
	// Total de arquivos que podem ser enviados paralelo no
	// AWS Transfer Family Connector.
	ConnectorMaxSendFiles = 100
	// Total de arquivos que podem ser recebidos em paralelo no
	// AWS Transfer Family Connector.
	ConnectorMaxReceiveFiles = 100
)

// Interface para o Client do AWS S3, permitindo a abstração e facilitando testes unitários.
type S3Service interface {
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// Interface para o Client do AWS Sqs, permitindo a abstração e facilitando testes unitários.
type SqsService interface {
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
}

// Interface para o Client do AWS Transfer Family, permitindo a abstração e facilitando testes unitários.
type TransferService interface {
	StartDirectoryListing(ctx context.Context, params *transfer.StartDirectoryListingInput, optFns ...func(*transfer.Options)) (*transfer.StartDirectoryListingOutput, error)
	StartFileTransfer(ctx context.Context, params *transfer.StartFileTransferInput, optFns ...func(*transfer.Options)) (*transfer.StartFileTransferOutput, error)
	StartRemoteDelete(ctx context.Context, params *transfer.StartRemoteDeleteInput, optFns ...func(*transfer.Options)) (*transfer.StartRemoteDeleteOutput, error)
}

// Define a interface do cliente do AWS DynamoDB.
type DynamoDBService interface {
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	UpdateTimeToLive(ctx context.Context, params *dynamodb.UpdateTimeToLiveInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

// Interface para o Client do AWS Secret Manager, permitindo a abstração e facilitando testes unitários.
type SecretService interface {
	GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

// Separa um caminho em formato de URL. Se o caminho terminal com "/" então
// considera que não foi informado o arquivo.
//
// Exemplo:
//
//	s3://bucket/prefix1/prefix2/file.txt
//
// Retorna:
//
//	root = "bucket"
//	prefix = "prefix1/prefix2"
//	file = "file.txt"
func SplitUrlPath(path string) (schema string, root string, prefix string, file string) {
	parts := strings.Split(path, "://")
	if len(parts) == 2 {
		schema = parts[0]
		path = parts[1]
	}
	// normaliza as barras
	path = strings.ReplaceAll(path, "\\", "/")
	path = strings.ReplaceAll(path, "//", "/")
	// separa novamente o restante da url para extrair
	// as informações
	parts = strings.Split(path, "/")
	// se só houver uma parte então ou esta parte é o arquivo ou é a raiz
	if len(parts) < 2 {
		if strings.HasSuffix(path, "") {
			return schema, parts[0], "", ""
		}
		return schema, "", "", parts[0]
	}
	// se houver duas partes então podemos ter uma raiz e um prefixo
	// ou uma raiz e um arquivo
	if len(parts) == 2 {
		if strings.Contains(parts[1], ".") {
			return schema, parts[0], "", parts[1]
		}
		return schema, parts[0], parts[1], ""
	}
	// se houver mais de duas partes então temos todos os valores
	return schema, parts[0], strings.Join(parts[1:len(parts)-1], "/"), parts[len(parts)-1]
}

// Retorna o nome do arquivo e sua extensão.
func FileNameExtension(value string) (name string, extension string) {
	// identifica o nome do arquivo
	for i := len(value) - 1; i >= 0; i-- {
		if value[i] == '/' || value[i] == '\\' {
			name = value[i+1:]
		}
	}
	if name == "" {
		name = value
	}
	// separa do nome a extensão
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			return name[:i], name[i:]
		}
	}
	return name, ""
}

// Converte as variaveis especiais em seus respectivos valores.
// Variavies disponíveis de tempo:
//
// Supondo que agora seja a data: 25/02/2026 17:00:01
//
//	#DY = ano com quatro digitos, exemplo: 2026
//	#DM = ano com quatro digitos, exemplo: 02
//	#DD = dia com dois digitos, exemplo: 25
//	#TH = hora com dois digitos, exemplo: 17
//	#TM = minuto com dois digitos, exemplo: 00
//	#TS = segundos com dois digitos, exemplo: 01
//
// Variavies disponíveis do arquivo:
//
// Supondo que o nome do arquivo seja: file.txt
//
//	#FN = nome do arquivo sem extensão, exemplo: file
//	#FE = extensão do arquivo com ponto, exemplo: .txt
func ParseVariables(mask string, fileName string) string {
	today := time.Now()
	// formato RFC3339: 2006-01-02T15:04:05Z07:00
	dy := today.Format("2006")
	dm := today.Format("01")
	dd := today.Format("02")
	th := today.Format("15")
	tm := today.Format("04")
	ts := today.Format("05")
	mask = strings.ReplaceAll(mask, "#DY", dy)
	mask = strings.ReplaceAll(mask, "#DM", dm)
	mask = strings.ReplaceAll(mask, "#DD", dd)
	mask = strings.ReplaceAll(mask, "#TH", th)
	mask = strings.ReplaceAll(mask, "#TM", tm)
	mask = strings.ReplaceAll(mask, "#TS", ts)
	// separa o nome da extensão do arquivo
	if fileName != "" {
		fn, fe := FileNameExtension(fileName)
		mask = strings.ReplaceAll(mask, "#FN", fn)
		mask = strings.ReplaceAll(mask, "#FE", fe)
	}
	return mask
}
