package repository

import (
	"awsconnector/internal"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Define a configuração do repositório do DynamoDB.
type DynamoDBUserConfig struct {
	// cliente do DynamoDB
	DynamoDBService internal.DynamoDBService
	// nome da tabela
	Table string
	// tempo de expiração dos registros
	TTL time.Duration
}

// Define a estrutura do repositório do DynamoDB.
type DynamoDBUser struct {
	// cliente do DynamoDB
	config *DynamoDBUserConfig
	// configura o tracer
	tracer trace.Tracer
}

// Cria uma nova instância do repositório do DynamoDB.
func NewDynamoDBUser(config *DynamoDBUserConfig) *DynamoDBUser {
	return &DynamoDBUser{
		config: config,
		tracer: otel.Tracer("dynamodb.repository"),
	}
}

// Cria a tabela DynamoDB com os índices secundários globais necessários.
func (p *DynamoDBUser) Create(ctx context.Context) error {
	_, err := p.config.DynamoDBService.CreateTable(ctx, &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("codigoMft"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("codigoCaixaPostal"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("codigoProduto"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("codigoConector"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("estado"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("codigoMft-estado-index"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("codigoMft"),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String("estado"),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
			{
				IndexName: aws.String("codigoCaixaPostal-estado-index"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("codigoCaixaPostal"),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String("estado"),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
			{
				IndexName: aws.String("codigoProduto-estado-index"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("codigoProduto"),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String("estado"),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
			{
				IndexName: aws.String("codigoConector-estado-index"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("codigoConector"),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String("estado"),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
		TableName:   &p.config.Table,
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		slog.ErrorContext(ctx, fmt.Sprintf("unable to create table, %s", err))
		return err
	}
	// deve aguardar até a tabela ser criada e estar disponível para uso
	waiter := dynamodb.NewTableExistsWaiter(p.config.DynamoDBService)
	err = waiter.Wait(context.Background(), &dynamodb.DescribeTableInput{TableName: &p.config.Table}, 5*time.Minute)
	if err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("unable to check if table are ready, %s", err))
		return err
	}
	// só é possível habilitar TTL na tabela após ela ter sido criada
	_, err = p.config.DynamoDBService.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: &p.config.Table,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("expiration"),
			Enabled:       aws.Bool(true),
		},
	})
	if err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("unable to configure TTL on table, %s", err))
		return err
	}
	return nil
}

// Salva o registro na tabela DynamoDB.
// Se já houver registro com o mesmo id, ele será substituído.
func (p *DynamoDBUser) Save(ctx context.Context, record *UserRecord) error {
	if record.Created.IsZero() {
		record.Created = time.Now()
	} else {
		record.Changed = time.Now()
	}
	if record.Expiration == 0 && p.config.TTL > 0 {
		record.Expiration = time.Now().Add(p.config.TTL).Unix()
	}
	if record.Id == "" {
		record.Id = uuid.NewString()
	}
	if record.Status == "" {
		record.Status = UserStatusActive
	}
	item, err := attributevalue.MarshalMap(record)
	if err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("unable to convert record to dynamodb object, %s", err))
		return err
	}
	_, err = p.config.DynamoDBService.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.config.Table,
		Item:      item,
	})
	if err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("unable to put item on dynamodb, %s", err))
		return err
	}
	return nil
}

// Deleta o registro da tabela DynamoDB pelo id.
func (p *DynamoDBUser) Delete(ctx context.Context, id string) (record *UserRecord, err error) {
	out, err := p.config.DynamoDBService.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &p.config.Table,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
		ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("unable to delete item from dynamodb, %s", err))
		return nil, err
	}
	if out.Attributes == nil {
		return nil, nil
	}
	record = &UserRecord{}
	err = attributevalue.UnmarshalMap(out.Attributes, &record)
	if err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("unable to convert dynamodb object to record, %s", err))
		return nil, err
	}
	return record, nil
}

// Recupera o registro da tabela DynamoDB pelo id.
func (p *DynamoDBUser) Get(ctx context.Context, id string) (record *UserRecord, err error) {
	out, err := p.config.DynamoDBService.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &p.config.Table,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
	})
	if err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("unable to get item from dynamodb, %s", err))
		return nil, err
	}
	if out.Item == nil {
		return nil, nil
	}
	record = &UserRecord{}
	err = attributevalue.UnmarshalMap(out.Item, &record)
	if err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("unable to convert dynamodb object to record, %s", err))
		return nil, err
	}
	return record, nil
}

// Procura registros na tabela DynamoDB pelo código MFT.
func (p *DynamoDBUser) FindByMft(ctx context.Context, mft string, status string) (events []*UserRecord, err error) {
	condition := &dynamodb.QueryInput{
		TableName: aws.String(p.config.Table),
		IndexName: aws.String("codigoMft-estado-index"),
		KeyConditionExpression: aws.String(
			"codigoMft = :codigoMft AND estado = :estado",
		),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":codigoMft": &types.AttributeValueMemberS{Value: mft},
			":estado":    &types.AttributeValueMemberS{Value: status},
		},
	}
	paginator := dynamodb.NewQueryPaginator(p.config.DynamoDBService, condition)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			slog.ErrorContext(ctx, fmt.Sprintf("unable to get next page of records from dynamodb, %s", err))
			return nil, err
		}
		for _, item := range page.Items {
			record := &UserRecord{}
			err = attributevalue.UnmarshalMap(item, record)
			if err != nil {
				slog.ErrorContext(ctx, fmt.Sprintf("unable to convert dynamodb object to record, %s", err))
				return nil, err
			}
			events = append(events, record)
		}
	}
	return events, nil
}

// Procura registros na tabela DynamoDB pelo código da caixa postal.
func (p *DynamoDBUser) FindByMailbox(ctx context.Context, mailbox string, status string) (events []*UserRecord, err error) {
	condition := &dynamodb.QueryInput{
		TableName: aws.String(p.config.Table),
		IndexName: aws.String("codigoCaixaPostal-estado-index"),
		KeyConditionExpression: aws.String(
			"caixaPostal = :caixaPostal AND estado = :estado",
		),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":caixaPostal": &types.AttributeValueMemberS{Value: mailbox},
			":estado":      &types.AttributeValueMemberS{Value: status},
		},
	}
	paginator := dynamodb.NewQueryPaginator(p.config.DynamoDBService, condition)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			slog.ErrorContext(ctx, fmt.Sprintf("unable to get next page of records from dynamodb, %s", err))
			return nil, err
		}
		for _, item := range page.Items {
			record := &UserRecord{}
			err = attributevalue.UnmarshalMap(item, record)
			if err != nil {
				slog.ErrorContext(ctx, fmt.Sprintf("unable to convert dynamodb object to record, %s", err))
				return nil, err
			}
			events = append(events, record)
		}
	}
	return events, nil
}

// Procura registros na tabela DynamoDB pelo código do produto.
func (p *DynamoDBUser) FindByProduct(ctx context.Context, product string, status string) (events []*UserRecord, err error) {
	condition := &dynamodb.QueryInput{
		TableName: aws.String(p.config.Table),
		IndexName: aws.String("codigoProduto-estado-index"),
		KeyConditionExpression: aws.String(
			"codigoProduto = :codigoProduto AND estado = :estado",
		),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":codigoProduto": &types.AttributeValueMemberS{Value: product},
			":estado":        &types.AttributeValueMemberS{Value: status},
		},
	}
	paginator := dynamodb.NewQueryPaginator(p.config.DynamoDBService, condition)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			slog.ErrorContext(ctx, fmt.Sprintf("unable to get next page of records from dynamodb, %s", err))
			return nil, err
		}
		for _, item := range page.Items {
			record := &UserRecord{}
			err = attributevalue.UnmarshalMap(item, record)
			if err != nil {
				slog.ErrorContext(ctx, fmt.Sprintf("unable to convert dynamodb object to record, %s", err))
				return nil, err
			}
			events = append(events, record)
		}
	}
	return events, nil
}

// Procura registros na tabela DynamoDB pelo código do conector.
func (p *DynamoDBUser) FindByConnector(ctx context.Context, connector string, status string) (events []*UserRecord, err error) {
	condition := &dynamodb.QueryInput{
		TableName: aws.String(p.config.Table),
		IndexName: aws.String("codigoConector-estado-index"),
		KeyConditionExpression: aws.String(
			"codigoConector = :codigoConector AND estado = :estado",
		),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":codigoConector": &types.AttributeValueMemberS{Value: connector},
			":estado":         &types.AttributeValueMemberS{Value: status},
		},
	}
	paginator := dynamodb.NewQueryPaginator(p.config.DynamoDBService, condition)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			slog.ErrorContext(ctx, fmt.Sprintf("unable to get next page of records from dynamodb, %s", err))
			return nil, err
		}
		for _, item := range page.Items {
			record := &UserRecord{}
			err = attributevalue.UnmarshalMap(item, record)
			if err != nil {
				slog.ErrorContext(ctx, fmt.Sprintf("unable to convert dynamodb object to record, %s", err))
				return nil, err
			}
			events = append(events, record)
		}
	}
	return events, nil
}
