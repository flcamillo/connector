package main

import (
	"awsconnector/internal"
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Define a estrutura para a configuração do processo responsável por consumir mensagens
type SqsConsumerConfig struct {
	// Client AWS SQS para executar operações na fila.
	SqsService internal.SqsService
	// URL da fila AWS SQS para consumo de mensagens.
	QueueUrl string
	// Canal para enviar as menagens recebidas para processamento.
	MessageChan chan *internal.MessageContext
}

// Define a estrutura para o processo responsável por consumir mensagens da
// fila AWS SQS.
type SqsConsumer struct {
	// Configuração gerais.
	config *SqsConsumerConfig
	// Tracer para criar spans de telemetria durante o processo.
	tracer trace.Tracer
	// metricas de requisições
	messageCounter       metric.Int64Counter
	messageWaitHistogram metric.Float64Histogram
}

// Construtor para criar uma nova instância do SqsConsumer.
func NewSqsConsumer(config *SqsConsumerConfig) *SqsConsumer {
	consumer := &SqsConsumer{
		tracer: otel.Tracer("SqsConsumer"),
		config: config,
	}
	// configura as metricas
	meter := otel.Meter("SQSConsumer.metrics")
	if counter, err := meter.Int64Counter("custom.sqsconsumer.messages.received",
		metric.WithDescription("The number of messages received from queue"),
		metric.WithUnit("{messages}")); err == nil {
		consumer.messageCounter = counter
	} else {
		panic(err)
	}
	if histogram, err := meter.Float64Histogram("custom.sqsconsumer.messages.wait.duration",
		metric.WithDescription("Message waiting time before being queued for a worker."),
		metric.WithUnit("s")); err == nil {
		consumer.messageWaitHistogram = histogram
	} else {
		panic(err)
	}
	return consumer
}

// Método para iniciar o processo de consumo de mensagens da fila AWS SQS.
func (p *SqsConsumer) Run(ctx context.Context) error {
	slog.InfoContext(ctx, "starting SqsConsumer...")
	defer slog.WarnContext(ctx, "SqsConsumer stopped")
	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "stopping SqsConsumer due to context cancellation...")
			return nil
		default:
			ctx, span := p.tracer.Start(ctx, "Waiting.For.Messages")
			messages, err := p.receiveMessage(ctx)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "failed to receive messages")
				slog.ErrorContext(ctx, "failed to receive messages",
					slog.Any("error", err),
				)
				span.End()
				return err
			}
			if len(messages) == 0 {
				span.End()
				continue
			}
			// envia as mensagens recebidas para o worker incrementando as
			// as metricas de quantidade de mensagens recebidas e também a
			// metrica que indica quanto tempo levou para a mensagem ser
			// enfileirada para o worker
			p.messageCounter.Add(ctx, int64(len(messages)))
			for _, msg := range messages {
				started := time.Now()
				p.config.MessageChan <- msg
				p.messageWaitHistogram.Record(ctx, time.Since(started).Seconds())
			}
			span.End()
		}
	}
}

// Recebe as mensagens do AWS SQS.
func (p *SqsConsumer) receiveMessage(ctx context.Context) (messagesContext []*internal.MessageContext, err error) {
	out, err := p.config.SqsService.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &p.config.QueueUrl,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     10,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, nil
		}
		return nil, err
	}
	for _, v := range out.Messages {
		// define a função para fazer o commit da mensagem processada.
		commitFunc := func(ctx context.Context) error {
			return p.commitMessage(ctx, *v.ReceiptHandle)
		}
		messagesContext = append(messagesContext, &internal.MessageContext{
			Context:  ctx,
			Id:       *v.MessageId,
			Body:     *v.Body,
			Received: time.Now(),
			Source:   "AWS SQS",
			Commit:   commitFunc,
		})
	}
	return messagesContext, nil
}

// Realiza o commit da mensagem no AWS SQS.
func (p *SqsConsumer) commitMessage(ctx context.Context, receiptHandle string) error {
	_, err := p.config.SqsService.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &p.config.QueueUrl,
		ReceiptHandle: &receiptHandle,
	})
	return err
}
