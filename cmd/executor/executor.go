package main

import (
	"awsconnector/internal"
	"awsconnector/internal/repository"
	"context"
	"log/slog"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Define a configuração do Executor.
type ExecutorConfig struct {
	// Client AWS Transfer para executar operações no conector.
	TransferService internal.TransferService
	// Client AWS S3 para executar operações no bucket.
	S3Service internal.S3Service
	// cliente do AWS SQS para consumir mensagens da fila AWS SQS.
	SqsService internal.SqsService
	// URL da fila AWS SQS para consumir mensagens.
	QueueUrl string
	// Canal para as menagens recebidas para processamento.
	MessageChan chan *internal.MessageContext
	// Número máximo de workers para processar as mensagens da fila AWS SQS.
	MaxWorkers int
	// Repositório para acessar os eventos de execução do conector.
	EventRepository repository.Event
	// URL do bucket para arquivos temporário.
	TemporaryFilePath string
}

// Executor é a estrutura principal que gerencia o processo de consumo e
// processamento de mensagens da fila AWS SQS.
type Executor struct {
	// Tracer para criar spans de telemetria durante o processo.
	tracer trace.Tracer
	// SqsConsumer para consumir mensagens da fila AWS SQS.
	consumer *SqsConsumer
	// Worker para processar as mensagens recebidas da fila AWS SQS.
	workers []*Worker
	// Canal para sinalizar o encerramento do processo.
	stopChan chan int
}

// Construtor para criar uma nova instância do Executor.
func NewExecutor(config *ExecutorConfig) *Executor {
	var workers []*Worker
	configWorker := &WorkerConfig{
		S3Service:         config.S3Service,
		SqsService:        config.SqsService,
		MessageChan:       config.MessageChan,
		EventRepository:   config.EventRepository,
		TransferService:   config.TransferService,
		TemporaryFilePath: config.TemporaryFilePath,
	}
	for i := 0; i < config.MaxWorkers; i++ {
		workers = append(workers, NewWorker(configWorker))
	}
	return &Executor{
		tracer: otel.Tracer("Executor"),
		consumer: NewSqsConsumer(&SqsConsumerConfig{
			SqsService:  config.SqsService,
			QueueUrl:    config.QueueUrl,
			MessageChan: config.MessageChan,
		}),
		workers:  workers,
		stopChan: make(chan int),
	}
}

// Inicia o processo de consumo e processamento de mensagens da fila AWS SQS.
func (p *Executor) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	var err error
	// incia todos os workers em processos separados
	for k, worker := range p.workers {
		wg.Add(1)
		go func(id int, worker *Worker) {
			defer wg.Done()
			worker.Start(ctx)
		}(k, worker)
	}
	// inicia o consumidor em um processo separado
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = p.consumer.Start(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "failed to start SqsConsumer",
				slog.Any("error", err),
			)
			return
		}
	}()
	// aguarda a conclusão de todos os processos
	wg.Wait()
	if err != nil {
		return err
	}
	return nil
}

// Encerra o processo de consumo e processamento de mensagens da fila AWS SQS.
func (p *Executor) Stop(ctx context.Context) {
	p.consumer.Stop(ctx)
	// os workers serão encerrados automaticamente quando o canal de
	// mensagens for fechado pelo consumidor
}
