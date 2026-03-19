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

// Define a configuração do Scheduler.
type SchedulerConfig struct {
	// Client AWS S3 para executar operações no bucket.
	S3Service internal.S3Service
	// cliente do AWS SQS para consumir mensagens da fila AWS SQS.
	SqsService internal.SqsService
	// URL da fila AWS SQS para consumir mensagens de inicio da agenda
	// recebidas do AWS EventBridge.
	SqsEventBridgeUrl string
	// URL da fila AWS SQS para enviar comandos para o AWS Transfer Family
	// Connector.
	SqsCommandUrl string
	// Canal para as menagens recebidas para processamento.
	MessageChan chan *internal.MessageContext
	// Número máximo de workers para processar as mensagens da fila AWS SQS.
	MaxWorkers int
	// Repositório para acessar os dados dos agendamentos.
	UserRepository repository.Client
	// Repositório para acessar os eventos de execução do conector.
	EventRepository repository.Event
	// Total maximo de comandos que podem ser executados em paralelo no
	// conector.
	MaxCommands int
}

// Scheduler é a estrutura principal que gerencia o processo de consumo e
// processamento de mensagens da fila AWS SQS.
type Scheduler struct {
	// Tracer para criar spans de telemetria durante o processo.
	tracer trace.Tracer
	// SqsConsumer para consumir mensagens da fila AWS SQS.
	consumer *SqsConsumer
	// Worker para processar as mensagens recebidas da fila AWS SQS.
	workers []*Worker
	// Canal para sinalizar o encerramento do processo.
	stopChan chan int
}

// Construtor para criar uma nova instância do Scheduler.
func NewScheduler(config *SchedulerConfig) *Scheduler {
	var workers []*Worker
	configWorker := &WorkerConfig{
		S3Service:       config.S3Service,
		SqsService:      config.SqsService,
		QueueUrl:        config.SqsCommandUrl,
		MessageChan:     config.MessageChan,
		UserRepository:  config.UserRepository,
		EventRepository: config.EventRepository,
		MaxCommands:     config.MaxCommands,
	}
	for i := 0; i < config.MaxWorkers; i++ {
		workers = append(workers, NewWorker(configWorker))
	}
	return &Scheduler{
		tracer: otel.Tracer("Scheduler"),
		consumer: NewSqsConsumer(&SqsConsumerConfig{
			SqsService:  config.SqsService,
			QueueUrl:    config.SqsEventBridgeUrl,
			MessageChan: config.MessageChan,
		}),
		workers:  workers,
		stopChan: make(chan int),
	}
}

// Inicia o processo de consumo e processamento de mensagens da fila AWS SQS.
func (p *Scheduler) Start(ctx context.Context) error {
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
func (p *Scheduler) Stop(ctx context.Context) {
	p.consumer.Stop(ctx)
	// os workers serão encerrados automaticamente quando o canal de
	// mensagens for fechado pelo consumidor
}
