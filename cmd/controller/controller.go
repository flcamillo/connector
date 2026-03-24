package main

import (
	"awsconnector/internal"
	"awsconnector/internal/repository"
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Define a configuração do Controller.
type ControllerConfig struct {
	// Client AWS S3 para executar operações no bucket.
	S3Service internal.S3Service
	// cliente do AWS SQS para consumir mensagens da fila AWS SQS.
	SqsService internal.SqsService
	// URL da fila AWS SQS para consumir mensagens de resultado de comandos
	// do AWS Transfer Family Connector recebidas do AWS EventBridge.
	SqsEventBridgeUrl string
	// URL da fila AWS SQS para enviar comandos para o AWS Transfer Family
	// Connector.
	SqsCommandUrl string
	// Canal para as menagens recebidas para processamento.
	MessageChan chan *internal.MessageContext
	// Número máximo de workers para processar as mensagens da fila AWS SQS.
	MaxWorkers int
	// Repositório para acessar os eventos de execução do conector.
	EventRepository repository.Event
	// Total maximo de comandos que podem ser executados em paralelo no
	// conector.
	MaxCommands int
}

// Controller é a estrutura principal que gerencia o processo de consumo e
// processamento de mensagens da fila AWS SQS.
type Controller struct {
	// Configuração gerais.
	config *ControllerConfig
	// Tracer para criar spans de telemetria durante o processo.
	tracer trace.Tracer
	// SqsConsumer para consumir mensagens da fila AWS SQS.
	consumer *SqsConsumer
	// Worker para processar as mensagens recebidas da fila AWS SQS.
	workers []*Worker
}

// Construtor para criar uma nova instância do Controller.
func NewController(config *ControllerConfig) *Controller {
	var workers []*Worker
	configWorker := &WorkerConfig{
		S3Service:       config.S3Service,
		SqsService:      config.SqsService,
		QueueUrl:        config.SqsCommandUrl,
		MessageChan:     config.MessageChan,
		EventRepository: config.EventRepository,
		MaxCommands:     config.MaxCommands,
	}
	for i := 0; i < config.MaxWorkers; i++ {
		workers = append(workers, NewWorker(configWorker))
	}
	return &Controller{
		config: config,
		tracer: otel.Tracer("Controller"),
		consumer: NewSqsConsumer(&SqsConsumerConfig{
			SqsService:  config.SqsService,
			QueueUrl:    config.SqsEventBridgeUrl,
			MessageChan: config.MessageChan,
		}),
		workers: workers,
	}
}

// Inicia o processo de consumo e processamento de mensagens da fila AWS SQS.
func (p *Controller) Run(ctx context.Context) error {
	// agenda uma funcão para aguardar todos os processos terminarem
	// os workers só serão encerrados com o fechamando do canal de mensagens
	wg := sync.WaitGroup{}
	defer func() {
		close(p.config.MessageChan)
		wg.Wait()
	}()
	// cria um channel para receber erros dos processos
	errChan := make(chan error, 1)
	// incia todos os workers em processos separados
	for _, worker := range p.workers {
		wg.Go(func() {
			worker.Run(ctx)
		})
	}
	// inicia o consumidor em um processo separado
	wg.Go(func() {
		errChan <- p.consumer.Run(ctx)
	})
	// aguarda por erro ou cancelamento
	select {
	case <-ctx.Done():
		return nil
	case err := <-errChan:
		return err
	}
}
