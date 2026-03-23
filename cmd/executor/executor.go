package main

import (
	"awsconnector/internal"
	"awsconnector/internal/repository"
	"context"
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
	// Configuração gerais.
	config *ExecutorConfig
	// Tracer para criar spans de telemetria durante o processo.
	tracer trace.Tracer
	// SqsConsumer para consumir mensagens da fila AWS SQS.
	consumer *SqsConsumer
	// Worker para processar as mensagens recebidas da fila AWS SQS.
	workers []*Worker
	// Função para cancelar a execução.
	cancel context.CancelFunc
}

// Construtor para criar uma nova instância do Executor.
func NewExecutor(config *ExecutorConfig) *Executor {
	var workers []*Worker
	configWorker := &WorkerConfig{
		S3Service:         config.S3Service,
		MessageChan:       config.MessageChan,
		EventRepository:   config.EventRepository,
		TransferService:   config.TransferService,
		TemporaryFilePath: config.TemporaryFilePath,
	}
	for i := 0; i < config.MaxWorkers; i++ {
		workers = append(workers, NewWorker(configWorker))
	}
	return &Executor{
		config: config,
		tracer: otel.Tracer("Executor"),
		consumer: NewSqsConsumer(&SqsConsumerConfig{
			SqsService:  config.SqsService,
			QueueUrl:    config.QueueUrl,
			MessageChan: config.MessageChan,
		}),
		workers: workers,
	}
}

// Inicia o processo de consumo e processamento de mensagens da fila AWS SQS.
func (p *Executor) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	// agenda uma funcão para aguardar todos os processos terminarem
	// os workers só serão encerrados caso o canal de mensagem
	// seja fechado
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
			worker.Start(ctx)
		})
	}
	// inicia o consumidor em um processo separado
	wg.Go(func() {
		errChan <- p.consumer.Start(ctx)
	})
	// aguarda por erro ou cancelamento
	select {
	case <-ctx.Done():
		return nil
	case err := <-errChan:
		return err
	}
}

// Encerra o processo de consumo e processamento de mensagens da fila AWS SQS.
func (p *Executor) Stop(ctx context.Context) {
	if p.cancel != nil {
		p.cancel()
	}
}
