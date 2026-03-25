package internal

import (
	"context"
	"time"
)

// Define a estrutura para representar uma mensagem recebida de uma fila.
type MessageContext struct {
	// Contexto de origem da recepção da mensagem.
	Context context.Context
	// Código de identificação da mensagem.
	Id string
	// Dados da mensagem recebida.
	Body string
	// Data de recebimento.
	Received time.Time
	// Origem da mensagem (Kafka, AWS SQS, etc)
	Source string
	// Função para execução do commit da mensagem.
	Commit func(ctx context.Context) error
}
