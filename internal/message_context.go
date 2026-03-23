package internal

import (
	"context"
	"time"
)

// Define a estrutura para representar uma mensagem recebida da fila AWS SQS.
type MessageContext struct {
	Context  context.Context
	Id       string
	Body     string
	Received time.Time
	Commit   func() error
}
