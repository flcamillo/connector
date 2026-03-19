package internal

import (
	"context"
	"time"
)

// Define a estrutura para representar uma mensagem recebida da fila AWS SQS.
type MessageContext struct {
	Context       context.Context
	Id            string
	ReceiptHandle string
	Body          string
	QueueUrl      string
	Received      time.Time
}
