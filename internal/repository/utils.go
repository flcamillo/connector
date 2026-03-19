package repository

import (
	"context"
	"time"
)

// Define a interface do repositório de clientes.
type Client interface {
	Create(ctx context.Context) error
	Save(ctx context.Context, client *UserRecord) error
	Delete(ctx context.Context, id string) (*UserRecord, error)
	Get(ctx context.Context, id string) (*UserRecord, error)
	FindByMft(ctx context.Context, mft string, status string) ([]*UserRecord, error)
	FindByProduct(ctx context.Context, product string, status string) ([]*UserRecord, error)
	FindByMailbox(ctx context.Context, mailbox string, status string) ([]*UserRecord, error)
	FindByConnector(ctx context.Context, connector string, status string) ([]*UserRecord, error)
}

// Define a interface do repositório de eventos.
type Event interface {
	Create(ctx context.Context) error
	Save(ctx context.Context, event *EventRecord) error
	SaveIfStatusIs(ctx context.Context, record *EventRecord, status string) (bool, error)
	Delete(ctx context.Context, id string) (*EventRecord, error)
	Get(ctx context.Context, id string) (*EventRecord, error)
	FindByMftAndCreated(ctx context.Context, mft string, from time.Time, to time.Time) ([]*EventRecord, error)
	FindByProductAndCreated(ctx context.Context, product string, from time.Time, to time.Time) ([]*EventRecord, error)
	FindByMailboxAndCreated(ctx context.Context, mailbox string, from time.Time, to time.Time) ([]*EventRecord, error)
	FindByConnectorAndCreated(ctx context.Context, connector string, from time.Time, to time.Time) ([]*EventRecord, error)
	FindByConnectorAndStatus(ctx context.Context, connector string, status string) ([]*EventRecord, error)
	FindByConnectorEventId(ctx context.Context, connectorEventId string) (*EventRecord, error)
}
