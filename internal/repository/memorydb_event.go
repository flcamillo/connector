package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Define a configuração do repositório de memória.
type MemoryDBEventConfig struct {
	// tempo de expiração dos registros
	TTL time.Duration
}

// Define a estrutura do repositório de memória.
type MemoryDBEvent struct {
	// banco de dados em memória
	db map[string]*EventRecord
	// configuração do repositório
	config *MemoryDBEventConfig
	// configura o tracer
	tracer trace.Tracer
}

// Cria uma nova instância do repositório de memória.
func NewMemoryDBEvent(config *MemoryDBEventConfig) *MemoryDBEvent {
	return &MemoryDBEvent{
		db:     make(map[string]*EventRecord),
		config: config,
		tracer: otel.Tracer("memorydb.repository"),
	}
}

// Cria um span contextualizado para o banco de dados de memória.
func (p *MemoryDBEvent) newSpan(ctx context.Context, operation string, statement string) (context.Context, trace.Span) {
	ctx, span := p.tracer.Start(
		ctx,
		operation,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "memorydb"),
			attribute.String("db.name", "events"),
			attribute.String("db.operation", operation),
		),
	)
	if statement != "" {
		span.SetAttributes(attribute.String("db.statement", statement))
	}
	return ctx, span
}

// Cria o repositório (não faz nada no caso do MemoryDBEvent).
func (p *MemoryDBEvent) Create(ctx context.Context) error {
	return nil
}

// Salva o registro na memória.
// Se já houver registro com o mesmo id, ele será substituído.
func (p *MemoryDBEvent) Save(ctx context.Context, record *EventRecord) error {
	ctx, span := p.newSpan(ctx, "save", "")
	defer span.End()
	if record.Created.IsZero() {
		record.Created = time.Now()
	} else {
		record.Changed = time.Now()
	}
	if record.Changed.IsZero() {
		record.Changed = record.Created
	}
	if record.Expiration == 0 && p.config.TTL > 0 {
		record.Expiration = time.Now().Add(p.config.TTL).Unix()
	}
	if record.Id == "" {
		record.Id = uuid.New().String()
	}
	p.db[record.Id] = record
	return nil
}

// Salva o registro na memória.
// Se já houver registro com o mesmo id, ele será substituído.
func (p *MemoryDBEvent) SaveIfStatusIs(ctx context.Context, record *EventRecord, status string) (bool, error) {
	ctx, span := p.newSpan(ctx, "save", "")
	defer span.End()
	if record.Created.IsZero() {
		record.Created = time.Now()
	}
	if record.Changed.IsZero() {
		record.Changed = record.Created
	}
	if record.Expiration == 0 && p.config.TTL > 0 {
		record.Expiration = time.Now().Add(p.config.TTL).Unix()
	}
	if record.Id == "" {
		record.Id = uuid.New().String()
	}
	if p.db[record.Id].Status == record.Status {
		p.db[record.Id] = record
		return true, nil
	}
	return false, nil
}

// Deleta o registro da memória pelo id.
func (p *MemoryDBEvent) Delete(ctx context.Context, id string) (record *EventRecord, err error) {
	ctx, span := p.newSpan(ctx, "delete", "id = "+id)
	defer span.End()
	record, ok := p.db[id]
	if !ok {
		span.AddEvent("record not found")
		return nil, nil
	}
	delete(p.db, id)
	return record, nil
}

// Recupera o registro da memória pelo id.
func (p *MemoryDBEvent) Get(ctx context.Context, id string) (record *EventRecord, err error) {
	ctx, span := p.newSpan(ctx, "get", "id = "+id)
	defer span.End()
	record, ok := p.db[id]
	if !ok {
		span.AddEvent("record not found")
		return nil, nil
	}
	if record.Expiration != 0 && time.Unix(record.Expiration, 0).Before(time.Now()) {
		delete(p.db, id)
		return nil, nil
	}
	return record, nil
}

// Procura registros pelo código MFT.
func (p *MemoryDBEvent) FindByMftAndCreated(ctx context.Context, mft string, from time.Time, to time.Time) (records []*EventRecord, err error) {
	ctx, span := p.newSpan(ctx, "query", fmt.Sprintf("mft = %s AND changed BETWEEN %s AND %s", mft, from.Format(time.RFC3339), to.Format(time.RFC3339)))
	defer span.End()
	expired := make([]string, 0)
	for k, v := range p.db {
		if v.Expiration != 0 && time.Unix(v.Expiration, 0).Before(time.Now()) {
			expired = append(expired, k)
			continue
		}
		if v.Mft == mft && v.Changed.After(from) && v.Changed.Before(to) {
			records = append(records, v)
		}
	}
	for _, v := range expired {
		delete(p.db, v)
	}
	return records, nil
}

// Procura registros pelo código da caixa postal.
func (p *MemoryDBEvent) FindByMailboxAndCreated(ctx context.Context, mailbox string, from time.Time, to time.Time) (records []*EventRecord, err error) {
	ctx, span := p.newSpan(ctx, "query", fmt.Sprintf("mailbox = %s AND changed BETWEEN %s AND %s", mailbox, from.Format(time.RFC3339), to.Format(time.RFC3339)))
	defer span.End()
	expired := make([]string, 0)
	for k, v := range p.db {
		if v.Expiration != 0 && time.Unix(v.Expiration, 0).Before(time.Now()) {
			expired = append(expired, k)
			continue
		}
		if v.Mailbox == mailbox && v.Changed.After(from) && v.Changed.Before(to) {
			records = append(records, v)
		}
	}
	for _, v := range expired {
		delete(p.db, v)
	}
	return records, nil
}

// Procura registros pelo código do produto.
func (p *MemoryDBEvent) FindByProductAndCreated(ctx context.Context, product string, from time.Time, to time.Time) (records []*EventRecord, err error) {
	ctx, span := p.newSpan(ctx, "query", fmt.Sprintf("product = %s AND changed BETWEEN %s AND %s", product, from.Format(time.RFC3339), to.Format(time.RFC3339)))
	defer span.End()
	expired := make([]string, 0)
	for k, v := range p.db {
		if v.Expiration != 0 && time.Unix(v.Expiration, 0).Before(time.Now()) {
			expired = append(expired, k)
			continue
		}
		if v.Product == product && v.Changed.After(from) && v.Changed.Before(to) {
			records = append(records, v)
		}
	}
	for _, v := range expired {
		delete(p.db, v)
	}
	return records, nil
}

// Procura registros pelo código do conector.
func (p *MemoryDBEvent) FindByConnectorAndCreated(ctx context.Context, connector string, from time.Time, to time.Time) (records []*EventRecord, err error) {
	ctx, span := p.newSpan(ctx, "query", fmt.Sprintf("connector = %s AND changed BETWEEN %s AND %s", connector, from.Format(time.RFC3339), to.Format(time.RFC3339)))
	defer span.End()
	expired := make([]string, 0)
	for k, v := range p.db {
		if v.Expiration != 0 && time.Unix(v.Expiration, 0).Before(time.Now()) {
			expired = append(expired, k)
			continue
		}
		if v.Connector == connector && v.Changed.After(from) && v.Changed.Before(to) {
			records = append(records, v)
		}
	}
	for _, v := range expired {
		delete(p.db, v)
	}
	return records, nil
}

// Procura registros na tabela DynamoDB pelo código do conector e estado.
func (p *MemoryDBEvent) FindByConnectorAndStatus(ctx context.Context, connector string, status string) (records []*EventRecord, err error) {
	ctx, span := p.newSpan(ctx, "query", fmt.Sprintf("connector = %s AND status = %s", connector, status))
	defer span.End()
	expired := make([]string, 0)
	for k, v := range p.db {
		if v.Expiration != 0 && time.Unix(v.Expiration, 0).Before(time.Now()) {
			expired = append(expired, k)
			continue
		}
		if v.Connector == connector && v.Status == status {
			records = append(records, v)
		}
	}
	for _, v := range expired {
		delete(p.db, v)
	}
	return records, nil
}

// Procura registros pelo código do conector.
func (p *MemoryDBEvent) FindByConnectorEventId(ctx context.Context, connectorEventId string) (record *EventRecord, err error) {
	ctx, span := p.newSpan(ctx, "query", fmt.Sprintf("connectorEventID = %s", connectorEventId))
	defer span.End()
	expired := make([]string, 0)
	for k, v := range p.db {
		if v.Expiration != 0 && time.Unix(v.Expiration, 0).Before(time.Now()) {
			expired = append(expired, k)
			continue
		}
		if v.ConnectorEventID == connectorEventId {
			record = v
		}
	}
	for _, v := range expired {
		delete(p.db, v)
	}
	return record, nil
}
