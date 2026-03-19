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
type MemoryDBUserConfig struct {
	// tempo de expiração dos registros
	TTL time.Duration
}

// Define a estrutura do repositório de memória.
type MemoryDBUser struct {
	// banco de dados em memória
	db map[string]*UserRecord
	// configuração do repositório
	config *MemoryDBUserConfig
	// configura o tracer
	tracer trace.Tracer
}

// Cria uma nova instância do repositório de memória.
func NewMemoryDBUser(config *MemoryDBUserConfig) *MemoryDBUser {
	return &MemoryDBUser{
		db:     make(map[string]*UserRecord),
		config: config,
		tracer: otel.Tracer("memorydb.repository"),
	}
}

// Cria um span contextualizado para o banco de dados de memória.
func (p *MemoryDBUser) newSpan(ctx context.Context, operation string, statement string) (context.Context, trace.Span) {
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

// Cria o repositório (não faz nada no caso do MemoryDBUser).
func (p *MemoryDBUser) Create(ctx context.Context) error {
	return nil
}

// Salva o registro na memória.
// Se já houver registro com o mesmo id, ele será substituído.
func (p *MemoryDBUser) Save(ctx context.Context, record *UserRecord) error {
	ctx, span := p.newSpan(ctx, "save", "")
	defer span.End()
	if record.Created.IsZero() {
		record.Created = time.Now()
	} else {
		record.Changed = time.Now()
	}
	if record.Expiration == 0 && p.config.TTL > 0 {
		record.Expiration = time.Now().Add(p.config.TTL).Unix()
	}
	if record.Id == "" {
		record.Id = uuid.New().String()
	}
	if record.Status == "" {
		record.Status = UserStatusActive
	}
	p.db[record.Id] = record
	return nil
}

// Deleta o registro da memória pelo id.
func (p *MemoryDBUser) Delete(ctx context.Context, id string) (record *UserRecord, err error) {
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
func (p *MemoryDBUser) Get(ctx context.Context, id string) (record *UserRecord, err error) {
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
func (p *MemoryDBUser) FindByMft(ctx context.Context, mft string, status string) (records []*UserRecord, err error) {
	ctx, span := p.newSpan(ctx, "query", fmt.Sprintf("mft = %s AND status = %s", mft, status))
	defer span.End()
	expired := make([]string, 0)
	for k, v := range p.db {
		if v.Expiration != 0 && time.Unix(v.Expiration, 0).Before(time.Now()) {
			expired = append(expired, k)
			continue
		}
		if v.Mft == mft && v.Status == status {
			records = append(records, v)
		}
	}
	for _, v := range expired {
		delete(p.db, v)
	}
	return records, nil
}

// Procura registros pelo código da caixa postal.
func (p *MemoryDBUser) FindByMailbox(ctx context.Context, mailbox string, status string) (records []*UserRecord, err error) {
	ctx, span := p.newSpan(ctx, "query", fmt.Sprintf("mailbox = %s AND status = %s", mailbox, status))
	defer span.End()
	expired := make([]string, 0)
	for k, v := range p.db {
		if v.Expiration != 0 && time.Unix(v.Expiration, 0).Before(time.Now()) {
			expired = append(expired, k)
			continue
		}
		if v.Mailbox == mailbox && v.Status == status {
			records = append(records, v)
		}
	}
	for _, v := range expired {
		delete(p.db, v)
	}
	return records, nil
}

// Procura registros pelo código do produto.
func (p *MemoryDBUser) FindByProduct(ctx context.Context, product string, status string) (records []*UserRecord, err error) {
	ctx, span := p.newSpan(ctx, "query", fmt.Sprintf("product = %s AND status = %s", product, status))
	defer span.End()
	expired := make([]string, 0)
	for k, v := range p.db {
		if v.Expiration != 0 && time.Unix(v.Expiration, 0).Before(time.Now()) {
			expired = append(expired, k)
			continue
		}
		if v.Product == product && v.Status == status {
			records = append(records, v)
		}
	}
	for _, v := range expired {
		delete(p.db, v)
	}
	return records, nil
}

// Procura registros pelo código do conector.
func (p *MemoryDBUser) FindByConnector(ctx context.Context, connector string, status string) (records []*UserRecord, err error) {
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
