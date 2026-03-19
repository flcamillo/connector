package repository

import (
	"fmt"
	"time"
)

// Define a estrutura do registro de evento na tabela DynamoDB.
type EventRecord struct {
	Id                  string    `json:"id,omitempty" dynamodbav:"id"`
	Mft                 string    `json:"codigoMft" dynamodbav:"codigoMft"`
	Mailbox             string    `json:"codigoCaixaPostal" dynamodbav:"codigoCaixaPostal"`
	Product             string    `json:"codigoProduto" dynamodbav:"codigoProduto"`
	Connector           string    `json:"codigoConector" dynamodbav:"codigoConector"`
	ConnectorEventID    string    `json:"codigoEventoConector" dynamodbav:"codigoEventoConector"`
	LocalPath           string    `json:"caminhoLocal" dynamodbav:"caminhoLocal"`
	RemotePath          string    `json:"caminhoRemoto" dynamodbav:"caminhoRemoto"`
	FileNameFilter      string    `json:"filtroNomeArquivo" dynamodbav:"filtroNomeArquivo"`
	RemoveAfterDownload bool      `json:"removerAposRecebimento" dynamodbav:"removerAposRecebimento"`
	FileSize            int64     `json:"tamanhoArquivo" dynamodbav:"tamanhoArquivo"`
	Expiration          int64     `json:"expiracao" dynamodbav:"expiracao"`
	Command             string    `json:"comando" dynamodbav:"comando"`
	Status              string    `json:"estado" dynamodbav:"estado"`
	ReturnCode          int       `json:"codigoRetorno" dynamodbav:"codigoRetorno"`
	ReturnMessage       string    `json:"mensagemRetorno" dynamodbav:"mensagemRetorno"`
	Created             time.Time `json:"dataCadastro" dynamodbav:"dataCadastro"`
	Changed             time.Time `json:"dataAlteracao" dynamodbav:"dataAlteracao"`
}

// Valida os campos do registro.
func (p *EventRecord) Validate() error {
	if p.Mft == "" {
		return fmt.Errorf("invalid mft")
	}
	if p.Mailbox == "" {
		return fmt.Errorf("invalid mailbox")
	}
	if p.Product == "" {
		return fmt.Errorf("invalid product")
	}
	if p.Connector == "" {
		return fmt.Errorf("invalid connector")
	}
	if p.Command == "" {
		return fmt.Errorf("invalid command")
	}
	return nil
}
