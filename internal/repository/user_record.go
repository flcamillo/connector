package repository

import (
	"fmt"
	"time"
)

// Define os estados possiveis do usuário
const (
	UserStatusActive   = "active"
	UserStatusInactive = "inactive"
)

// Define a estrutura do registro cliente na tabela DynamoDB.
type UserRecord struct {
	Id                    string    `json:"id,omitempty" dynamodbav:"id"`
	Mft                   string    `json:"codigoMft" dynamodbav:"codigoMft"`
	Mailbox               string    `json:"codigoCaixaPostal" dynamodbav:"codigoCaixaPostal"`
	Product               string    `json:"codigoProduto" dynamodbav:"codigoProduto"`
	Connector             string    `json:"codigoConector" dynamodbav:"codigoConector"`
	LocalPathForUpload    string    `json:"caminhoLocalParaEnvio" dynamodbav:"caminhoLocalParaEnvio"`
	LocalPathForDownload  string    `json:"caminhoLocalParaRecepcao" dynamodbav:"caminhoLocalParaRecepcao"`
	RemotePathForUpload   string    `json:"caminhoRemotoParaEnvio" dynamodbav:"caminhoRemotoParaEnvio"`
	RemotePathForDownload string    `json:"caminhoRemotoParaRecepcao" dynamodbav:"caminhoRemotoParaRecepcao"`
	UploadEnabled         bool      `json:"envioHabilitado" dynamodbav:"envioHabilitado"`
	DownloadEnabled       bool      `json:"recepcaoHabilitado" dynamodbav:"recepcaoHabilitado"`
	FileNameFilter        string    `json:"filtroNomeArquivo" dynamodbav:"filtroNomeArquivo"`
	RemoveAfterDownload   bool      `json:"removerAposRecebimento" dynamodbav:"removerAposRecebimento"`
	StartInterval         int       `json:"intervaloInicio" dynamodbav:"intervaloInicio"`
	Expiration            int64     `json:"expiracao" dynamodbav:"expiracao"`
	Status                string    `json:"estado" dynamodbav:"estado"`
	Created               time.Time `json:"dataCadastro" dynamodbav:"dataCadastro"`
	CreatedBy             string    `json:"cadastradoPor" dynamodbav:"cadastradoPor"`
	Changed               time.Time `json:"dataAlteracao" dynamodbav:"dataAlteracao"`
	ChangedBy             string    `json:"alteradoPor" dynamodbav:"alteradoPor"`
}

// Valida os campos do registro.
func (p *UserRecord) Validate() error {
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
	if p.DownloadEnabled {
		if p.RemotePathForUpload == "" {
			return fmt.Errorf("invalid remote path for send")
		}
		if p.LocalPathForDownload == "" {
			return fmt.Errorf("invalid local path for receive")
		}
	}
	if p.UploadEnabled {
		if p.RemotePathForDownload == "" {
			return fmt.Errorf("invalid remote path for receive")
		}
		if p.LocalPathForUpload == "" {
			return fmt.Errorf("invalid local path for send")
		}
	}
	return nil
}
