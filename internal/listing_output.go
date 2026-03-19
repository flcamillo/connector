package internal

import "time"

// Formato do arquivo de listagem de diretórios gerado pelo AWS Transfer
// Family Connector.
type SftpConnectorOutputDirectoryListing struct {
	Files     []Files `json:"files"`
	Paths     []Paths `json:"paths"`
	Truncated string  `json:"truncated"`
}

// Estrutura de um arquivo da listagem de diretórios gerado pelo AWS Transfer
// Family Connector.
type Files struct {
	FilePath          string    `json:"filePath"`
	ModifiedTimestamp time.Time `json:"modifiedTimestamp"`
	Size              int       `json:"size"`
}

// Estrutura de uma pasta da listagem de diretórios gerado pelo AWS Transfer
// Family Connector.
type Paths struct {
	Path string `json:"path"`
}
