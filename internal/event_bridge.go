package internal

import "time"

// Estrutura base do evento gerado pelo AWS EventBridge
type EventBridgeEvent struct {
	Version    string              `json:"version"`
	Id         string              `json:"id"`
	DetailType string              `json:"detail-type"`
	Source     string              `json:"source"`
	Account    string              `json:"account"`
	Time       time.Time           `json:"time"`
	Region     string              `json:"region"`
	Resources  []string            `json:"resources"`
	Detail     *SftpConnectorEvent `json:"detail"`
}

// Local do arquivo de saída gerado pela listagem de diretório.
type OutputFileLocation struct {
	Domain string `json:"domain"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

// Local do arquivo local enviado ou recebido.
type LocalFileLocation struct {
	Domain string `json:"domain"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

// Evento do AWS Transfer Family Connector para listagem de diretório com
// sucesso.
type SftpConnectorEvent struct {
	ConnectorId         string              `json:"connector-id"`
	Url                 string              `json:"url"`
	StatusCode          string              `json:"status-code"`
	StartTimestamp      time.Time           `json:"start-timestamp"`
	EndTimestamp        time.Time           `json:"end-timestamp"`
	RemoteDirectoryPath string              `json:"remote-directory-path"`
	MaxItems            int                 `json:"max-items"`
	OutputDirectoryPath string              `json:"output-directory-path"`
	ListingId           string              `json:"listing-id"`
	ItemCount           int                 `json:"item-count"`
	Truncated           bool                `json:"truncated"`
	OutputFileLocation  *OutputFileLocation `json:"output-file-location"`
	FailureCode         string              `json:"failure-code"`
	FailureMessage      string              `json:"failure-message"`
	Bytes               int                 `json:"bytes"`
	FilePath            string              `json:"file-path"`
	LocalFileLocation   *LocalFileLocation  `json:"local-file-location"`
	Operation           string              `json:"operation"`
	TransferId          string              `json:"transfer-id"`
	FileTransferId      string              `json:"file-transfer-id"`
	LocalDirectoryPath  string              `json:"local-directory-path"`
	DeletePath          string              `json:"delete-path"`
	DeleteId            string              `json:"delete-id"`
}
