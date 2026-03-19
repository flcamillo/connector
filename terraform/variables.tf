variable "sqs_event_bridge_name" {
  type        = string
  description = "Nome da fila SQS que vai receber os eventos do Event Bridge relacionados ao AWS Transfer Family Connector"
  default     = "sftpclient-controller"
}

variable "sqs_command_name" {
  type        = string
  description = "Nome da fila SQS que vai receber os comandos para executar no AWS Transfer Family Connector"
  default     = "sftpclient-executor"
}

variable "sqs_scheduler_name" {
  type        = string
  description = "Nome da fila SQS que vai receber os comandos de agendamento para executar no AWS Transfer Family Connector"
  default     = "sftpclient-scheduler"
}

variable "connector_user" {
  type        = string
  description = "Nome do usuário de SFTP do Transfer Family Connector"
  default     = "localuser"
}

variable "connector_password" {
  type        = string
  description = "Senha do usuário de SFTP do Transfer Family Connector"
  default     = "teste"
}

variable "lambda_idp_name" {
  type        = string
  description = "Nome da Lambda para o Custom IDP do Servidor do Transfer Family"
  default     = "sftp-custom-idp"
}

variable "sftp_server_name" {
  type        = string
  description = "Nome do Servidor do Transfer Family"
  default     = "sftp-public"
}
variable "sftp_server_user" {
  type        = string
  description = "Nome do usuário de SFTP do Transfer Family"
  default     = "localuser"
}

variable "sftp_server_password" {
  type        = string
  description = "Senha do usuário de SFTP do Transfer Family"
  default     = "teste"
}
