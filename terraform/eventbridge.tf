###########################################################################################################
# Captura os eventos do Conector do AWS Transfer Family e envia para o SQS
###########################################################################################################
resource "aws_cloudwatch_event_rule" "transfer_connector_event" {
  name        = "transfer-connector-event"
  description = "Captura eventos das acões realizadas pelo Transfer Family Connector"
  event_pattern = jsonencode({
    source = ["aws.transfer"]
    detail-type = [
      "SFTP Connector Remote Delete Failed",
      "SFTP Connector Remote Delete Completed",
      "SFTP Connector Remote Move Failed",
      "SFTP Connector Remote Move Completed",
      "SFTP Connector Directory Listing Failed",
      "SFTP Connector Directory Listing Completed",
      "SFTP Connector File Retrieve Failed",
      "SFTP Connector File Retrieve Completed",
      "SFTP Connector File Send Failed",
      "SFTP Connector File Send Completed"
    ]
  })
}

resource "aws_cloudwatch_event_target" "transfer_connector_event_to_sqs" {
  rule      = aws_cloudwatch_event_rule.transfer_connector_event.name
  target_id = "SendToSQS"
  arn       = aws_sqs_queue.sqs_event_bridge.arn
}

###########################################################################################################
# Cria uma agenda para enviar uma mensagem a cada 5min para a Fila SQS do agendador do Transfer Family Connector
###########################################################################################################
resource "aws_cloudwatch_event_rule" "transfer_connector_start_schedule" {
  name                = "transfer-connector-start-schedule"
  description         = "Inicia a agenda do Transfer Family Connector"
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "transfer_connector_start_schedule_to_sqs" {
  rule      = aws_cloudwatch_event_rule.transfer_connector_start_schedule.name
  target_id = "SendToSQS"
  arn       = aws_sqs_queue.sqs_event_bridge.arn
  input = jsonencode({
    codigoComando = "Start"
    codigoMft     = "SFTPClient"
  })
}
