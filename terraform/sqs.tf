###########################################################################################################
# fila SQS para receber os eventos do Event Bridge relacionados ao AWS Transfer Family Connector
###########################################################################################################

# fila principal
resource "aws_sqs_queue" "sqs_event_bridge" {
  name = var.sqs_event_bridge_name
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.sqs_event_bridge_dlq.arn
    maxReceiveCount     = 4
  })
}

# policy para permitir que o Event Bridge envie mensagens para a fila SQS
data "aws_iam_policy_document" "sqs_event_bridge_policy" {
  statement {
    sid    = "AllowEventBridgeToSendMessage"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.sqs_event_bridge.arn]
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_cloudwatch_event_rule.transfer_connector_event.arn]
    }
  }
}

# dlq
resource "aws_sqs_queue" "sqs_event_bridge_dlq" {
  name = "${var.sqs_event_bridge_name}-dlq"
}

# policy para permitir que a fila SQS envie mensagens para a dlq
resource "aws_sqs_queue_redrive_allow_policy" "sqs_event_bridge_redrive_policy" {
  queue_url = aws_sqs_queue.sqs_event_bridge_dlq.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue",
    sourceQueueArns   = [aws_sqs_queue.sqs_event_bridge.arn]
  })
}

# adiciona a policy na fila
resource "aws_sqs_queue_policy" "sqs_event_bridge" {
  queue_url = aws_sqs_queue.sqs_event_bridge.id
  policy    = data.aws_iam_policy_document.sqs_event_bridge_policy.json
}

###########################################################################################################
# fila SQS para receber os comandos para executar no AWS Transfer Family Connector
###########################################################################################################

# fila principal
resource "aws_sqs_queue" "sqs_command" {
  name = var.sqs_command_name
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.sqs_command_dlq.arn
    maxReceiveCount     = 4
  })
}

# policy para permitir que o Event Bridge envie mensagens para a fila SQS
data "aws_iam_policy_document" "sqs_command_policy" {
  statement {
    sid    = "AllowEventBridgeToSendMessage"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.sqs_command.arn]
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_cloudwatch_event_rule.transfer_connector_start_schedule.arn]
    }
  }
}

# dlq
resource "aws_sqs_queue" "sqs_command_dlq" {
  name = "${var.sqs_command_name}-dlq"
}

# policy para permitir que a fila SQS envie mensagens para a dlq
resource "aws_sqs_queue_redrive_allow_policy" "sqs_command_redrive_policy" {
  queue_url = aws_sqs_queue.sqs_command_dlq.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue",
    sourceQueueArns   = [aws_sqs_queue.sqs_command.arn]
  })
}

# adiciona a policy na fila
resource "aws_sqs_queue_policy" "sqs_command" {
  queue_url = aws_sqs_queue.sqs_command.id
  policy    = data.aws_iam_policy_document.sqs_command_policy.json
}

###########################################################################################################
# fila SQS para receber os comandos da agenda para executar o AWS Transfer Family Connector
###########################################################################################################

# fila principal
resource "aws_sqs_queue" "sqs_scheduler" {
  name = var.sqs_scheduler_name
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.sqs_scheduler_dlq.arn
    maxReceiveCount     = 4
  })
}

# policy para permitir que o Event Bridge envie mensagens para a fila SQS
data "aws_iam_policy_document" "sqs_scheduler_policy" {
  statement {
    sid    = "AllowEventBridgeToSendMessage"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.sqs_scheduler.arn]
  }
}

# dlq
resource "aws_sqs_queue" "sqs_scheduler_dlq" {
  name = "${var.sqs_scheduler_name}-dlq"
}

# policy para permitir que a fila SQS envie mensagens para a dlq
resource "aws_sqs_queue_redrive_allow_policy" "sqs_scheduler_redrive_policy" {
  queue_url = aws_sqs_queue.sqs_scheduler_dlq.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue",
    sourceQueueArns   = [aws_sqs_queue.sqs_scheduler.arn]
  })
}

# adiciona a policy na fila
resource "aws_sqs_queue_policy" "sqs_scheduler" {
  queue_url = aws_sqs_queue.sqs_scheduler.id
  policy    = data.aws_iam_policy_document.sqs_scheduler_policy.json
}
