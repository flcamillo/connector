###########################################################################################################
# Cria o servidor SFTP do Transfer Family
###########################################################################################################

# servidor do transfer family
resource "aws_transfer_server" "public" {
  security_policy_name        = "TransferSecurityPolicy-2025-03"
  identity_provider_type      = "AWS_LAMBDA"
  function                    = aws_lambda_function.lambda_idp.arn
  endpoint_type               = "PUBLIC"
  logging_role                = aws_iam_role.transfer_log.arn
  host_key                    = local.ssh_private_key_server
  protocols                   = ["SFTP"]
  sftp_authentication_methods = "PUBLIC_KEY_AND_PASSWORD"
  tags = {
    Name = "public"
  }
}

# grupo de log do cloudwatch
resource "aws_cloudwatch_log_group" "transfer" {
  name              = "/aws/transfer/${aws_transfer_server.public.id}"
  retention_in_days = 7
  tags = {
    ServerName = var.sftp_server_name
  }
}

# trusted para o serviço do transfer family
data "aws_iam_policy_document" "transfer_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["transfer.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

# role para o transfer family gravar logs no cloudwatch
resource "aws_iam_role" "transfer_log" {
  name               = "transfer_logging"
  assume_role_policy = data.aws_iam_policy_document.transfer_assume_role.json
}

# Anexa uma policy gerenciada da AWS
resource "aws_iam_role_policy_attachment" "transfer_log" {
  role       = aws_iam_role.transfer_log.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSTransferLoggingAccess"
}

###########################################################################################################
# Role de acesso do usuário Transfer Family
###########################################################################################################

# trusted para o serviço do transfer family
data "aws_iam_policy_document" "transfer_server_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["transfer.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

# trusted para o serviço do transfer family
data "aws_iam_policy_document" "transfer_server_user_access" {
  statement {
    sid    = "AllowListBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      "${aws_s3_bucket.server_upload.arn}",
      "${aws_s3_bucket.server_download.arn}",
    ]
  }
  statement {
    sid    = "AllowUploadAccess"
    effect = "Allow"
    actions = [
      "s3:PutObject*",
    ]
    resources = [
      "${aws_s3_bucket.server_upload.arn}/*",
    ]
  }
  statement {
    sid    = "AllowDownloadAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject*",
      "s3:DeleteObject*",
    ]
    resources = [
      "${aws_s3_bucket.server_download.arn}/*"
    ]
  }
  statement {
    sid    = "AllowSecretManagerAccess"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
    ]
    resources = ["arn:aws:secretsmanager:*:*:*"]
  }
}

# cria a policy para a role de acesso
resource "aws_iam_policy" "transfer_server_user_access" {
  name        = "transfer_server_user_access"
  description = "Transfer Family User Access Policy"
  policy      = data.aws_iam_policy_document.transfer_server_user_access.json
}

# role para o transfer family gravar logs no cloudwatch
resource "aws_iam_role" "transfer_server_user_access" {
  name               = "transfer_server_user_access"
  assume_role_policy = data.aws_iam_policy_document.transfer_server_assume_role.json
}

# anexa uma policy gerenciada da AWS
resource "aws_iam_role_policy_attachment" "transfer_server_user_access" {
  role       = aws_iam_role.transfer_server_user_access.name
  policy_arn = aws_iam_policy.transfer_server_user_access.arn
}

###########################################################################################################
# Secret Manager para as credenciais do usuário Transfer Family
###########################################################################################################

# segredo com as credencias do usuário SFTP
resource "aws_secretsmanager_secret" "server_user" {
  name = "sftp/server/${var.sftp_server_user}"
  recovery_window_in_days = 0
}

# valor do segredo do usuário (apenas para teste)
resource "aws_secretsmanager_secret_version" "server_user" {
  secret_id     = aws_secretsmanager_secret.server_user.id
  secret_string = jsonencode(local.server_secret_value)
}
