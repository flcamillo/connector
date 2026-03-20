###########################################################################################################
# Connector do Transfer Family
###########################################################################################################

# conector
resource "aws_transfer_connector" "local" {
  access_role  = aws_iam_role.transfer_connector_access.arn
  logging_role = aws_iam_role.transfer_connector_log.arn
  sftp_config {
    trusted_host_keys = [local.ssh_trusted_keys]
    user_secret_id    = aws_secretsmanager_secret.connector_user.id
  }
  url = "sftp://${aws_transfer_server.public.endpoint}"
}

# trusted para o serviço do transfer family
data "aws_iam_policy_document" "transfer_connector_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["transfer.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

###########################################################################################################
# Role de acesso do Transfer Family Connector
###########################################################################################################

# trusted para o serviço do transfer family
data "aws_iam_policy_document" "transfer_connector_access" {
  statement {
    sid    = "AllowListBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      "${aws_s3_bucket.connector_upload.arn}",
      "${aws_s3_bucket.connector_download.arn}",
      "${aws_s3_bucket.connector_temporary.arn}"
    ]
  }
  statement {
    sid    = "AllowUploadAccess"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:PutObjectACL",
    ]
    resources = [
      "${aws_s3_bucket.connector_download.arn}/*",
      "${aws_s3_bucket.connector_temporary.arn}/*"
    ]
  }
  statement {
    sid    = "AllowDownloadAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:DeleteObject",
      "s3:DeleteObjectVersion",
      "s3:GetObjectVersion",
      "s3:GetObjectACL",
    ]
    resources = [
      "${aws_s3_bucket.connector_upload.arn}/*"
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
resource "aws_iam_policy" "transfer_connector_access" {
  name        = "transfer_connector_access"
  description = "Transfer Family Connector Access Policy"
  policy      = data.aws_iam_policy_document.transfer_connector_access.json
}

# role para o transfer family gravar logs no cloudwatch
resource "aws_iam_role" "transfer_connector_access" {
  name               = "transfer_connector_access"
  assume_role_policy = data.aws_iam_policy_document.transfer_connector_assume_role.json
}

# anexa uma policy gerenciada da AWS
resource "aws_iam_role_policy_attachment" "transfer_connector_access" {
  role       = aws_iam_role.transfer_connector_access.name
  policy_arn = aws_iam_policy.transfer_connector_access.arn
}

###########################################################################################################
# Role de Log do Transfer Family Connector
###########################################################################################################

# role para o transfer family gravar logs no cloudwatch
resource "aws_iam_role" "transfer_connector_log" {
  name               = "transfer_connector_logging"
  assume_role_policy = data.aws_iam_policy_document.transfer_connector_assume_role.json
}

# anexa uma policy gerenciada da AWS
resource "aws_iam_role_policy_attachment" "transfer_connector_log" {
  role       = aws_iam_role.transfer_connector_log.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSTransferLoggingAccess"
}

###########################################################################################################
# Secret Manager para as credenciais do Transfer Family Connector
###########################################################################################################

# segredo com as credencias do usuário SFTP
resource "aws_secretsmanager_secret" "connector_user" {
  name                    = "sftp/client/${var.connector_user}"
  recovery_window_in_days = 0
}

# valor do segredo do usuário (apenas para teste)
resource "aws_secretsmanager_secret_version" "connector_user" {
  secret_id     = aws_secretsmanager_secret.connector_user.id
  secret_string = jsonencode(local.connector_secret_value)
}
