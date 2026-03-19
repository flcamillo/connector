# define a policy de acesso para execução da lambda
data "aws_iam_policy_document" "lambda_execution_policy_document" {
  statement {
    sid    = "AllowLogging"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }
  statement {
    sid    = "AllowSecretsManagerAccess"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
    ]
    resources = ["arn:aws:secretsmanager:*:*:*"]
  }
  statement {
    sid    = "AllowUploadAccess"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:PutObjectACL",
    ]
    resources = [
      "${aws_s3_bucket.server_upload.arn}/*",
      "${aws_s3_bucket.server_download.arn}/*",
    ]
  }
  statement {
    sid    = "AllowKeyPermissions"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey",
    ]
    resources = ["arn:aws:kms:*:*:*"]
  }
}

# define a policy para execução da lambda
resource "aws_iam_policy" "lambda_execution_policy" {
  name        = "lambda-${var.lambda_idp_name}-execution-policy"
  description = "Policy for Lambda to access S3 bucket and Secrets Manager"
  policy      = data.aws_iam_policy_document.lambda_execution_policy_document.json
}

# define a policy de confiança para execução da lambda
data "aws_iam_policy_document" "lambda_execution_trusted_policy_document" {
  statement {
    effect = "Allow"
    actions = [
      "sts:AssumeRole",
    ]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# define a role para execução da lambda
resource "aws_iam_role" "lambda_execution_role" {
  name               = "lambda-${var.lambda_idp_name}-execution-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_execution_trusted_policy_document.json
}

# associa a policy na role de execução da lambda
resource "aws_iam_role_policy_attachment" "lambda_execution_policy_attachment" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.lambda_execution_policy.arn
}
