###########################################################################################################
# Bucket para Upload de arquivos do connector
###########################################################################################################

# cria o bucket
resource "aws_s3_bucket" "connector_upload" {
  bucket = "${data.aws_caller_identity.current.account_id}-connector-upload"
}

# define o bucket como privado
resource "aws_s3_bucket_public_access_block" "connector_upload" {
  bucket                  = aws_s3_bucket.connector_upload.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# configura o ciclo de vida dos objetos do bucket
resource "aws_s3_bucket_lifecycle_configuration" "connector_upload" {
  bucket = aws_s3_bucket.connector_upload.id
  rule {
    id = "delete_after_1day"
    expiration {
      days = 1
    }
    status = "Enabled"
  }
}

###########################################################################################################
# Bucket para Download de arquivos do connector
###########################################################################################################

# cria o bucket
resource "aws_s3_bucket" "connector_download" {
  bucket = "${data.aws_caller_identity.current.account_id}-connector-download"
}

# define o bucket como privado
resource "aws_s3_bucket_public_access_block" "connector_download" {
  bucket                  = aws_s3_bucket.connector_download.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# configura o ciclo de vida dos objetos do bucket
resource "aws_s3_bucket_lifecycle_configuration" "connector_download" {
  bucket = aws_s3_bucket.connector_download.id
  rule {
    id = "delete_after_1day"
    expiration {
      days = 1
    }
    status = "Enabled"
  }
}

###########################################################################################################
# Bucket para arquivos temporários do connector
###########################################################################################################

# cria o bucket
resource "aws_s3_bucket" "connector_temporary" {
  bucket = "${data.aws_caller_identity.current.account_id}-connector-temporary"
}

# define o bucket como privado
resource "aws_s3_bucket_public_access_block" "connector_temporary" {
  bucket                  = aws_s3_bucket.connector_temporary.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# configura o ciclo de vida dos objetos do bucket
resource "aws_s3_bucket_lifecycle_configuration" "connector_temporary" {
  bucket = aws_s3_bucket.connector_temporary.id
  rule {
    id = "delete_after_1day"
    expiration {
      days = 1
    }
    status = "Enabled"
  }
}

###########################################################################################################
# Bucket para Upload de arquivos do servidor
###########################################################################################################

# cria o bucket
resource "aws_s3_bucket" "server_upload" {
  bucket = "${data.aws_caller_identity.current.account_id}-server-upload"
}

# define o bucket como privado
resource "aws_s3_bucket_public_access_block" "upload" {
  bucket                  = aws_s3_bucket.server_upload.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# configura o ciclo de vida dos objetos do bucket
resource "aws_s3_bucket_lifecycle_configuration" "server_upload" {
  bucket = aws_s3_bucket.server_upload.id
  rule {
    id = "delete_after_1day"
    expiration {
      days = 1
    }
    status = "Enabled"
  }
}

###########################################################################################################
# Bucket para Download de arquivos
###########################################################################################################

# cria o bucket
resource "aws_s3_bucket" "server_download" {
  bucket = "${data.aws_caller_identity.current.account_id}-server-download"
}

# define o bucket como privado
resource "aws_s3_bucket_public_access_block" "server_download" {
  bucket                  = aws_s3_bucket.server_download.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# configura o ciclo de vida dos objetos do bucket
resource "aws_s3_bucket_lifecycle_configuration" "server_download" {
  bucket = aws_s3_bucket.server_download.id
  rule {
    id = "delete_after_1day"
    expiration {
      days = 1
    }
    status = "Enabled"
  }
}
