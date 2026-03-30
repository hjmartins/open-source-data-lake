# ============================================================
# Terraform — LocalStack Infrastructure (simulates AWS)
# Provisions: S3 buckets, IAM roles/policies, SQS queues
# ============================================================

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# ─── Provider: points to LocalStack instead of real AWS ─────────────────────
provider "aws" {
  region                      = var.aws_region
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3             = "http://localhost:4566"
    iam            = "http://localhost:4566"
    sqs            = "http://localhost:4566"
    cloudwatch     = "http://localhost:4566"
    secretsmanager = "http://localhost:4566"
  }

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      CostCenter  = "DataEngineering"
    }
  }
}

# ─── Data Lake S3 Buckets ────────────────────────────────────────────────────

# Bronze layer — raw ingested data
resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_name}-${var.environment}-bronze"
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"   # SSE-S3 encryption
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Cost optimisation: lifecycle rule moves raw data to cheaper storage
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "expire-raw-data"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"  # Infrequent Access after 30d
    }
    transition {
      days          = 90
      storage_class = "GLACIER"       # Archive after 90d
    }
    expiration {
      days = 365                       # Delete after 1 year
    }
  }
}

# Silver layer — cleaned / transformed data
resource "aws_s3_bucket" "silver" {
  bucket = "${var.project_name}-${var.environment}-silver"
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Gold layer — aggregated / business-ready data
resource "aws_s3_bucket" "gold" {
  bucket = "${var.project_name}-${var.environment}-gold"
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ─── SQS Queue for event-driven ingestion ────────────────────────────────────
resource "aws_sqs_queue" "ingestion_queue" {
  name                       = "${var.project_name}-${var.environment}-ingestion"
  visibility_timeout_seconds = 300
  message_retention_seconds  = 86400  # 24 hours

  tags = {
    Component = "Ingestion"
  }
}

resource "aws_sqs_queue" "ingestion_dlq" {
  name                      = "${var.project_name}-${var.environment}-ingestion-dlq"
  message_retention_seconds = 1209600  # 14 days

  tags = {
    Component = "DeadLetterQueue"
  }
}

# ─── Secrets Manager ──────────────────────────────────────────────────────────
resource "aws_secretsmanager_secret" "db_credentials" {
  name        = "${var.project_name}/${var.environment}/db-credentials"
  description = "Database credentials for the data pipeline"
}

resource "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = aws_secretsmanager_secret.db_credentials.id
  secret_string = jsonencode({
    username = "pipeline_user"
    password = "REPLACE_IN_PRODUCTION"
    host     = "postgres"
    port     = 5432
    database = "datawarehouse"
  })
}
