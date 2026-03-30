# ============================================================
# security.tf — IAM Roles & Policies (Least-Privilege)
# ============================================================

# ─── IAM Role: Spark ETL ─────────────────────────────────────────────────────
# Spark jobs can only read Bronze, write Silver/Gold

resource "aws_iam_role" "spark_etl" {
  name = "${var.project_name}-${var.environment}-spark-etl"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "spark_etl_s3" {
  name        = "${var.project_name}-${var.environment}-spark-s3"
  description = "Least-privilege S3 access for Spark ETL jobs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadBronze"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetObjectVersion"
        ]
        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*"
        ]
      },
      {
        Sid    = "WriteSilver"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*"
        ]
      },
      {
        Sid    = "WriteGold"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*"
        ]
      },
      {
        # Explicitly DENY all other buckets
        Sid    = "DenyAllOtherBuckets"
        Effect = "Deny"
        Action = "s3:*"
        NotResource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*",
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "spark_etl_s3" {
  role       = aws_iam_role.spark_etl.name
  policy_arn = aws_iam_policy.spark_etl_s3.arn
}

# ─── IAM Role: Airflow Orchestrator ──────────────────────────────────────────
# Airflow needs SQS + limited S3 for coordination

resource "aws_iam_role" "airflow_orchestrator" {
  name = "${var.project_name}-${var.environment}-airflow"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "airflow_sqs" {
  name        = "${var.project_name}-${var.environment}-airflow-sqs"
  description = "Airflow access to SQS queues for event-driven triggers"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "SQSReadWrite"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:SendMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl"
        ]
        Resource = [
          aws_sqs_queue.ingestion_queue.arn,
          aws_sqs_queue.ingestion_dlq.arn
        ]
      },
      {
        Sid    = "SecretsRead"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = aws_secretsmanager_secret.db_credentials.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_sqs" {
  role       = aws_iam_role.airflow_orchestrator.name
  policy_arn = aws_iam_policy.airflow_sqs.arn
}

# ─── IAM Role: Ingestion (write-only to Bronze) ───────────────────────────────
resource "aws_iam_role" "data_ingestion" {
  name = "${var.project_name}-${var.environment}-ingestion"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "ingestion_write_only" {
  name        = "${var.project_name}-${var.environment}-ingestion-write"
  description = "Write-only access to Bronze bucket — no read, no delete"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "WriteOnlyBronze"
        Effect = "Allow"
        Action = [
          "s3:PutObject"
        ]
        Resource = "${aws_s3_bucket.bronze.arn}/*"
        # Additional condition: only allow uploads with encryption
        Condition = {
          StringEquals = {
            "s3:x-amz-server-side-encryption" = "AES256"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ingestion_write_only" {
  role       = aws_iam_role.data_ingestion.name
  policy_arn = aws_iam_policy.ingestion_write_only.arn
}
