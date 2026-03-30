# outputs.tf

output "bronze_bucket" {
  description = "Bronze S3 bucket name"
  value       = aws_s3_bucket.bronze.bucket
}

output "silver_bucket" {
  description = "Silver S3 bucket name"
  value       = aws_s3_bucket.silver.bucket
}

output "gold_bucket" {
  description = "Gold S3 bucket name"
  value       = aws_s3_bucket.gold.bucket
}

output "ingestion_queue_url" {
  description = "SQS ingestion queue URL"
  value       = aws_sqs_queue.ingestion_queue.url
}

output "spark_etl_role_arn" {
  description = "IAM role ARN for Spark ETL"
  value       = aws_iam_role.spark_etl.arn
}
