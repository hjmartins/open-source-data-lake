# variables.tf

variable "aws_region" {
  description = "AWS region (LocalStack)"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used in all resource names"
  type        = string
  default     = "de-project"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}
