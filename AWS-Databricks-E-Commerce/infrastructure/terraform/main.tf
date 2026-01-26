# Terraform Configuration for E-Commerce Data Platform
# AWS Infrastructure: S3, Lambda, API Gateway, IAM

terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Variables
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-southeast-2"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "ecommerce-data-platform"
}

variable "s3_bucket_name" {
  description = "S3 landing zone bucket name"
  type        = string
  default     = "ecommerce-landing-zone"
}

variable "databricks_account_id" {
  description = "Databricks AWS account ID for cross-account access"
  type        = string
  default     = ""
}

# Locals
locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# =============================================================================
# S3 LANDING ZONE BUCKET
# =============================================================================

resource "aws_s3_bucket" "landing_zone" {
  bucket = var.s3_bucket_name

  tags = merge(local.common_tags, {
    Name = "E-Commerce Landing Zone"
  })
}

resource "aws_s3_bucket_versioning" "landing_zone" {
  bucket = aws_s3_bucket.landing_zone.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "landing_zone" {
  bucket = aws_s3_bucket.landing_zone.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "landing_zone" {
  bucket = aws_s3_bucket.landing_zone.id

  rule {
    id     = "archive-old-data"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }
}

# Create folder structure
resource "aws_s3_object" "orders_folder" {
  bucket = aws_s3_bucket.landing_zone.id
  key    = "orders/"
}

resource "aws_s3_object" "customers_folder" {
  bucket = aws_s3_bucket.landing_zone.id
  key    = "customers/"
}

resource "aws_s3_object" "products_folder" {
  bucket = aws_s3_bucket.landing_zone.id
  key    = "products/"
}

# =============================================================================
# IAM ROLE FOR LAMBDA
# =============================================================================

resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "lambda_s3_policy" {
  name = "${var.project_name}-lambda-s3-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.landing_zone.arn,
          "${aws_s3_bucket.landing_zone.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# =============================================================================
# LAMBDA FUNCTION
# =============================================================================

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambda/woocommerce_webhook.py"
  output_path = "${path.module}/lambda_function.zip"
}

resource "aws_lambda_function" "webhook_handler" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "${var.project_name}-webhook-handler"
  role             = aws_iam_role.lambda_role.arn
  handler          = "woocommerce_webhook.lambda_handler"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  runtime          = "python3.11"
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      S3_BUCKET          = aws_s3_bucket.landing_zone.id
      WOOCOMMERCE_SECRET = ""  # Set via AWS Secrets Manager in production
    }
  }

  tags = local.common_tags
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.webhook_handler.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.webhook_api.execution_arn}/*/*"
}

# =============================================================================
# API GATEWAY
# =============================================================================

resource "aws_apigatewayv2_api" "webhook_api" {
  name          = "${var.project_name}-webhook-api"
  protocol_type = "HTTP"

  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["POST", "OPTIONS"]
    allow_headers = ["*"]
  }

  tags = local.common_tags
}

resource "aws_apigatewayv2_integration" "lambda_integration" {
  api_id             = aws_apigatewayv2_api.webhook_api.id
  integration_type   = "AWS_PROXY"
  integration_uri    = aws_lambda_function.webhook_handler.invoke_arn
  integration_method = "POST"
}

resource "aws_apigatewayv2_route" "webhook_route" {
  api_id    = aws_apigatewayv2_api.webhook_api.id
  route_key = "POST /webhook"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_integration.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.webhook_api.id
  name        = "$default"
  auto_deploy = true

  tags = local.common_tags
}

# =============================================================================
# IAM ROLE FOR DATABRICKS
# =============================================================================

resource "aws_iam_role" "databricks_s3_role" {
  name = "${var.project_name}-databricks-s3-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = var.databricks_account_id != "" ? var.databricks_account_id : "*"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_account_id
          }
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "databricks_s3_policy" {
  name = "${var.project_name}-databricks-s3-policy"
  role = aws_iam_role.databricks_s3_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.landing_zone.arn,
          "${aws_s3_bucket.landing_zone.arn}/*"
        ]
      }
    ]
  })
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "s3_bucket_name" {
  description = "S3 landing zone bucket name"
  value       = aws_s3_bucket.landing_zone.id
}

output "s3_bucket_arn" {
  description = "S3 landing zone bucket ARN"
  value       = aws_s3_bucket.landing_zone.arn
}

output "webhook_api_endpoint" {
  description = "API Gateway endpoint for WooCommerce webhooks"
  value       = "${aws_apigatewayv2_api.webhook_api.api_endpoint}/webhook"
}

output "lambda_function_arn" {
  description = "Lambda function ARN"
  value       = aws_lambda_function.webhook_handler.arn
}

output "databricks_role_arn" {
  description = "IAM role ARN for Databricks to access S3"
  value       = aws_iam_role.databricks_s3_role.arn
}
