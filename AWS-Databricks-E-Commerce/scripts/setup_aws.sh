#!/bin/bash
# =============================================================================
# AWS Infrastructure Setup Script (No Terraform)
# Run this to set up S3, Lambda, and API Gateway using AWS CLI
# =============================================================================

set -e

# Configuration
AWS_REGION="${AWS_REGION:-ap-southeast-2}"
S3_BUCKET="ecommerce-landing-zone"
PROJECT_NAME="ecommerce-data-platform"
LAMBDA_FUNCTION_NAME="${PROJECT_NAME}-webhook-handler"
API_NAME="${PROJECT_NAME}-webhook-api"

echo "=========================================="
echo "E-Commerce Data Platform - AWS Setup"
echo "=========================================="
echo "Region: ${AWS_REGION}"
echo "S3 Bucket: ${S3_BUCKET}"
echo ""

# =============================================================================
# 1. CREATE S3 BUCKET
# =============================================================================
echo "1. Creating S3 landing zone bucket..."

# Check if bucket exists
if aws s3api head-bucket --bucket "${S3_BUCKET}" 2>/dev/null; then
    echo "   Bucket ${S3_BUCKET} already exists"
else
    aws s3api create-bucket \
        --bucket "${S3_BUCKET}" \
        --region "${AWS_REGION}" \
        --create-bucket-configuration LocationConstraint="${AWS_REGION}"
    echo "   ✓ Bucket created"
fi

# Enable versioning
aws s3api put-bucket-versioning \
    --bucket "${S3_BUCKET}" \
    --versioning-configuration Status=Enabled
echo "   ✓ Versioning enabled"

# Create folder structure
aws s3api put-object --bucket "${S3_BUCKET}" --key "orders/"
aws s3api put-object --bucket "${S3_BUCKET}" --key "customers/"
aws s3api put-object --bucket "${S3_BUCKET}" --key "products/"
echo "   ✓ Folder structure created"

# =============================================================================
# 2. CREATE IAM ROLE FOR LAMBDA
# =============================================================================
echo ""
echo "2. Creating IAM role for Lambda..."

LAMBDA_ROLE_NAME="${PROJECT_NAME}-lambda-role"

# Create trust policy
cat > /tmp/lambda-trust-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF

# Create role (ignore if exists)
aws iam create-role \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --assume-role-policy-document file:///tmp/lambda-trust-policy.json \
    2>/dev/null || echo "   Role already exists"

# Create policy for S3 and CloudWatch
cat > /tmp/lambda-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::${S3_BUCKET}",
                "arn:aws:s3:::${S3_BUCKET}/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        }
    ]
}
EOF

aws iam put-role-policy \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --policy-name "${PROJECT_NAME}-lambda-policy" \
    --policy-document file:///tmp/lambda-policy.json

LAMBDA_ROLE_ARN=$(aws iam get-role --role-name "${LAMBDA_ROLE_NAME}" --query 'Role.Arn' --output text)
echo "   ✓ Lambda role created: ${LAMBDA_ROLE_ARN}"

# Wait for role to propagate
echo "   Waiting for IAM role to propagate..."
sleep 10

# =============================================================================
# 3. CREATE LAMBDA FUNCTION
# =============================================================================
echo ""
echo "3. Creating Lambda function..."

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAMBDA_CODE_PATH="${SCRIPT_DIR}/../infrastructure/lambda"

# Create deployment package
cd "${LAMBDA_CODE_PATH}"
zip -j /tmp/lambda_function.zip woocommerce_webhook.py

# Create or update Lambda function
if aws lambda get-function --function-name "${LAMBDA_FUNCTION_NAME}" 2>/dev/null; then
    echo "   Updating existing function..."
    aws lambda update-function-code \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --zip-file fileb:///tmp/lambda_function.zip
else
    echo "   Creating new function..."
    aws lambda create-function \
        --function-name "${LAMBDA_FUNCTION_NAME}" \
        --runtime python3.11 \
        --role "${LAMBDA_ROLE_ARN}" \
        --handler woocommerce_webhook.lambda_handler \
        --zip-file fileb:///tmp/lambda_function.zip \
        --timeout 30 \
        --memory-size 256 \
        --environment "Variables={S3_BUCKET=${S3_BUCKET}}"
fi

LAMBDA_ARN=$(aws lambda get-function --function-name "${LAMBDA_FUNCTION_NAME}" --query 'Configuration.FunctionArn' --output text)
echo "   ✓ Lambda function ready: ${LAMBDA_ARN}"

# =============================================================================
# 4. CREATE API GATEWAY
# =============================================================================
echo ""
echo "4. Creating API Gateway..."

# Create HTTP API
API_ID=$(aws apigatewayv2 create-api \
    --name "${API_NAME}" \
    --protocol-type HTTP \
    --query 'ApiId' \
    --output text 2>/dev/null) || \
API_ID=$(aws apigatewayv2 get-apis --query "Items[?Name=='${API_NAME}'].ApiId" --output text)

echo "   API ID: ${API_ID}"

# Create Lambda integration
INTEGRATION_ID=$(aws apigatewayv2 create-integration \
    --api-id "${API_ID}" \
    --integration-type AWS_PROXY \
    --integration-uri "${LAMBDA_ARN}" \
    --payload-format-version "2.0" \
    --query 'IntegrationId' \
    --output text 2>/dev/null) || \
INTEGRATION_ID=$(aws apigatewayv2 get-integrations --api-id "${API_ID}" --query 'Items[0].IntegrationId' --output text)

# Create route
aws apigatewayv2 create-route \
    --api-id "${API_ID}" \
    --route-key "POST /webhook" \
    --target "integrations/${INTEGRATION_ID}" 2>/dev/null || echo "   Route already exists"

# Create/update stage
aws apigatewayv2 create-stage \
    --api-id "${API_ID}" \
    --stage-name '$default' \
    --auto-deploy 2>/dev/null || echo "   Stage already exists"

# Add Lambda permission for API Gateway
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text)
aws lambda add-permission \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --statement-id "AllowAPIGateway" \
    --action "lambda:InvokeFunction" \
    --principal apigateway.amazonaws.com \
    --source-arn "arn:aws:execute-api:${AWS_REGION}:${AWS_ACCOUNT_ID}:${API_ID}/*/*" 2>/dev/null || echo "   Permission already exists"

API_ENDPOINT=$(aws apigatewayv2 get-api --api-id "${API_ID}" --query 'ApiEndpoint' --output text)
echo "   ✓ API Gateway ready"

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
echo "=========================================="
echo "✓ AWS INFRASTRUCTURE SETUP COMPLETE"
echo "=========================================="
echo ""
echo "Resources Created:"
echo "  S3 Bucket:      s3://${S3_BUCKET}"
echo "  Lambda:         ${LAMBDA_FUNCTION_NAME}"
echo "  API Gateway:    ${API_ID}"
echo ""
echo "=========================================="
echo "WEBHOOK ENDPOINT (use this in WooCommerce):"
echo "${API_ENDPOINT}/webhook"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Configure Databricks CLI: databricks configure"
echo "  2. Deploy bundle: databricks bundle deploy"
echo "  3. Add webhook URL to WooCommerce"
echo ""

# Save config to file
cat > "${SCRIPT_DIR}/../.aws-config" << EOF
AWS_REGION=${AWS_REGION}
S3_BUCKET=${S3_BUCKET}
LAMBDA_ARN=${LAMBDA_ARN}
API_ENDPOINT=${API_ENDPOINT}/webhook
EOF

echo "Config saved to .aws-config"
