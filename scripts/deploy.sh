#!/bin/bash

# AWS Serverless Data Pipeline Deployment Script
# Enterprise-grade deployment with proper error handling and validation

set -euo pipefail

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PROJECT_NAME="healthcare-claims-pipeline"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Usage function
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Deploy AWS Serverless Data Pipeline

OPTIONS:
    -e, --environment ENV    Environment to deploy to (dev, staging, prod)
    -r, --region REGION      AWS region (default: us-east-1)
    -b, --bucket BUCKET      S3 bucket for deployment artifacts
    -v, --validate-only      Validate templates without deploying
    -c, --cleanup           Clean up failed deployments
    -h, --help              Show this help message

EXAMPLES:
    $0 --environment dev --region us-east-1 --bucket my-deployment-bucket
    $0 --environment prod --region us-west-2 --bucket prod-deployment-bucket --validate-only

EOF
}

# Parse command line arguments
parse_args() {
    ENVIRONMENT=""
    REGION="us-east-1"
    DEPLOYMENT_BUCKET=""
    VALIDATE_ONLY=false
    CLEANUP=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            -e|--environment)
                ENVIRONMENT="$2"
                shift 2
                ;;
            -r|--region)
                REGION="$2"
                shift 2
                ;;
            -b|--bucket)
                DEPLOYMENT_BUCKET="$2"
                shift 2
                ;;
            -v|--validate-only)
                VALIDATE_ONLY=true
                shift
                ;;
            -c|--cleanup)
                CLEANUP=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done

    # Validate required parameters
    if [[ -z "$ENVIRONMENT" ]]; then
        log_error "Environment is required. Use -e or --environment"
        usage
        exit 1
    fi

    if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
        log_error "Environment must be one of: dev, staging, prod"
        exit 1
    fi

    if [[ -z "$DEPLOYMENT_BUCKET" && "$VALIDATE_ONLY" == false ]]; then
        log_error "Deployment bucket is required for deployment. Use -b or --bucket"
        exit 1
    fi
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed"
        exit 1
    fi

    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured or invalid"
        exit 1
    fi

    # Check Node.js for Lambda packaging
    if ! command -v node &> /dev/null; then
        log_error "Node.js is not installed"
        exit 1
    fi

    # Check Python for Glue scripts
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 is not installed"
        exit 1
    fi

    # Check jq for JSON processing
    if ! command -v jq &> /dev/null; then
        log_error "jq is not installed"
        exit 1
    fi

    # Verify AWS region
    if ! aws ec2 describe-regions --region-names "$REGION" &> /dev/null; then
        log_error "Invalid AWS region: $REGION"
        exit 1
    fi

    log_success "Prerequisites check completed"
}

# Validate CloudFormation templates
validate_templates() {
    log_info "Validating CloudFormation templates..."

    local templates=(
        "$PROJECT_ROOT/infrastructure/main.yaml"
        "$PROJECT_ROOT/infrastructure/lambda-functions.yaml"
        "$PROJECT_ROOT/monitoring/cloudwatch-monitoring.yaml"
    )

    for template in "${templates[@]}"; do
        if [[ -f "$template" ]]; then
            log_info "Validating $(basename "$template")..."
            if aws cloudformation validate-template \
                --template-body "file://$template" \
                --region "$REGION" > /dev/null; then
                log_success "$(basename "$template") is valid"
            else
                log_error "$(basename "$template") validation failed"
                exit 1
            fi
        else
            log_warning "Template not found: $template"
        fi
    done

    log_success "Template validation completed"
}

# Package Lambda functions
package_lambda_functions() {
    log_info "Packaging Lambda functions..."

    local lambda_dir="$PROJECT_ROOT/lambda"
    local package_dir="$PROJECT_ROOT/build/lambda-packages"

    # Create package directory
    mkdir -p "$package_dir"

    # Package each Lambda function
    for function_dir in "$lambda_dir"/*/; do
        if [[ -d "$function_dir" ]]; then
            local function_name=$(basename "$function_dir")
            local package_file="$package_dir/${function_name}.zip"

            log_info "Packaging $function_name..."

            # Create temporary directory for packaging
            local temp_dir=$(mktemp -d)
            
            # Copy function code
            cp -r "$function_dir"/* "$temp_dir/"

            # Install dependencies if requirements.txt exists
            if [[ -f "$temp_dir/requirements.txt" ]]; then
                log_info "Installing dependencies for $function_name..."
                pip3 install -r "$temp_dir/requirements.txt" -t "$temp_dir" --quiet
            fi

            # Create zip package
            (cd "$temp_dir" && zip -r "$package_file" . > /dev/null)

            # Clean up
            rm -rf "$temp_dir"

            log_success "Packaged $function_name"
        fi
    done

    log_success "Lambda functions packaging completed"
}

# Upload artifacts to S3
upload_artifacts() {
    log_info "Uploading deployment artifacts to S3..."

    local build_dir="$PROJECT_ROOT/build"
    local s3_prefix="deployments/$ENVIRONMENT/$(date +%Y%m%d-%H%M%S)"

    # Upload Lambda packages
    if [[ -d "$build_dir/lambda-packages" ]]; then
        log_info "Uploading Lambda packages..."
        aws s3 cp "$build_dir/lambda-packages/" "s3://$DEPLOYMENT_BUCKET/$s3_prefix/lambda/" \
            --recursive --region "$REGION"
    fi

    # Upload Glue scripts
    if [[ -d "$PROJECT_ROOT/glue" ]]; then
        log_info "Uploading Glue scripts..."
        aws s3 cp "$PROJECT_ROOT/glue/" "s3://$DEPLOYMENT_BUCKET/$s3_prefix/glue/" \
            --recursive --region "$REGION"
    fi

    # Store deployment metadata
    local metadata_file="$build_dir/deployment-metadata.json"
    cat > "$metadata_file" << EOF
{
    "deployment_id": "$(date +%Y%m%d-%H%M%S)",
    "environment": "$ENVIRONMENT",
    "region": "$REGION",
    "s3_prefix": "$s3_prefix",
    "deployed_by": "$(aws sts get-caller-identity --query 'UserName' --output text)",
    "deployment_time": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "git_commit": "$(git rev-parse HEAD 2>/dev/null || echo 'unknown')"
}
EOF

    aws s3 cp "$metadata_file" "s3://$DEPLOYMENT_BUCKET/$s3_prefix/metadata.json" --region "$REGION"

    # Export S3 prefix for use in CloudFormation
    export ARTIFACTS_S3_PREFIX="$s3_prefix"

    log_success "Artifacts upload completed"
}

# Deploy CloudFormation stacks
deploy_infrastructure() {
    log_info "Deploying infrastructure..."

    local stack_name="$PROJECT_NAME-$ENVIRONMENT"
    local template_file="$PROJECT_ROOT/infrastructure/main.yaml"

    # Generate unique bucket names
    local data_bucket="$PROJECT_NAME-$ENVIRONMENT-data-$(date +%s)"
    local monitoring_email="devops@company.com"  # This should come from config

    # Deploy main infrastructure stack
    log_info "Deploying main infrastructure stack..."
    
    local parameters=(
        "ParameterKey=Environment,ParameterValue=$ENVIRONMENT"
        "ParameterKey=ProjectName,ParameterValue=$PROJECT_NAME"
        "ParameterKey=DataBucketName,ParameterValue=$data_bucket"
        "ParameterKey=MonitoringEmail,ParameterValue=$monitoring_email"
    )

    if aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --region "$REGION" &> /dev/null; then
        
        log_info "Updating existing stack..."
        aws cloudformation update-stack \
            --stack-name "$stack_name" \
            --template-body "file://$template_file" \
            --parameters "${parameters[@]}" \
            --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
            --region "$REGION"
    else
        log_info "Creating new stack..."
        aws cloudformation create-stack \
            --stack-name "$stack_name" \
            --template-body "file://$template_file" \
            --parameters "${parameters[@]}" \
            --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
            --region "$REGION"
    fi

    # Wait for stack to complete
    log_info "Waiting for stack deployment to complete..."
    aws cloudformation wait stack-update-complete \
        --stack-name "$stack_name" \
        --region "$REGION" || \
    aws cloudformation wait stack-create-complete \
        --stack-name "$stack_name" \
        --region "$REGION"

    log_success "Infrastructure deployment completed"
}

# Deploy Lambda functions
deploy_lambda_functions() {
    log_info "Deploying Lambda functions..."

    local stack_name="$PROJECT_NAME-$ENVIRONMENT-lambda"
    local template_file="$PROJECT_ROOT/infrastructure/lambda-functions.yaml"

    local parameters=(
        "ParameterKey=Environment,ParameterValue=$ENVIRONMENT"
        "ParameterKey=ProjectName,ParameterValue=$PROJECT_NAME"
        "ParameterKey=LambdaCodeBucket,ParameterValue=$DEPLOYMENT_BUCKET"
    )

    if aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --region "$REGION" &> /dev/null; then
        
        aws cloudformation update-stack \
            --stack-name "$stack_name" \
            --template-body "file://$template_file" \
            --parameters "${parameters[@]}" \
            --capabilities CAPABILITY_IAM \
            --region "$REGION"
    else
        aws cloudformation create-stack \
            --stack-name "$stack_name" \
            --template-body "file://$template_file" \
            --parameters "${parameters[@]}" \
            --capabilities CAPABILITY_IAM \
            --region "$REGION"
    fi

    aws cloudformation wait stack-update-complete \
        --stack-name "$stack_name" \
        --region "$REGION" || \
    aws cloudformation wait stack-create-complete \
        --stack-name "$stack_name" \
        --region "$REGION"

    log_success "Lambda functions deployment completed"
}

# Deploy monitoring
deploy_monitoring() {
    log_info "Deploying monitoring and alerting..."

    local stack_name="$PROJECT_NAME-$ENVIRONMENT-monitoring"
    local template_file="$PROJECT_ROOT/monitoring/cloudwatch-monitoring.yaml"
    local monitoring_email="devops@company.com"  # This should come from config

    local parameters=(
        "ParameterKey=Environment,ParameterValue=$ENVIRONMENT"
        "ParameterKey=ProjectName,ParameterValue=$PROJECT_NAME"
        "ParameterKey=AlertEmail,ParameterValue=$monitoring_email"
    )

    if aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --region "$REGION" &> /dev/null; then
        
        aws cloudformation update-stack \
            --stack-name "$stack_name" \
            --template-body "file://$template_file" \
            --parameters "${parameters[@]}" \
            --region "$REGION"
    else
        aws cloudformation create-stack \
            --stack-name "$stack_name" \
            --template-body "file://$template_file" \
            --parameters "${parameters[@]}" \
            --region "$REGION"
    fi

    aws cloudformation wait stack-update-complete \
        --stack-name "$stack_name" \
        --region "$REGION" || \
    aws cloudformation wait stack-create-complete \
        --stack-name "$stack_name" \
        --region "$REGION"

    log_success "Monitoring deployment completed"
}

# Post-deployment configuration
post_deployment_config() {
    log_info "Performing post-deployment configuration..."

    # Configure S3 event notifications
    local data_bucket=$(aws cloudformation describe-stacks \
        --stack-name "$PROJECT_NAME-$ENVIRONMENT" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`DataBucketName`].OutputValue' \
        --output text)

    local lambda_arn=$(aws cloudformation describe-stacks \
        --stack-name "$PROJECT_NAME-$ENVIRONMENT-lambda" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`S3EventTriggerFunctionArn`].OutputValue' \
        --output text)

    if [[ -n "$data_bucket" && -n "$lambda_arn" ]]; then
        log_info "Configuring S3 event notifications..."
        
        # Create notification configuration
        local notification_config='{
            "LambdaConfigurations": [
                {
                    "Id": "HealthcareClaimsProcessing",
                    "LambdaFunctionArn": "'$lambda_arn'",
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [
                                {
                                    "Name": "prefix",
                                    "Value": "claims/"
                                }
                            ]
                        }
                    }
                }
            ]
        }'

        echo "$notification_config" | aws s3api put-bucket-notification-configuration \
            --bucket "$data_bucket" \
            --notification-configuration file:///dev/stdin \
            --region "$REGION"

        log_success "S3 event notifications configured"
    fi

    log_success "Post-deployment configuration completed"
}

# Cleanup function
cleanup_failed_deployment() {
    log_info "Cleaning up failed deployment..."

    local stacks=(
        "$PROJECT_NAME-$ENVIRONMENT-monitoring"
        "$PROJECT_NAME-$ENVIRONMENT-lambda"
        "$PROJECT_NAME-$ENVIRONMENT"
    )

    for stack in "${stacks[@]}"; do
        if aws cloudformation describe-stacks \
            --stack-name "$stack" \
            --region "$REGION" &> /dev/null; then
            
            local stack_status=$(aws cloudformation describe-stacks \
                --stack-name "$stack" \
                --region "$REGION" \
                --query 'Stacks[0].StackStatus' \
                --output text)

            if [[ "$stack_status" =~ FAILED$ ]]; then
                log_info "Deleting failed stack: $stack"
                aws cloudformation delete-stack \
                    --stack-name "$stack" \
                    --region "$REGION"
                
                aws cloudformation wait stack-delete-complete \
                    --stack-name "$stack" \
                    --region "$REGION"
                
                log_success "Deleted stack: $stack"
            fi
        fi
    done

    log_success "Cleanup completed"
}

# Main execution flow
main() {
    log_info "Starting AWS Serverless Data Pipeline deployment"
    log_info "Environment: $ENVIRONMENT"
    log_info "Region: $REGION"
    log_info "Validate only: $VALIDATE_ONLY"

    # Cleanup if requested
    if [[ "$CLEANUP" == true ]]; then
        cleanup_failed_deployment
        exit 0
    fi

    # Check prerequisites
    check_prerequisites

    # Validate templates
    validate_templates

    # Exit if validation only
    if [[ "$VALIDATE_ONLY" == true ]]; then
        log_success "Validation completed successfully"
        exit 0
    fi

    # Create build directory
    mkdir -p "$PROJECT_ROOT/build"

    # Package and upload artifacts
    package_lambda_functions
    upload_artifacts

    # Deploy infrastructure
    deploy_infrastructure
    deploy_lambda_functions
    deploy_monitoring

    # Post-deployment configuration
    post_deployment_config

    # Get deployment outputs
    local dashboard_url=$(aws cloudformation describe-stacks \
        --stack-name "$PROJECT_NAME-$ENVIRONMENT-monitoring" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`DashboardURL`].OutputValue' \
        --output text 2>/dev/null || echo "Not available")

    log_success "Deployment completed successfully!"
    log_info "Dashboard URL: $dashboard_url"
    log_info "Environment: $ENVIRONMENT"
    log_info "Region: $REGION"
}

# Error handling
trap 'log_error "Deployment failed on line $LINENO"' ERR

# Parse arguments and run
parse_args "$@"
main
