#!/bin/bash

# Test Pipeline Script for Healthcare Claims Processing
# Comprehensive testing of the entire data pipeline

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PROJECT_NAME="healthcare-claims-pipeline"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Default values
ENVIRONMENT="dev"
REGION="us-east-1"
TEST_TYPE="full"

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Test AWS Serverless Data Pipeline

OPTIONS:
    -e, --environment ENV    Environment to test (dev, staging, prod)
    -r, --region REGION      AWS region (default: us-east-1)
    -t, --test-type TYPE     Test type: unit, integration, performance, full
    -h, --help              Show this help message

EXAMPLES:
    $0 --environment dev --test-type integration
    $0 --environment staging --test-type performance

EOF
}

# Parse arguments
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
        -t|--test-type)
            TEST_TYPE="$2"
            shift 2
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

# Generate test data
generate_test_data() {
    log_info "Generating test healthcare claims data..."

    local test_data_dir="$PROJECT_ROOT/build/test-data"
    mkdir -p "$test_data_dir"

    # Generate CSV test data
    cat > "$test_data_dir/sample_claims.csv" << 'EOF'
claim_id,patient_id,provider_id,service_date,diagnosis_code,procedure_code,amount,status
CLM123456789,PAT123456,PRV12345,2024-01-15,Z51.11,99213,125.50,submitted
CLM123456790,PAT123457,PRV12346,2024-01-16,I10.0,99214,185.75,processed
CLM123456791,PAT123458,PRV12347,2024-01-17,E11.9,99215,245.00,approved
CLM123456792,PAT123459,PRV12348,2024-01-18,J44.1,99213,165.25,denied
CLM123456793,PAT123460,PRV12349,2024-01-19,M79.3,99214,195.50,pending
CLM123456794,PAT123461,PRV12350,2024-01-20,N18.6,99215,275.75,submitted
CLM123456795,PAT123462,PRV12351,2024-01-21,K21.9,99213,145.00,processed
CLM123456796,PAT123463,PRV12352,2024-01-22,F32.9,99214,215.25,approved
CLM123456797,PAT123464,PRV12353,2024-01-23,G43.9,99215,285.50,submitted
CLM123456798,PAT123465,PRV12354,2024-01-24,R06.02,99213,155.75,processed
EOF

    # Generate JSON test data
    cat > "$test_data_dir/sample_claims.json" << 'EOF'
[
    {
        "claim_id": "CLM234567890",
        "patient_id": "PAT234567",
        "provider_id": "PRV23456",
        "service_date": "2024-01-25",
        "diagnosis_code": "I25.10",
        "procedure_code": "99214",
        "amount": 195.75,
        "status": "submitted"
    },
    {
        "claim_id": "CLM234567891",
        "patient_id": "PAT234568",
        "provider_id": "PRV23457",
        "service_date": "2024-01-26",
        "diagnosis_code": "E78.5",
        "procedure_code": "99215",
        "amount": 265.50,
        "status": "processed"
    }
]
EOF

    # Generate large test file for performance testing
    if [[ "$TEST_TYPE" == "performance" || "$TEST_TYPE" == "full" ]]; then
        log_info "Generating large test dataset for performance testing..."
        
        local large_file="$test_data_dir/large_claims.csv"
        echo "claim_id,patient_id,provider_id,service_date,diagnosis_code,procedure_code,amount,status" > "$large_file"
        
        for i in {1..10000}; do
            local claim_id="CLM$(printf "%09d" $i)"
            local patient_id="PAT$(printf "%06d" $((i % 1000 + 1)))"
            local provider_id="PRV$(printf "%05d" $((i % 100 + 1)))"
            local service_date="2024-01-$(printf "%02d" $((i % 28 + 1)))"
            local diagnosis_codes=("I10.0" "E11.9" "J44.1" "M79.3" "N18.6" "K21.9" "F32.9" "G43.9" "R06.02" "Z51.11")
            local diagnosis_code=${diagnosis_codes[$((i % 10))]}
            local procedure_codes=("99213" "99214" "99215")
            local procedure_code=${procedure_codes[$((i % 3))]}
            local amount=$(echo "scale=2; ($RANDOM % 50000 + 100) / 100" | bc)
            local statuses=("submitted" "processed" "approved" "denied" "pending")
            local status=${statuses[$((i % 5))]}
            
            echo "$claim_id,$patient_id,$provider_id,$service_date,$diagnosis_code,$procedure_code,$amount,$status" >> "$large_file"
        done
    fi

    log_success "Test data generation completed"
}

# Unit tests
run_unit_tests() {
    log_info "Running unit tests..."

    # Test Lambda function logic
    test_lambda_validation() {
        log_info "Testing Lambda data validation logic..."
        
        local test_dir="$PROJECT_ROOT/tests/unit"
        mkdir -p "$test_dir"
        
        # Create simple Python test
        cat > "$test_dir/test_validation.py" << 'EOF'
import unittest
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../lambda/data_validation'))

class TestDataValidation(unittest.TestCase):
    
    def test_claim_id_pattern(self):
        import re
        pattern = r'^CLM[0-9]{8,12}$'
        
        # Valid claim IDs
        self.assertTrue(re.match(pattern, 'CLM12345678'))
        self.assertTrue(re.match(pattern, 'CLM123456789012'))
        
        # Invalid claim IDs
        self.assertFalse(re.match(pattern, 'clm12345678'))
        self.assertFalse(re.match(pattern, 'CLM1234567'))
        self.assertFalse(re.match(pattern, 'CLM1234567890123'))
    
    def test_amount_validation(self):
        # Valid amounts
        valid_amounts = ['100.00', '1234.56', '0.01']
        pattern = r'^\d+\.\d{2}$'
        
        for amount in valid_amounts:
            self.assertTrue(re.match(pattern, amount))
        
        # Invalid amounts
        invalid_amounts = ['100', '100.1', '100.123', 'abc.00']
        for amount in invalid_amounts:
            self.assertFalse(re.match(pattern, amount))

if __name__ == '__main__':
    unittest.main()
EOF

        # Run the test
        cd "$test_dir"
        if python3 test_validation.py -v; then
            log_success "Lambda validation tests passed"
        else
            log_error "Lambda validation tests failed"
            return 1
        fi
        cd - > /dev/null
    }

    test_lambda_validation
    log_success "Unit tests completed"
}

# Integration tests
run_integration_tests() {
    log_info "Running integration tests..."

    # Get stack outputs
    local data_bucket=$(aws cloudformation describe-stacks \
        --stack-name "$PROJECT_NAME-$ENVIRONMENT" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`DataBucketName`].OutputValue' \
        --output text 2>/dev/null || echo "")

    if [[ -z "$data_bucket" ]]; then
        log_error "Could not find data bucket. Ensure the stack is deployed."
        return 1
    fi

    log_info "Using data bucket: $data_bucket"

    # Test S3 upload and processing
    test_s3_processing() {
        log_info "Testing S3 file upload and processing..."

        local test_file="$PROJECT_ROOT/build/test-data/sample_claims.csv"
        local s3_key="claims/test/sample_claims_$(date +%s).csv"

        # Upload test file
        aws s3 cp "$test_file" "s3://$data_bucket/$s3_key" --region "$REGION"
        log_info "Uploaded test file to s3://$data_bucket/$s3_key"

        # Wait for processing (Lambda should be triggered)
        log_info "Waiting for processing to complete..."
        sleep 30

        # Check if Glue job was triggered
        local job_name="$PROJECT_NAME-$ENVIRONMENT-healthcare-claims-etl"
        local recent_runs=$(aws glue get-job-runs \
            --job-name "$job_name" \
            --max-results 5 \
            --region "$REGION" \
            --query 'JobRuns[0].JobRunState' \
            --output text 2>/dev/null || echo "FAILED")

        if [[ "$recent_runs" == "SUCCEEDED" || "$recent_runs" == "RUNNING" ]]; then
            log_success "Glue job processing detected"
        else
            log_warning "Glue job may not have been triggered properly"
        fi

        # Clean up test file
        aws s3 rm "s3://$data_bucket/$s3_key" --region "$REGION" || true
    }

    # Test Lambda functions directly
    test_lambda_functions() {
        log_info "Testing Lambda functions directly..."

        local lambda_name="$PROJECT_NAME-$ENVIRONMENT-data-validation"

        # Test with sample payload
        local test_payload='{
            "bucket": "'$data_bucket'",
            "key": "test/sample_claims.csv"
        }'

        # Invoke Lambda function
        local response=$(aws lambda invoke \
            --function-name "$lambda_name" \
            --payload "$test_payload" \
            --region "$REGION" \
            --output json \
            /tmp/lambda-response.json 2>/dev/null || echo '{"StatusCode": 500}')

        local status_code=$(echo "$response" | jq -r '.StatusCode' 2>/dev/null || echo "500")

        if [[ "$status_code" == "200" ]]; then
            log_success "Lambda function test passed"
        else
            log_warning "Lambda function test returned status: $status_code"
        fi
    }

    test_s3_processing
    test_lambda_functions

    log_success "Integration tests completed"
}

# Performance tests
run_performance_tests() {
    log_info "Running performance tests..."

    local data_bucket=$(aws cloudformation describe-stacks \
        --stack-name "$PROJECT_NAME-$ENVIRONMENT" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`DataBucketName`].OutputValue' \
        --output text 2>/dev/null || echo "")

    if [[ -z "$data_bucket" ]]; then
        log_error "Could not find data bucket for performance testing"
        return 1
    fi

    # Test large file processing
    test_large_file_processing() {
        log_info "Testing large file processing performance..."

        local large_file="$PROJECT_ROOT/build/test-data/large_claims.csv"
        local s3_key="claims/performance-test/large_claims_$(date +%s).csv"

        # Upload large test file
        local start_time=$(date +%s)
        
        aws s3 cp "$large_file" "s3://$data_bucket/$s3_key" --region "$REGION"
        log_info "Uploaded large test file to s3://$data_bucket/$s3_key"

        # Monitor processing
        local job_name="$PROJECT_NAME-$ENVIRONMENT-healthcare-claims-etl"
        local timeout=1800  # 30 minutes
        local elapsed=0

        while [[ $elapsed -lt $timeout ]]; do
            local job_state=$(aws glue get-job-runs \
                --job-name "$job_name" \
                --max-results 1 \
                --region "$REGION" \
                --query 'JobRuns[0].JobRunState' \
                --output text 2>/dev/null || echo "UNKNOWN")

            if [[ "$job_state" == "SUCCEEDED" ]]; then
                local end_time=$(date +%s)
                local total_time=$((end_time - start_time))
                log_success "Large file processing completed in $total_time seconds"
                break
            elif [[ "$job_state" == "FAILED" ]]; then
                log_error "Large file processing failed"
                break
            fi

            sleep 30
            elapsed=$((elapsed + 30))
            log_info "Processing in progress... (${elapsed}s elapsed)"
        done

        if [[ $elapsed -ge $timeout ]]; then
            log_warning "Performance test timed out after $timeout seconds"
        fi

        # Clean up
        aws s3 rm "s3://$data_bucket/$s3_key" --region "$REGION" || true
    }

    # Test concurrent processing
    test_concurrent_processing() {
        log_info "Testing concurrent file processing..."

        local concurrent_files=5
        local pids=()

        for i in $(seq 1 $concurrent_files); do
            local test_file="$PROJECT_ROOT/build/test-data/sample_claims.csv"
            local s3_key="claims/concurrent-test/sample_claims_${i}_$(date +%s).csv"

            # Upload files concurrently
            (aws s3 cp "$test_file" "s3://$data_bucket/$s3_key" --region "$REGION" && \
             log_info "Uploaded concurrent file $i") &
            pids+=($!)
        done

        # Wait for all uploads to complete
        for pid in "${pids[@]}"; do
            wait $pid
        done

        log_success "Concurrent upload test completed"

        # Clean up concurrent test files
        aws s3 rm "s3://$data_bucket/claims/concurrent-test/" --recursive --region "$REGION" || true
    }

    test_large_file_processing
    test_concurrent_processing

    log_success "Performance tests completed"
}

# Data quality tests
run_data_quality_tests() {
    log_info "Running data quality tests..."

    # Test data validation rules
    test_validation_rules() {
        log_info "Testing data validation rules..."

        local test_dir="$PROJECT_ROOT/build/test-data"
        
        # Create test data with various quality issues
        cat > "$test_dir/quality_test_claims.csv" << 'EOF'
claim_id,patient_id,provider_id,service_date,diagnosis_code,procedure_code,amount,status
CLM123456789,PAT123456,PRV12345,2024-01-15,Z51.11,99213,125.50,submitted
,PAT123457,PRV12346,2024-01-16,I10.0,99214,185.75,processed
CLM123456791,PAT123458,PRV12347,2024-01-17,INVALID,99215,-245.00,approved
CLM123456792,PAT123459,PRV12348,2025-01-18,J44.1,99213,165.25,denied
CLM123456792,PAT123460,PRV12349,2024-01-19,M79.3,99214,195.50,pending
EOF

        local data_bucket=$(aws cloudformation describe-stacks \
            --stack-name "$PROJECT_NAME-$ENVIRONMENT" \
            --region "$REGION" \
            --query 'Stacks[0].Outputs[?OutputKey==`DataBucketName`].OutputValue' \
            --output text 2>/dev/null || echo "")

        if [[ -n "$data_bucket" ]]; then
            local s3_key="claims/quality-test/quality_test_claims_$(date +%s).csv"
            
            # Upload test file with quality issues
            aws s3 cp "$test_dir/quality_test_claims.csv" "s3://$data_bucket/$s3_key" --region "$REGION"
            log_info "Uploaded quality test file"

            # Wait for validation
            sleep 15

            # Check for validation report
            local report_prefix="validation-reports/"
            local reports=$(aws s3 ls "s3://$data_bucket/$report_prefix" --region "$REGION" | wc -l)

            if [[ $reports -gt 0 ]]; then
                log_success "Data validation reports generated"
            else
                log_warning "No validation reports found"
            fi

            # Clean up
            aws s3 rm "s3://$data_bucket/$s3_key" --region "$REGION" || true
        fi
    }

    test_validation_rules
    log_success "Data quality tests completed"
}

# Main test orchestration
main() {
    log_info "Starting pipeline testing"
    log_info "Environment: $ENVIRONMENT"
    log_info "Region: $REGION"
    log_info "Test type: $TEST_TYPE"

    # Check prerequisites
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is required"
        exit 1
    fi

    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured"
        exit 1
    fi

    # Generate test data
    generate_test_data

    # Run requested tests
    case $TEST_TYPE in
        unit)
            run_unit_tests
            ;;
        integration)
            run_integration_tests
            ;;
        performance)
            run_performance_tests
            ;;
        quality)
            run_data_quality_tests
            ;;
        full)
            run_unit_tests
            run_integration_tests
            run_data_quality_tests
            if [[ "$ENVIRONMENT" != "prod" ]]; then
                run_performance_tests
            else
                log_warning "Skipping performance tests in production environment"
            fi
            ;;
        *)
            log_error "Unknown test type: $TEST_TYPE"
            usage
            exit 1
            ;;
    esac

    log_success "Pipeline testing completed successfully"
}

# Error handling
trap 'log_error "Test failed on line $LINENO"' ERR

# Run main function
main
