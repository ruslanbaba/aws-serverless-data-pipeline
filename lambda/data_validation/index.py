import json
import boto3
import os
import logging
import pandas as pd
import re
from datetime import datetime
from typing import Dict, Any, List, Tuple
from io import StringIO

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize AWS clients
s3 = boto3.client('s3')
sns = boto3.client('sns')

# Environment variables
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
ERROR_TOPIC_ARN = os.environ['ERROR_TOPIC_ARN']
ENVIRONMENT = os.environ['ENVIRONMENT']

# Healthcare claims validation rules
REQUIRED_FIELDS = [
    'claim_id', 'patient_id', 'provider_id', 'service_date',
    'diagnosis_code', 'procedure_code', 'amount', 'status'
]

FIELD_PATTERNS = {
    'claim_id': r'^CLM[0-9]{8,12}$',
    'patient_id': r'^PAT[0-9]{6,10}$',
    'provider_id': r'^PRV[0-9]{6,8}$',
    'diagnosis_code': r'^[A-Z][0-9]{2}\.[0-9X]{1,3}$',  # ICD-10 format
    'procedure_code': r'^[0-9]{5}$',  # CPT code format
    'amount': r'^\d+\.\d{2}$'
}

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function for validating healthcare claims data.
    
    Args:
        event: SNS event or direct invocation
        context: Lambda context object
        
    Returns:
        Dict containing validation results
    """
    try:
        logger.info(f"Starting data validation: {json.dumps(event, default=str)}")
        
        # Parse event to get file information
        file_info = _parse_event(event)
        if not file_info:
            raise ValueError("Could not extract file information from event")
        
        bucket_name = file_info['bucket']
        object_key = file_info['key']
        
        logger.info(f"Validating file: s3://{bucket_name}/{object_key}")
        
        # Download and validate the file
        validation_results = _validate_claims_file(bucket_name, object_key)
        
        # Generate validation report
        report = _generate_validation_report(validation_results, bucket_name, object_key)
        
        # Store validation report
        report_key = f"validation-reports/{object_key.replace('/', '_')}_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        _store_validation_report(bucket_name, report_key, report)
        
        # Send notification based on validation results
        if validation_results['is_valid']:
            _send_validation_success_notification(report)
        else:
            _send_validation_failure_notification(report)
        
        response = {
            'statusCode': 200,
            'body': {
                'validation_status': 'success' if validation_results['is_valid'] else 'failed',
                'file': f"s3://{bucket_name}/{object_key}",
                'report_location': f"s3://{bucket_name}/{report_key}",
                'summary': validation_results['summary'],
                'timestamp': datetime.now().isoformat()
            }
        }
        
        logger.info(f"Validation complete: {validation_results['summary']}")
        return response
        
    except Exception as error:
        logger.error(f"Validation error: {str(error)}")
        _send_error_notification(error, {
            'context': 'data_validation',
            'function_name': context.function_name,
            'request_id': context.aws_request_id
        })
        
        return {
            'statusCode': 500,
            'body': {
                'error': str(error),
                'timestamp': datetime.now().isoformat()
            }
        }

def _parse_event(event: Dict[str, Any]) -> Dict[str, str]:
    """
    Parse event to extract file information.
    
    Args:
        event: Lambda event
        
    Returns:
        Dictionary with bucket and key information
    """
    # Handle SNS event
    if 'Records' in event and event['Records']:
        record = event['Records'][0]
        if 'Sns' in record:
            message = json.loads(record['Sns']['Message'])
            if 'input_file' in message:
                # Parse S3 URL
                s3_url = message['input_file']
                if s3_url.startswith('s3://'):
                    parts = s3_url[5:].split('/', 1)
                    return {'bucket': parts[0], 'key': parts[1]}
    
    # Handle direct invocation
    if 'bucket' in event and 'key' in event:
        return {'bucket': event['bucket'], 'key': event['key']}
    
    return None

def _validate_claims_file(bucket_name: str, object_key: str) -> Dict[str, Any]:
    """
    Validate healthcare claims data file.
    
    Args:
        bucket_name: S3 bucket name
        object_key: S3 object key
        
    Returns:
        Dictionary containing validation results
    """
    try:
        # Get file object
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read()
        
        # Determine file format and read data
        if object_key.lower().endswith('.csv'):
            data = pd.read_csv(StringIO(file_content.decode('utf-8')))
        elif object_key.lower().endswith('.json'):
            data = pd.read_json(StringIO(file_content.decode('utf-8')))
        elif object_key.lower().endswith('.parquet'):
            data = pd.read_parquet(StringIO(file_content))
        else:
            raise ValueError(f"Unsupported file format: {object_key}")
        
        # Perform validation checks
        validation_results = {
            'file_info': {
                'name': object_key,
                'size': len(file_content),
                'rows': len(data),
                'columns': len(data.columns)
            },
            'schema_validation': _validate_schema(data),
            'data_quality': _validate_data_quality(data),
            'business_rules': _validate_business_rules(data),
            'summary': {}
        }
        
        # Calculate overall validation status
        validation_results['is_valid'] = (
            validation_results['schema_validation']['is_valid'] and
            validation_results['data_quality']['is_valid'] and
            validation_results['business_rules']['is_valid']
        )
        
        # Generate summary
        validation_results['summary'] = {
            'total_records': len(data),
            'valid_records': validation_results['data_quality']['valid_records'],
            'error_records': validation_results['data_quality']['error_records'],
            'warnings': validation_results['data_quality']['warnings'],
            'overall_status': 'PASSED' if validation_results['is_valid'] else 'FAILED'
        }
        
        return validation_results
        
    except Exception as error:
        logger.error(f"File validation error: {str(error)}")
        raise

def _validate_schema(data: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate data schema against required fields.
    
    Args:
        data: Pandas DataFrame
        
    Returns:
        Schema validation results
    """
    missing_fields = [field for field in REQUIRED_FIELDS if field not in data.columns]
    extra_fields = [col for col in data.columns if col not in REQUIRED_FIELDS]
    
    return {
        'is_valid': len(missing_fields) == 0,
        'required_fields': REQUIRED_FIELDS,
        'actual_fields': list(data.columns),
        'missing_fields': missing_fields,
        'extra_fields': extra_fields,
        'field_count': len(data.columns)
    }

def _validate_data_quality(data: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate data quality rules.
    
    Args:
        data: Pandas DataFrame
        
    Returns:
        Data quality validation results
    """
    errors = []
    warnings = []
    valid_records = 0
    
    for index, row in data.iterrows():
        row_errors = []
        row_warnings = []
        
        # Check for null values in required fields
        for field in REQUIRED_FIELDS:
            if field in data.columns and pd.isna(row[field]):
                row_errors.append(f"Missing required field: {field}")
        
        # Validate field patterns
        for field, pattern in FIELD_PATTERNS.items():
            if field in data.columns and not pd.isna(row[field]):
                if not re.match(pattern, str(row[field])):
                    row_errors.append(f"Invalid format for {field}: {row[field]}")
        
        # Validate amount field
        if 'amount' in data.columns and not pd.isna(row['amount']):
            try:
                amount = float(row['amount'])
                if amount <= 0:
                    row_errors.append("Amount must be positive")
                elif amount > 100000:  # Warning for high amounts
                    row_warnings.append(f"High amount detected: ${amount:,.2f}")
            except (ValueError, TypeError):
                row_errors.append(f"Invalid amount format: {row['amount']}")
        
        # Validate date fields
        if 'service_date' in data.columns and not pd.isna(row['service_date']):
            try:
                service_date = pd.to_datetime(row['service_date'])
                if service_date > datetime.now():
                    row_errors.append("Service date cannot be in the future")
            except:
                row_errors.append(f"Invalid service date: {row['service_date']}")
        
        if not row_errors:
            valid_records += 1
        
        if row_errors:
            errors.append({'row': index + 1, 'errors': row_errors})
        
        if row_warnings:
            warnings.append({'row': index + 1, 'warnings': row_warnings})
    
    error_rate = (len(errors) / len(data)) * 100 if len(data) > 0 else 0
    
    return {
        'is_valid': error_rate <= 5,  # Allow up to 5% error rate
        'valid_records': valid_records,
        'error_records': len(errors),
        'error_rate': error_rate,
        'errors': errors[:100],  # Limit to first 100 errors
        'warnings': warnings[:100],  # Limit to first 100 warnings
        'total_records': len(data)
    }

def _validate_business_rules(data: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate business-specific rules for healthcare claims.
    
    Args:
        data: Pandas DataFrame
        
    Returns:
        Business rules validation results
    """
    violations = []
    
    # Check for duplicate claim IDs
    if 'claim_id' in data.columns:
        duplicates = data[data.duplicated(['claim_id'], keep=False)]
        if not duplicates.empty:
            violations.append(f"Found {len(duplicates)} duplicate claim IDs")
    
    # Check status field values
    if 'status' in data.columns:
        valid_statuses = ['submitted', 'processed', 'approved', 'denied', 'pending']
        invalid_statuses = data[~data['status'].isin(valid_statuses)]
        if not invalid_statuses.empty:
            violations.append(f"Found {len(invalid_statuses)} records with invalid status")
    
    # Check for reasonable amount ranges by procedure
    if 'procedure_code' in data.columns and 'amount' in data.columns:
        # This would typically involve more complex business logic
        high_amount_procedures = data[data['amount'] > 50000]
        if not high_amount_procedures.empty:
            violations.append(f"Found {len(high_amount_procedures)} procedures with amounts over $50,000")
    
    return {
        'is_valid': len(violations) == 0,
        'violations': violations,
        'rules_checked': ['duplicate_claims', 'valid_status', 'amount_ranges']
    }

def _generate_validation_report(validation_results: Dict[str, Any], bucket_name: str, object_key: str) -> Dict[str, Any]:
    """
    Generate comprehensive validation report.
    
    Args:
        validation_results: Validation results
        bucket_name: S3 bucket name
        object_key: S3 object key
        
    Returns:
        Formatted validation report
    """
    return {
        'report_metadata': {
            'report_id': f"VAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hash(object_key) % 10000:04d}",
            'generated_at': datetime.now().isoformat(),
            'environment': ENVIRONMENT,
            'file_path': f"s3://{bucket_name}/{object_key}"
        },
        'validation_results': validation_results,
        'recommendations': _generate_recommendations(validation_results)
    }

def _generate_recommendations(validation_results: Dict[str, Any]) -> List[str]:
    """
    Generate recommendations based on validation results.
    
    Args:
        validation_results: Validation results
        
    Returns:
        List of recommendations
    """
    recommendations = []
    
    if not validation_results['schema_validation']['is_valid']:
        recommendations.append("Fix schema issues before reprocessing")
    
    if validation_results['data_quality']['error_rate'] > 1:
        recommendations.append("Review data quality issues")
    
    if not validation_results['business_rules']['is_valid']:
        recommendations.append("Address business rule violations")
    
    if validation_results['data_quality']['warnings']:
        recommendations.append("Review warnings for potential data issues")
    
    return recommendations

def _store_validation_report(bucket_name: str, report_key: str, report: Dict[str, Any]) -> None:
    """
    Store validation report in S3.
    
    Args:
        bucket_name: S3 bucket name
        report_key: S3 key for report
        report: Validation report
    """
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=report_key,
            Body=json.dumps(report, indent=2, default=str),
            ContentType='application/json',
            ServerSideEncryption='AES256'
        )
        logger.info(f"Stored validation report: s3://{bucket_name}/{report_key}")
    except Exception as error:
        logger.error(f"Failed to store validation report: {str(error)}")

def _send_validation_success_notification(report: Dict[str, Any]) -> None:
    """Send notification for successful validation."""
    try:
        message = {
            'event_type': 'validation_success',
            'report_id': report['report_metadata']['report_id'],
            'file_path': report['report_metadata']['file_path'],
            'summary': report['validation_results']['summary'],
            'timestamp': report['report_metadata']['generated_at']
        }
        
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(message, default=str),
            Subject=f'Claims Data Validation Passed: {report["report_metadata"]["report_id"]}',
            MessageAttributes={
                'event_type': {'DataType': 'String', 'StringValue': 'validation_success'},
                'environment': {'DataType': 'String', 'StringValue': ENVIRONMENT}
            }
        )
    except Exception as error:
        logger.error(f"Failed to send success notification: {str(error)}")

def _send_validation_failure_notification(report: Dict[str, Any]) -> None:
    """Send notification for failed validation."""
    try:
        message = {
            'event_type': 'validation_failure',
            'report_id': report['report_metadata']['report_id'],
            'file_path': report['report_metadata']['file_path'],
            'summary': report['validation_results']['summary'],
            'recommendations': report['recommendations'],
            'timestamp': report['report_metadata']['generated_at']
        }
        
        sns.publish(
            TopicArn=ERROR_TOPIC_ARN,
            Message=json.dumps(message, default=str),
            Subject=f'Claims Data Validation Failed: {report["report_metadata"]["report_id"]}',
            MessageAttributes={
                'event_type': {'DataType': 'String', 'StringValue': 'validation_failure'},
                'environment': {'DataType': 'String', 'StringValue': ENVIRONMENT},
                'severity': {'DataType': 'String', 'StringValue': 'medium'}
            }
        )
    except Exception as error:
        logger.error(f"Failed to send failure notification: {str(error)}")

def _send_error_notification(error: Exception, context: Dict[str, Any]) -> None:
    """Send error notification via SNS."""
    try:
        error_message = {
            'event_type': 'error',
            'error_message': str(error),
            'error_type': type(error).__name__,
            'context': context,
            'timestamp': datetime.now().isoformat(),
            'environment': ENVIRONMENT
        }
        
        sns.publish(
            TopicArn=ERROR_TOPIC_ARN,
            Message=json.dumps(error_message, default=str),
            Subject=f'Data Validation Error - {context.get("context", "Unknown")}',
            MessageAttributes={
                'event_type': {'DataType': 'String', 'StringValue': 'error'},
                'environment': {'DataType': 'String', 'StringValue': ENVIRONMENT},
                'severity': {'DataType': 'String', 'StringValue': 'high'}
            }
        )
    except Exception as notification_error:
        logger.error(f"Failed to send error notification: {str(notification_error)}")
