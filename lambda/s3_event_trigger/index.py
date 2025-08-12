import json
import boto3
import os
import logging
from datetime import datetime
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize AWS clients
sns = boto3.client('sns')
glue = boto3.client('glue')
s3 = boto3.client('s3')

# Environment variables
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
ERROR_TOPIC_ARN = os.environ['ERROR_TOPIC_ARN']
GLUE_JOB_NAME = os.environ['GLUE_JOB_NAME']
ENVIRONMENT = os.environ['ENVIRONMENT']

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function triggered by S3 events to initiate healthcare claims processing.
    
    Args:
        event: S3 event notification
        context: Lambda context object
        
    Returns:
        Dict containing processing status and metadata
    """
    try:
        logger.info(f"Processing S3 event: {json.dumps(event, default=str)}")
        
        # Extract S3 event records
        s3_records = event.get('Records', [])
        if not s3_records:
            raise ValueError("No S3 records found in event")
        
        processing_jobs = []
        
        for record in s3_records:
            try:
                # Extract S3 object information
                s3_info = record['s3']
                bucket_name = s3_info['bucket']['name']
                object_key = s3_info['object']['key']
                object_size = s3_info['object']['size']
                
                logger.info(f"Processing file: s3://{bucket_name}/{object_key} (Size: {object_size} bytes)")
                
                # Validate file for healthcare claims processing
                if not _is_valid_claims_file(object_key, object_size):
                    logger.warning(f"Skipping invalid file: {object_key}")
                    continue
                
                # Generate unique job run ID
                job_run_id = f"claims-etl-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{hash(object_key) % 10000:04d}"
                
                # Prepare Glue job arguments
                job_arguments = {
                    '--input_bucket': bucket_name,
                    '--input_key': object_key,
                    '--output_bucket': f"{bucket_name}-processed",
                    '--job_run_id': job_run_id,
                    '--environment': ENVIRONMENT,
                    '--file_size': str(object_size)
                }
                
                # Start Glue ETL job
                glue_response = glue.start_job_run(
                    JobName=GLUE_JOB_NAME,
                    Arguments=job_arguments
                )
                
                job_info = {
                    'job_run_id': glue_response['JobRunId'],
                    'custom_job_id': job_run_id,
                    'input_file': f"s3://{bucket_name}/{object_key}",
                    'file_size': object_size,
                    'status': 'STARTED',
                    'timestamp': datetime.now().isoformat()
                }
                
                processing_jobs.append(job_info)
                
                # Send SNS notification for job start
                _send_job_notification(job_info, 'STARTED')
                
                logger.info(f"Started Glue job: {glue_response['JobRunId']} for file: {object_key}")
                
            except Exception as file_error:
                logger.error(f"Error processing file {object_key}: {str(file_error)}")
                _send_error_notification(file_error, {
                    'file': object_key,
                    'bucket': bucket_name,
                    'context': 'file_processing'
                })
                continue
        
        # Return processing summary
        response = {
            'statusCode': 200,
            'body': {
                'message': f'Successfully processed {len(processing_jobs)} files',
                'jobs_started': processing_jobs,
                'environment': ENVIRONMENT,
                'timestamp': datetime.now().isoformat()
            }
        }
        
        logger.info(f"Processing complete: {len(processing_jobs)} jobs started")
        return response
        
    except Exception as error:
        logger.error(f"Lambda execution error: {str(error)}")
        _send_error_notification(error, {
            'context': 'lambda_execution',
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

def _is_valid_claims_file(object_key: str, object_size: int) -> bool:
    """
    Validate if the uploaded file is suitable for claims processing.
    
    Args:
        object_key: S3 object key
        object_size: File size in bytes
        
    Returns:
        Boolean indicating if file is valid
    """
    # Check file extension
    valid_extensions = ['.csv', '.json', '.parquet', '.txt']
    if not any(object_key.lower().endswith(ext) for ext in valid_extensions):
        return False
    
    # Check file size (minimum 1KB, maximum 10GB)
    if object_size < 1024 or object_size > 10 * 1024 * 1024 * 1024:
        return False
    
    # Check if it's in the claims processing path
    if not ('claims' in object_key.lower() or 'healthcare' in object_key.lower()):
        return False
    
    return True

def _send_job_notification(job_info: Dict[str, Any], status: str) -> None:
    """
    Send SNS notification for job status updates.
    
    Args:
        job_info: Job information dictionary
        status: Job status
    """
    try:
        message = {
            'event_type': 'job_status_update',
            'job_id': job_info['custom_job_id'],
            'glue_job_run_id': job_info['job_run_id'],
            'status': status,
            'input_file': job_info['input_file'],
            'file_size': job_info['file_size'],
            'timestamp': job_info['timestamp'],
            'environment': ENVIRONMENT
        }
        
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(message, default=str),
            Subject=f'Claims Processing Job {status}: {job_info["custom_job_id"]}',
            MessageAttributes={
                'event_type': {
                    'DataType': 'String',
                    'StringValue': 'job_status_update'
                },
                'environment': {
                    'DataType': 'String',
                    'StringValue': ENVIRONMENT
                }
            }
        )
        
        logger.info(f"Sent job notification: {job_info['custom_job_id']} - {status}")
        
    except Exception as error:
        logger.error(f"Failed to send job notification: {str(error)}")

def _send_error_notification(error: Exception, context: Dict[str, Any]) -> None:
    """
    Send error notification via SNS.
    
    Args:
        error: Exception object
        context: Error context information
    """
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
            Subject=f'Claims Pipeline Error - {context.get("context", "Unknown")}',
            MessageAttributes={
                'event_type': {
                    'DataType': 'String',
                    'StringValue': 'error'
                },
                'environment': {
                    'DataType': 'String',
                    'StringValue': ENVIRONMENT
                },
                'severity': {
                    'DataType': 'String',
                    'StringValue': 'high'
                }
            }
        )
        
        logger.info("Sent error notification")
        
    except Exception as notification_error:
        logger.error(f"Failed to send error notification: {str(notification_error)}")
