"""
Healthcare Claims ETL Job for AWS Glue
Processes 500M+ healthcare claims monthly with optimized performance
"""

import sys
import boto3
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Initialize AWS clients
sns = boto3.client('sns')
s3 = boto3.client('s3')

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'input_bucket',
    'input_key', 
    'output_bucket',
    'job_run_id',
    'environment',
    'file_size'
])

# Job configuration
JOB_NAME = args['JOB_NAME']
INPUT_BUCKET = args['input_bucket']
INPUT_KEY = args['input_key']
OUTPUT_BUCKET = args['output_bucket']
JOB_RUN_ID = args['job_run_id']
ENVIRONMENT = args['environment']
FILE_SIZE = int(args['file_size'])

# Spark optimization settings for large-scale processing
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Dynamic partitioning based on file size
if FILE_SIZE > 1_000_000_000:  # 1GB+
    NUM_PARTITIONS = 200
elif FILE_SIZE > 100_000_000:  # 100MB+
    NUM_PARTITIONS = 50
else:
    NUM_PARTITIONS = 10

print(f"Starting ETL job: {JOB_RUN_ID}")
print(f"Input: s3://{INPUT_BUCKET}/{INPUT_KEY}")
print(f"Output: s3://{OUTPUT_BUCKET}/")
print(f"File size: {FILE_SIZE:,} bytes")
print(f"Target partitions: {NUM_PARTITIONS}")

def main():
    """Main ETL processing function."""
    try:
        job.init(JOB_NAME, args)
        
        # Start processing timer
        start_time = datetime.now()
        
        # Read input data
        print("Reading input data...")
        input_df = read_input_data()
        
        print(f"Input records: {input_df.count():,}")
        
        # Data cleaning and standardization
        print("Cleaning and standardizing data...")
        cleaned_df = clean_and_standardize(input_df)
        
        # Data enrichment
        print("Enriching data...")
        enriched_df = enrich_data(cleaned_df)
        
        # Data transformation
        print("Transforming data...")
        transformed_df = transform_data(enriched_df)
        
        # Data quality checks
        print("Performing quality checks...")
        quality_results = perform_quality_checks(transformed_df)
        
        # Partition and write output
        print("Writing output data...")
        write_output_data(transformed_df)
        
        # Generate processing metrics
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        metrics = {
            'job_run_id': JOB_RUN_ID,
            'input_file': f"s3://{INPUT_BUCKET}/{INPUT_KEY}",
            'output_location': f"s3://{OUTPUT_BUCKET}/processed/",
            'processing_time_seconds': processing_time,
            'input_records': input_df.count(),
            'output_records': transformed_df.count(),
            'quality_results': quality_results,
            'environment': ENVIRONMENT,
            'completed_at': end_time.isoformat()
        }
        
        # Send completion notification
        send_completion_notification(metrics)
        
        print(f"ETL job completed successfully in {processing_time:.2f} seconds")
        print(f"Processed {metrics['input_records']:,} records")
        
        job.commit()
        
    except Exception as error:
        print(f"ETL job failed: {str(error)}")
        send_error_notification(error)
        raise

def read_input_data():
    """Read and parse input data from S3."""
    input_path = f"s3://{INPUT_BUCKET}/{INPUT_KEY}"
    
    # Determine file format and read accordingly
    if INPUT_KEY.lower().endswith('.parquet'):
        df = spark.read.parquet(input_path)
    elif INPUT_KEY.lower().endswith('.json'):
        df = spark.read.option("multiline", "true").json(input_path)
    elif INPUT_KEY.lower().endswith('.csv'):
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    else:
        # Try to infer format
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    
    # Repartition for optimal processing
    return df.repartition(NUM_PARTITIONS)

def clean_and_standardize(df):
    """Clean and standardize healthcare claims data."""
    
    # Define schema for consistent data types
    schema_mapping = {
        'claim_id': StringType(),
        'patient_id': StringType(),
        'provider_id': StringType(),
        'service_date': DateType(),
        'diagnosis_code': StringType(),
        'procedure_code': StringType(),
        'amount': DecimalType(10, 2),
        'status': StringType()
    }
    
    # Apply schema corrections
    for column, data_type in schema_mapping.items():
        if column in df.columns:
            df = df.withColumn(column, col(column).cast(data_type))
    
    # Remove records with null required fields
    required_fields = ['claim_id', 'patient_id', 'provider_id', 'service_date']
    for field in required_fields:
        if field in df.columns:
            df = df.filter(col(field).isNotNull())
    
    # Standardize string fields
    string_fields = ['claim_id', 'patient_id', 'provider_id', 'diagnosis_code', 'procedure_code', 'status']
    for field in string_fields:
        if field in df.columns:
            df = df.withColumn(field, upper(trim(col(field))))
    
    # Validate and clean amounts
    if 'amount' in df.columns:
        df = df.filter(col('amount') > 0)  # Remove negative or zero amounts
        df = df.filter(col('amount') < 1000000)  # Remove unrealistic amounts
    
    # Add processing metadata
    df = df.withColumn('processed_date', current_timestamp())
    df = df.withColumn('job_run_id', lit(JOB_RUN_ID))
    df = df.withColumn('source_file', lit(f"s3://{INPUT_BUCKET}/{INPUT_KEY}"))
    
    return df

def enrich_data(df):
    """Enrich claims data with additional calculated fields."""
    
    # Add derived date fields
    if 'service_date' in df.columns:
        df = df.withColumn('service_year', year(col('service_date')))
        df = df.withColumn('service_month', month(col('service_date')))
        df = df.withColumn('service_quarter', quarter(col('service_date')))
        df = df.withColumn('days_since_service', 
                          datediff(current_date(), col('service_date')))
    
    # Add claim categorization
    if 'diagnosis_code' in df.columns:
        df = df.withColumn('diagnosis_category',
                          when(col('diagnosis_code').startswith('A'), 'Infectious')
                          .when(col('diagnosis_code').startswith('C'), 'Oncology')
                          .when(col('diagnosis_code').startswith('E'), 'Endocrine')
                          .when(col('diagnosis_code').startswith('I'), 'Circulatory')
                          .when(col('diagnosis_code').startswith('J'), 'Respiratory')
                          .when(col('diagnosis_code').startswith('K'), 'Digestive')
                          .when(col('diagnosis_code').startswith('M'), 'Musculoskeletal')
                          .when(col('diagnosis_code').startswith('N'), 'Genitourinary')
                          .when(col('diagnosis_code').startswith('S'), 'Injury')
                          .otherwise('Other'))
    
    # Add amount categorization
    if 'amount' in df.columns:
        df = df.withColumn('amount_category',
                          when(col('amount') < 100, 'Low')
                          .when(col('amount') < 1000, 'Medium')
                          .when(col('amount') < 10000, 'High')
                          .otherwise('Very High'))
    
    # Add provider statistics (using window functions for performance)
    if 'provider_id' in df.columns and 'amount' in df.columns:
        provider_window = Window.partitionBy('provider_id')
        df = df.withColumn('provider_total_amount', 
                          sum('amount').over(provider_window))
        df = df.withColumn('provider_claim_count', 
                          count('claim_id').over(provider_window))
        df = df.withColumn('provider_avg_amount', 
                          avg('amount').over(provider_window))
    
    return df

def transform_data(df):
    """Apply business transformations to the data."""
    
    # Create summary aggregations
    daily_summary = df.groupBy('service_date', 'diagnosis_category') \
                     .agg(count('claim_id').alias('claim_count'),
                          sum('amount').alias('total_amount'),
                          avg('amount').alias('avg_amount'),
                          countDistinct('patient_id').alias('unique_patients'),
                          countDistinct('provider_id').alias('unique_providers'))
    
    # Add the daily summary as a separate dataset
    df = df.withColumn('is_summary', lit(False))
    daily_summary = daily_summary.withColumn('is_summary', lit(True))
    
    # Create patient risk scoring
    if all(col in df.columns for col in ['patient_id', 'amount', 'diagnosis_category']):
        patient_window = Window.partitionBy('patient_id')
        df = df.withColumn('patient_total_claims', 
                          count('claim_id').over(patient_window))
        df = df.withColumn('patient_total_amount', 
                          sum('amount').over(patient_window))
        
        # Risk score based on claim frequency and amount
        df = df.withColumn('risk_score',
                          when((col('patient_total_claims') > 10) & 
                               (col('patient_total_amount') > 50000), 'High')
                          .when((col('patient_total_claims') > 5) | 
                                (col('patient_total_amount') > 20000), 'Medium')
                          .otherwise('Low'))
    
    # Add data lineage
    df = df.withColumn('etl_version', lit('1.0'))
    df = df.withColumn('data_source', lit('healthcare_claims'))
    
    return df

def perform_quality_checks(df):
    """Perform data quality checks and return results."""
    
    total_records = df.count()
    
    # Check for duplicates
    duplicate_claims = df.groupBy('claim_id').count().filter(col('count') > 1).count()
    
    # Check data completeness
    completeness_checks = {}
    required_fields = ['claim_id', 'patient_id', 'provider_id', 'service_date']
    
    for field in required_fields:
        if field in df.columns:
            null_count = df.filter(col(field).isNull()).count()
            completeness_checks[field] = {
                'null_count': null_count,
                'completeness_rate': (total_records - null_count) / total_records * 100
            }
    
    # Check amount validity
    amount_checks = {}
    if 'amount' in df.columns:
        negative_amounts = df.filter(col('amount') <= 0).count()
        high_amounts = df.filter(col('amount') > 100000).count()
        amount_checks = {
            'negative_amounts': negative_amounts,
            'high_amounts': high_amounts,
            'avg_amount': df.agg(avg('amount')).collect()[0][0]
        }
    
    # Check date validity
    date_checks = {}
    if 'service_date' in df.columns:
        future_dates = df.filter(col('service_date') > current_date()).count()
        old_dates = df.filter(col('service_date') < date_sub(current_date(), 365 * 7)).count()
        date_checks = {
            'future_dates': future_dates,
            'very_old_dates': old_dates
        }
    
    quality_results = {
        'total_records': total_records,
        'duplicate_claims': duplicate_claims,
        'completeness_checks': completeness_checks,
        'amount_checks': amount_checks,
        'date_checks': date_checks,
        'overall_quality_score': calculate_quality_score(
            total_records, duplicate_claims, completeness_checks, amount_checks, date_checks
        )
    }
    
    return quality_results

def calculate_quality_score(total_records, duplicates, completeness, amounts, dates):
    """Calculate overall data quality score."""
    if total_records == 0:
        return 0
    
    # Start with 100 points
    score = 100
    
    # Deduct for duplicates
    if duplicates > 0:
        score -= min(20, (duplicates / total_records) * 100)
    
    # Deduct for incompleteness
    for field, stats in completeness.items():
        if stats['completeness_rate'] < 95:
            score -= (100 - stats['completeness_rate']) * 0.5
    
    # Deduct for amount issues
    if amounts:
        if amounts.get('negative_amounts', 0) > 0:
            score -= min(10, (amounts['negative_amounts'] / total_records) * 100)
    
    # Deduct for date issues
    if dates:
        if dates.get('future_dates', 0) > 0:
            score -= min(10, (dates['future_dates'] / total_records) * 100)
    
    return max(0, score)

def write_output_data(df):
    """Write processed data to S3 in optimized format."""
    
    output_base_path = f"s3://{OUTPUT_BUCKET}/processed"
    
    # Partition by year and month for efficient querying
    if 'service_year' in df.columns and 'service_month' in df.columns:
        df.write \
          .mode('overwrite') \
          .partitionBy('service_year', 'service_month') \
          .option("compression", "snappy") \
          .parquet(f"{output_base_path}/claims/")
    else:
        df.write \
          .mode('overwrite') \
          .option("compression", "snappy") \
          .parquet(f"{output_base_path}/claims/")
    
    # Create separate aggregated datasets for analytics
    if 'diagnosis_category' in df.columns:
        # Monthly aggregations
        monthly_agg = df.groupBy('service_year', 'service_month', 'diagnosis_category') \
                       .agg(count('claim_id').alias('claim_count'),
                            sum('amount').alias('total_amount'),
                            avg('amount').alias('avg_amount'),
                            countDistinct('patient_id').alias('unique_patients'))
        
        monthly_agg.write \
                   .mode('overwrite') \
                   .partitionBy('service_year') \
                   .option("compression", "snappy") \
                   .parquet(f"{output_base_path}/monthly_aggregations/")
        
        # Provider aggregations
        provider_agg = df.groupBy('provider_id', 'service_year', 'service_month') \
                        .agg(count('claim_id').alias('claim_count'),
                             sum('amount').alias('total_amount'),
                             countDistinct('patient_id').alias('unique_patients'),
                             countDistinct('diagnosis_category').alias('diagnosis_variety'))
        
        provider_agg.write \
                    .mode('overwrite') \
                    .partitionBy('service_year') \
                    .option("compression", "snappy") \
                    .parquet(f"{output_base_path}/provider_aggregations/")

def send_completion_notification(metrics):
    """Send job completion notification."""
    try:
        message = {
            'event_type': 'etl_job_completed',
            'job_run_id': JOB_RUN_ID,
            'metrics': metrics,
            'environment': ENVIRONMENT
        }
        
        # This would typically use SNS topic from environment variables
        print(f"Job completion metrics: {json.dumps(metrics, indent=2, default=str)}")
        
    except Exception as error:
        print(f"Failed to send completion notification: {str(error)}")

def send_error_notification(error):
    """Send job error notification."""
    try:
        error_message = {
            'event_type': 'etl_job_failed',
            'job_run_id': JOB_RUN_ID,
            'error_message': str(error),
            'error_type': type(error).__name__,
            'input_file': f"s3://{INPUT_BUCKET}/{INPUT_KEY}",
            'environment': ENVIRONMENT,
            'timestamp': datetime.now().isoformat()
        }
        
        print(f"Job error: {json.dumps(error_message, indent=2, default=str)}")
        
    except Exception as notification_error:
        print(f"Failed to send error notification: {str(notification_error)}")

if __name__ == "__main__":
    main()
