from airflow.utils.email import send_email
from airflow.utils import timezone
import pendulum
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import json
import os
from sqlalchemy import text
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils import Iqama_table, get_conn_engine, map_row, change_date, send_json_to_api, create_json_payload, extract_code, extract_outcome, extract_note, update_table

CAIRO_TZ = pendulum.timezone('Africa/Cairo')


def failure_callback(context):
    """Enhanced failure callback with SMTP email sending"""
    try:
        dag_run = context.get("dag_run")
        task_instance = context.get("task_instance")
        exception = context.get("exception")
        execution_date = context.get('execution_date')
        
        # Format execution date properly
        exec_date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S') if execution_date else 'Unknown'
        
        subject = f"[Airflow FAILURE] DAG: {dag_run.dag_id} - Task: {task_instance.task_id}"
        log_url = f"http://10.24.105.221:8080/log?dag_id={dag_run.dag_id}&task_id={task_instance.task_id}&execution_date={execution_date.isoformat()}"
        
        # Create a more detailed HTML email body
        html_body = f"""
        <html>
        <body>
            <h2 style="color: #d32f2f;">Airflow DAG Failure Alert</h2>
            <table border="1" cellpadding="10" cellspacing="0" style="border-collapse: collapse;">
                <tr>
                    <td><strong>DAG ID:</strong></td>
                    <td>{dag_run.dag_id}</td>
                </tr>
                <tr>
                    <td><strong>Task ID:</strong></td>
                    <td>{task_instance.task_id}</td>
                </tr>
                <tr>
                    <td><strong>Execution Date:</strong></td>
                    <td>{exec_date_str}</td>
                </tr>
                <tr>
                    <td><strong>Run ID:</strong></td>
                    <td>{dag_run.run_id}</td>
                </tr>
                <tr>
                    <td><strong>State:</strong></td>
                    <td style="color: #d32f2f;">{task_instance.state}</td>
                </tr>
                <tr>
                    <td><strong>Exception:</strong></td>
                    <td><pre style="background-color: #f5f5f5; padding: 10px;">{str(exception) if exception else 'No exception details available'}</pre></td>
                </tr>
            </table>
            <br>
            <p><strong>Please investigate this failure and take appropriate action.</strong></p>
        </body>
        </html>
        """
        
        # List of recipients
        recipients = [
            'Mohamed.Reda@Andalusiagroup.net',
            'Omar.Wafy@Andalusiagroup.net',
            'Asmaa.Awad@Andalusiagroup.net',
            'Andrew.Alfy@Andalusiagroup.net',
            'Shehata.Amr@Andalusiagroup.net',
        ]

        # Send email using SMTP
        try:
            print("Sending failure notification via SMTP...")
            server = smtplib.SMTP('aws-ex-07.andalusia.loc', 25)
            server.set_debuglevel(1)
            server.starttls()
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = 'ai-service@andalusiagroup.net'
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = subject
            msg.attach(MIMEText(html_body, 'html'))
            
            # Send without authentication
            server.send_message(msg)
            server.quit()
            print("✅ Failure notification sent successfully!")
            
        except Exception as smtp_error:
            print(f"❌ Failed to send failure notification via SMTP: {smtp_error}")
            # Fall back to Airflow's send_email if SMTP fails
            try:
                send_email(
                    to=['Mohamed.Reda@Andalusiagroup.net',
                        'Omar.Wafy@Andalusiagroup.net',
                        'Asmaa.Awad@Andalusiagroup.net',
                        'Andrew.Alfy@Andalusiagroup.net',
                        'Shehata.Amr@Andalusiagroup.net',],
                    subject=subject,
                    html_content=html_body,
                )
                print("Used fallback email method successfully")
            except Exception as fallback_error:
                print(f"Failed to send email with fallback method: {fallback_error}")
        
    except Exception as e:
        print(f"Failed to process failure notification: {str(e)}")


def success_callback(context):
    """Optional success callback"""
    try:
        dag_run = context.get("dag_run")
        execution_date = context.get('execution_date')
        
        exec_date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S') if execution_date else 'Unknown'
        
        subject = f"[Airflow SUCCESS] DAG: {dag_run.dag_id} completed successfully"
        
        html_body = f"""
        <html>
        <body>
            <h2 style="color: #388e3c;">Airflow DAG Success</h2>
            <table border="1" cellpadding="10" cellspacing="0" style="border-collapse: collapse;">
                <tr>
                    <td><strong>DAG ID:</strong></td>
                    <td>{dag_run.dag_id}</td>
                </tr>
                <tr>
                    <td><strong>Execution Date:</strong></td>
                    <td>{exec_date_str}</td>
                </tr>
                <tr>
                    <td><strong>Run ID:</strong></td>
                    <td>{dag_run.run_id}</td>
                </tr>
                <tr>
                    <td><strong>Status:</strong></td>
                    <td style="color: #388e3c;">SUCCESS</td>
                </tr>
            </table>
            <br>
            <p><strong>All tasks completed successfully!</strong></p>
        </body>
        </html>
        """
        
        send_email(
            to=['Mohamed.Reda@Andalusiagroup.net',
                'Omar.Wafy@Andalusiagroup.net',
                'Asmaa.Awad@Andalusiagroup.net',
                'Andrew.Alfy@Andalusiagroup.net',
                'Shehata.Amr@Andalusiagroup.net',],
            subject=subject,
            html_content=html_body,
        )
        print("Success notification email sent")
        
    except Exception as e:
        print(f"Failed to send success notification email: {str(e)}")


# Enhanced default args with anonymous SMTP configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success': False,  # Set to True if you want success emails for individual tasks
    'email': ['Mohamed.Reda@Andalusiagroup.net',
              'Omar.Wafy@Andalusiagroup.net',
              'Asmaa.Awad@Andalusiagroup.net',
              'Andrew.Alfy@Andalusiagroup.net',
              'Shehata.Amr@Andalusiagroup.net',],  # Default email list
    'on_failure_callback': failure_callback,
    # 'on_success_callback': success_callback,  # Uncomment if you want success emails
}


@dag(
    dag_id="eligibility_job_midnight",
    default_args=default_args,
    start_date=pendulum.now(CAIRO_TZ).subtract(days=1),
    schedule_interval='58 23 * * *',  # 11:58 PM
    catchup=False,
    tags=["eligibility", "dotcare", "parallel"],
    max_active_runs=1,  # Prevent overlapping runs
    description="ETL pipeline for eligibility data from DOT-CARE with parallel processing",
    # DAG-level email configuration
    params={
        'email_on_dag_failure': True,
        'notification_emails': ['Mohamed.Reda@Andalusiagroup.net',
                                'Omar.Wafy@Andalusiagroup.net',
                                'Asmaa.Awad@Andalusiagroup.net',
                                'Andrew.Alfy@Andalusiagroup.net',
                                'Shehata.Amr@Andalusiagroup.net',]
    }
)
def eligibility_etl_pipeline():

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=['Mohamed.Reda@Andalusiagroup.net',
               'Omar.Wafy@Andalusiagroup.net',
               'Asmaa.Awad@Andalusiagroup.net',
               'Andrew.Alfy@Andalusiagroup.net',
               'Shehata.Amr@Andalusiagroup.net',]
    )
    def extract_data(**context):
        """Extract data with overlap handling to prevent data gaps"""
        try:
            source = "LIVE"
            query_path = Path(__file__).resolve().parent.parent / "sql" / "test.sql"
            query = query_path.read_text()
            current_time = datetime.now()
            engine = get_conn_engine(source=source)
            
            try:
                with engine.connect() as conn:
                    result = conn.execute(text(query))
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
            finally:
                engine.dispose()
            
            if df.empty:
                print("No new data to process.")
                return None
            
            print(f"Extracted {len(df)} records")
                    
            # For large datasets, save to file instead of XCom to enable parallel processing
            timestamp = current_time.strftime('%Y%m%d_%H%M%S')
            temp_file = f"/tmp/extracted_data_{timestamp}.parquet"
            df.to_parquet(temp_file)
            
            return {
                'file_path': temp_file,
                'record_count': len(df)
            }
            
        except Exception as e:
            print(f"Error in extract_data: {str(e)}")
            raise

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=['Shehata.Amr@Andalusiagroup.net']
    )
    def transform_iqama(extraction_info):
        """Transform data for Iqama table"""
        try:
            if not extraction_info:
                return None
                
            # Read from file instead of XCom for better performance
            df = pd.read_parquet(extraction_info['file_path'])
            
            print(f"Processing {len(df)} records for Iqama transformation")
            
            # Apply transformations
            df_iqama = df.apply(map_row, axis=1).drop_duplicates().reset_index(drop=True)
            df_iqama = df_iqama.drop_duplicates(keep='last').reset_index(drop=True)
            
            result_df = Iqama_table(df_iqama)
            result_df['Insertion_Date'] = datetime.now().strftime('%Y-%m-%d %H:%M')
            
            # Save transformed data to file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"/tmp/iqama_transformed_{timestamp}.parquet"
            result_df.to_parquet(output_file)
            
            return {
                'file_path': output_file,
                'record_count': len(result_df)
            }
            
        except Exception as e:
            print(f"Error in transform_iqama: {str(e)}")
            raise

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=['Shehata.Amr@Andalusiagroup.net']
    )
    def transform_eligibility(extraction_info):
        """Transform data for Eligibility table"""
        try:
            if not extraction_info:
                return None
                
            # Read from file instead of XCom for better performance
            df = pd.read_parquet(extraction_info['file_path'])
            df["insertion_date"] = datetime.now().strftime('%Y-%m-%d %H:%M')

            print(f"Processing {len(df)} records for Eligibility transformation")
            
            # Apply date transformations
            df["start_date"] = df["start_date"].astype(str).apply(change_date)
            df["end_date"] = df["end_date"].astype(str).apply(change_date)
            df["date_of_birth"] = df["date_of_birth"].astype(str).apply(change_date)

            # Process API requests with progress bar
            tqdm.pandas(desc="API Requests")
            df["elgability_response"] = df.progress_apply(
                lambda row: send_json_to_api(create_json_payload(row, source="AHJ_DOT-CARE")),
                axis=1
            )

            # Extract response data
            df["class"] = df["elgability_response"].apply(extract_code)
            df["outcome"] = df["elgability_response"].apply(extract_outcome)
            df["note"] = df["elgability_response"].apply(extract_note)
            

            # Apply business rules
            df.loc[(df["note"] == "1680 ") & (df["class"].isna()), "class"] = "out-network"
            df.loc[(df["note"] == "1658 ") & (df["class"].isna()), "class"] = "not-active"
            
            print(f"The number of the null values equal: {df['outcome'].isna().sum()}")

            final_df = df[["visit_id", "outcome", "note", "class", "insertion_date"]]
            
            # Save transformed data to file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"/tmp/eligibility_transformed_{timestamp}.parquet"
            final_df.to_parquet(output_file)
            
            return {
                'file_path': output_file,
                'record_count': len(final_df)
            }
            
        except Exception as e:
            print(f"Error in transform_eligibility: {str(e)}")
            raise

    @task(
        email_on_failure=True,
        email_on_retry=False,
        email=['Shehata.Amr@Andalusiagroup.net']
    )
    def load_data(iqama_info, eligibility_info):
        """Load transformed data to destinations"""
        try:
            if not iqama_info and not eligibility_info:
                print("No data to load")
                return
            
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
            
            # Create output directory
            os.makedirs("data/DOT-CARE", exist_ok=True)
            
            # Process Iqama data
            if iqama_info:
                iqama_df = pd.read_parquet(iqama_info['file_path'])
                iqama_df.to_csv(f"data/DOT-CARE/iqama_{timestamp}.csv", index=False)
                update_table(table_name="Iqama_dotcare", df=iqama_df)
                print(f"Loaded {len(iqama_df)} Iqama records")
            
            # Process Eligibility data
            if eligibility_info:
                eligibility_df = pd.read_parquet(eligibility_info['file_path'])
                eligibility_df.to_csv(f"data/DOT-CARE/eligibilty_{timestamp}.csv", index=False)
                update_table(table_name="Eligibility_dotcare", df=eligibility_df)
                print(f"Loaded {len(eligibility_df)} Eligibility records")
                
        except Exception as e:
            print(f"Error in load_data: {str(e)}")
            raise

    @task
    def cleanup_extraction_file(extraction_info, eligibility_info, iqama_info):
        """Clean up the extracted data file"""
        try:
            if extraction_info and os.path.exists(extraction_info['file_path']):
                os.remove(extraction_info['file_path'])
                print(f"Cleaned up extraction file: {extraction_info['file_path']}")
                
            if eligibility_info and os.path.exists(eligibility_info['file_path']):
                os.remove(eligibility_info['file_path'])
                print(f"Cleaned up extraction file: {eligibility_info['file_path']}")
                
            if iqama_info and os.path.exists(iqama_info['file_path']):
                os.remove(iqama_info['file_path'])
                print(f"Cleaned up extraction file: {iqama_info['file_path']}")
        except Exception as e:
            print(f"Error cleaning up extraction file: {str(e)}")
            # Don't raise here as cleanup failure shouldn't fail the DAG
            

    # DAG flow with parallel transforms
    extracted = extract_data()
    
    # These two transforms will run in parallel since they don't depend on each other
    iqama_transformed = transform_iqama(extracted)
    eligibility_transformed = transform_eligibility(extracted)
    
    # Load data after both transforms complete
    loaded = load_data(iqama_transformed, eligibility_transformed)
    
    # Clean up extraction file after loading is complete
    loaded >> cleanup_extraction_file(extracted, iqama_transformed, eligibility_transformed)


dag = eligibility_etl_pipeline()