import pyhive
import yaml
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pyhive import hive
import pandas as pd
import prestodb
from google.cloud import storage
from datetime import timedelta, datetime
import argparse
from typing import Dict, Any, Optional
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

CONFIG: Optional[Dict[str, Any]] = None
result_csv_file_path: Optional[str] = None
startDate: Optional[str] = None
endDate: Optional[str] = None
df_columns=['db_name', 'table_name', 'dt', 'result_aws_df', 'result_gcp_df', 'result_difference', 'comments']

def load_config(file_path):
    global CONFIG
    with open(file_path, 'r') as file:
        CONFIG = yaml.safe_load(file)


def update_result_csv(csv_file_path, updated_df):
    updated_df.to_csv(CONFIG['JobDetails']['localPath']+csv_file_path, mode='a', header=False, index=False)
    print(f"Added df to CSV file {csv_file_path}")

def send_email_with_attachment(result_df, dt, attachment_path):
    smtp_server = CONFIG['MailDetails']['smtpServer']
    smtp_port = CONFIG['MailDetails']['smtpPort']
    smtp_user = CONFIG['MailDetails']['smtpUser']
    smtp_password = CONFIG['MailDetails']['smtpPassword']
    from_email = CONFIG['MailDetails']['fromEmail']
    to = CONFIG['MailDetails']['toEmail']
    # Create a multipart message
    msg = MIMEMultipart()
    msg['From'] = CONFIG['MailDetails']['fromEmail']
    msg['To'] = CONFIG['MailDetails']['toEmail']
    msg['Subject'] = "AWS Hive vs GCP Hive DQ - Report : " + dt


    body = """
       <html>
       <head>
       <style>
         h2 {
             text-align: middle;
             font-family: Helvetica, Arial, sans-serif;
         }
         table {
             margin-left: auto;
             margin-right: auto;
             background-color:#f0f7f7;
         }
         table, th, td {
             border: 1px solid black;
             border-collapse: collapse;
             vertical-align: middle;
         }
         th, td {
             padding: 5px;
             align: middle;
             text-align: left;
             font-family: Helvetica, Arial, sans-serif;
             font-size: 90%;
         }
         table tbody tr:hover {
             background-color: #dddddd;
         }
         .wide {
             width: 100%;
         }
         th {
             background-color:#085e13;
             color:#ffffff;
         } 
       </style>
       </head>
       <body>
         """

    body += f"""
        <p>Please find the AWS hive to GCP hive report below:</p>
        {result_df.to_html(index=False, classes='wide', ).replace(".0","")}
      </body>
    </html>
        """


    # Attach the body with the msg instance
    msg.attach(MIMEText(body, 'html'))

    # Open the file to be sent
    with open(attachment_path, "rb") as attachment:
        # Instance of MIMEBase and named as part
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())

        # Encode into base64
        encoders.encode_base64(part)

        part.add_header('Content-Disposition', f'attachment; filename= {attachment_path}')

        # Attach the instance 'part' to instance 'msg'
        msg.attach(part)

    # Create SMTP session
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()  # Enable security
        server.login(smtp_user, smtp_password)  # Login with email and password
        server.sendmail(from_email, to, msg.as_string())  # Send email
    print(f"Mail send successfully to {to}")

def get_gcp_hive_connection(database, thread_name):
    try:
        return hive.Connection(
            host=CONFIG['GCPHiveDetails']['host'],
            port=CONFIG['GCPHiveDetails']['port'],
            database=database,
            auth=CONFIG['GCPHiveDetails']['auth']
        )
    except Exception as error:
        print(f"{thread_name}: {error}")

def get_aws_hive_connection(database, thread_name):
    try:
        conn=hive.Connection(
            host=CONFIG['AWSHiveDetails']['host'],
            port=CONFIG['AWSHiveDetails']['port'],
            username=CONFIG['AWSHiveDetails']['username'],
            password=CONFIG['AWSHiveDetails']['password'],
            database=database,
            auth=CONFIG['AWSHiveDetails']['auth']
        )
        return conn
    except Exception as error:
        raise RuntimeError(f"{thread_name}: {error}")

def execute_query(connection, query, schema, thread_name, conn_name, row):
    print(f"#"*50)
    print(f"{[thread_name]} Running query on {conn_name}: {query}")
    try:
        start_time = time.time()
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        end_time = time.time()
        total_time = end_time - start_time
        print(f"{[thread_name]} Total Time Taken: {total_time:.2f} seconds by {conn_name}")
        print(f"#"*50)
        return df
    except pyhive.exc.OperationalError as e:
        if "Table not found" in str(e):
            err_df = pd.DataFrame({
                'db_name': [row['db_name']],
                'table_name': [row['table_name']],
                'dt': None,
                'result_aws_df': None,
                'result_gcp_df': None,
                'result_difference': None,
                'comments': "The specified table was not found in the database"
            })
            update_result_csv(result_csv_file_path, err_df)
            raise RuntimeError(f"The specified table was not found in the database.")
        else:
            err_df = pd.DataFrame({
                'db_name': [row['db_name']],
                'table_name': [row['table_name']],
                'dt': None,
                'result_aws_df': None,
                'result_gcp_df': None,
                'result_difference': None,
                'comments': e
            })
            update_result_csv(result_csv_file_path, err_df)
    except prestodb.exceptions.PrestoQueryError as e:
        if "Internal Server Error" in str(e):
            err_df = pd.DataFrame({
                'db_name': [row['db_name']],
                'table_name': [row['table_name']],
                'dt': None,
                'result_aws_df': None,
                'result_gcp_df': None,
                'result_difference': None,
                'comments': "Internal Server Error"
            })
            update_result_csv(result_csv_file_path, err_df)
            raise RuntimeError(f"Internal Server Error")
        else:
            err_df = pd.DataFrame({
                'db_name': [row['db_name']],
                'table_name': [row['table_name']],
                'dt': None,
                'result_aws_df': None,
                'result_gcp_df': None,
                'result_difference': None,
                'comments': e
            })
            update_result_csv(result_csv_file_path, err_df)
    except Exception as e:
        err_df = pd.DataFrame({
            'db_name': [row['db_name']],
            'table_name': [row['table_name']],
            'dt': None,
            'result_aws_df': None,
            'result_gcp_df': None,
            'result_difference': None,
            'comments': e
        })
        update_result_csv(result_csv_file_path, err_df)
        print(f"{[thread_name]} Error: {e}")
        raise RuntimeError(f"{[thread_name]} {conn_name} Failed to execute query on schema {schema} Query: {query}") from e
    finally:
        cursor.close()
        connection.close()


def read_csv(csv_file_path):
    try:
        dtype={
            'db_name': 'string',
            'table_name': 'string',
            'partiton_col': 'string',
            'primary_key': 'string'
        }
        return pd.read_csv(csv_file_path, index_col=False, header=0, sep=",",dtype=dtype)
    except Exception as e:
        raise RuntimeError(f"Failed to read CSV file") from e

def get_dt(days_to_subtract):
    # Get the current date
    current_date = datetime.now()

    # Subtract days using timedelta
    new_date_obj = current_date - timedelta(days=days_to_subtract)

    # Format the new date as a string in "YYYY-MM-DD" format
    new_date_str = new_date_obj.strftime("%Y-%m-%d")
    return new_date_str

def date_to_epoch_millis(dt):
    date_obj = datetime.strptime(dt, "%Y-%m-%d")
    return int(date_obj.timestamp() * 1000)

def get_hive_query(row):
    if pd.isna(row['partiton_col']):
        return f"select count({row['primary_key']}) as result from {row['table_name']} where skull_opcode != 'D'"
    else:
        return f"""select count({row['primary_key']}) as result from {row['table_name']} where skull_opcode != 'D' and  {row['partiton_col']} BETWEEN '{startDate} 18:30:00' and '{endDate} 18:30:00' and dt BETWEEN '{startDate}' and '{endDate}'"""

def get_presto_query(row):
    if pd.isna(row['partiton_col']):
        return f"select count(row({row['primary_key']})) as result from {row['table_name']} where skull_opcode != 'D'"
    else:
        return f"""select count(row({row['primary_key']})) as result from {row['table_name']} 
        where skull_opcode != 'D' and  {row['partiton_col']} BETWEEN '{startDate} 18:30:00' and '{endDate} 18:30:00' 
        and dt BETWEEN date '{startDate}' and date '{endDate}'"""


def print_df(df, comm):
    # Display the DataFrames
    print(comm)
    print(df.head())


def main(row):
    try:
        thread_name = threading.current_thread().name
        print(
            f"{[thread_name]} Running DQ check for below Details \ndb_name: {row['db_name']} table_name: {row['table_name']}, partiton_col: {row['partiton_col']} primary_key: {row['primary_key']}\n")

        # Connection details for the first Hive metastore
        aws_hive_conn = get_aws_hive_connection(
            database=row['db_name'],
            thread_name=thread_name
        )

        gcp_hive_conn = get_gcp_hive_connection(
            database=row['db_name'],
            thread_name=thread_name
        )

        # Define your queries
        hive_query = get_hive_query(row)

        isTablePartition = "Non partition table" if pd.isna(row['partiton_col']) else "Partition table"

        # Execute queries and get results as DataFrames
        aws_df = execute_query(aws_hive_conn, hive_query, row['db_name'], thread_name, "AWS Hive Connection", row)
        gcp_df = execute_query(gcp_hive_conn, hive_query, row['db_name'], thread_name, "GCP Hive Connection", row)

        if not aws_df.empty and not gcp_df.empty:
            # Calculate the difference
            diff = aws_df['result'] - gcp_df['result']

            # Create the new DataFrame
            result_df = pd.DataFrame({
                'db_name': row['db_name'],
                'table_name': row['table_name'],
                'dt': isTablePartition,
                'result_aws_df': aws_df['result'],
                'result_gcp_df': gcp_df['result'],
                'result_difference': diff,
                'comments': None
            })

            merged_df = result_df.reindex(columns=['db_name', 'table_name', 'dt', 'result_aws_df', 'result_gcp_df', 'result_difference', 'comments'])

            merged_df['comments'] = merged_df.apply(lambda row: 'Count matching' if row['result_difference'] == 0 else "Count not matching", axis=1)

            update_result_csv(result_csv_file_path, merged_df)
        else:
            empty_df = pd.DataFrame({
                'db_name': [row['db_name']],
                'table_name': [row['table_name']],
                'dt': endDate,
                'result_aws_df': 0,
                'result_gcp_df': 0,
                'result_difference': 0,
                'comments': f"Both tables have 0 records for {startDate} to {endDate}"
            })
            update_result_csv(result_csv_file_path, empty_df)
    except KeyError as e:
        raise RuntimeError(f"{[thread_name]} KeyError: {e} - Check column names in the CSV")

def process_rows_multithreaded(df_csv):
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(main, row) for index, row in df_csv.iterrows()]
        for future in futures:
            try:
                future.result()
            except Exception as exc:
                print(f"Row processing generated an exception: {exc}")
                #raise Exception(f"Row processing generated an exception: {exc}")

    # Upload result csv to gcs
    upload_to_gcs(CONFIG['JobDetails']['bucketName'], local_path+result_csv_file_path, CONFIG['JobDetails']['gcsPath']+"dt="+startDate+"/"+result_csv_file_path)
    result_df = pd.read_csv(local_path+result_csv_file_path)
    send_email_with_attachment(result_df, endDate, result_csv_file_path)


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # Initialize a client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a blob object from the filepath
    blob = bucket.blob(destination_blob_name)

    # Upload the file to a destination
    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # Initialize a storage client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Get the blob
    blob = bucket.blob(source_blob_name)

    # Download the blob to a file
    blob.download_to_filename(destination_file_name)

    print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Hive 2 Hive DQ config Details')
    parser.add_argument('--config', type=str, required=True, help='Path to the YAML configuration file')

    args = parser.parse_args()
    config_file_path = args.config

    # Load Hive configuration
    load_config(config_file_path)

    local_path = CONFIG['JobDetails']['localPath']
    bucket_name = CONFIG['JobDetails']['bucketName']
    table_details_file_name = CONFIG['JobDetails']['tableDetailsFileName']
    presto_file_name = CONFIG['JobDetails']['prestoFileName']
    result_csv_file_path = str(int(datetime.now().timestamp()) * 1000)+".csv"

    table_details_blob_name = CONFIG['JobDetails']['gcsPath']+CONFIG['JobDetails']['tableDetailsFileName']
    destination_table_details_file_name = local_path+CONFIG['JobDetails']['tableDetailsFileName']
    presto_blob_name = CONFIG['JobDetails']['gcsPath']+CONFIG['JobDetails']['prestoFileName']
    destination_presto_file_name = local_path+CONFIG['JobDetails']['prestoFileName']
    endDate = get_dt(1)
    startDate = get_dt(2)
    if CONFIG['JobDetails']['runMode'].strip().lower() != 'local':
        download_blob(bucket_name, table_details_blob_name, destination_table_details_file_name)
        download_blob(bucket_name, presto_blob_name, destination_presto_file_name)

    destination_table_details_file_name = local_path+table_details_file_name
    destination_presto_file_name = local_path+presto_file_name
    df_csv = read_csv(destination_table_details_file_name)
    try:
        result_df = pd.DataFrame(columns=df_columns)
        result_df.to_csv(local_path+result_csv_file_path, mode='w', index=False)
        print(f"Successfully created CSV file {result_csv_file_path}")
        process_rows_multithreaded(df_csv)
    finally:
        print("Ran Hive DQ for all tables")

