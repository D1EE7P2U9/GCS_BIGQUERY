from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
from google.oauth2 import service_account
import uuid
import os
from dotenv import load_dotenv


load_dotenv()

service_account_key_path = os.getenv("service_account_key_path")

# Load service account credentials
credentials = service_account.Credentials.from_service_account_file(
    service_account_key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

gcs_bucket_name = os.getenv("gcs_bucket_name")
gcs_file_path = 'students.csv'

project_id = os.getenv("project_id")
table_id = 'student_table'

# Initialize the BigQuery client
try:
    client = bigquery.Client(project=project_id, credentials=credentials)
    print("client created")
except Exception as e:
    print(e)

#creating dataset_id
dataset_id = f'dataset_{str(uuid.uuid4())[:8]}'  

dataset_ref = client.dataset(dataset_id)

# Attempt to get the dataset, catching NotFound exception if it doesn't exist
try:
    dataset = client.get_dataset(dataset_ref)
    print(f"Dataset {dataset_id} already exists.")
except Exception as e:
    print(f"Error: {e}")
    print(f"Dataset {dataset_id} does not exist. Creating...")

    # Create the dataset
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = 'US'  # Set the desired location
    dataset = client.create_dataset(dataset)
    print(f"Dataset {dataset_id} created.")


# Define the table reference
table_ref = dataset_ref.table(table_id)

# Define the schema based on the CSV file (you may need to customize this)
schema = [
    bigquery.SchemaField("Roll_Number", "INTEGER"),
    bigquery.SchemaField("Name", "STRING"),
    # Add more fields as needed
]

# Define the job configuration
job_config = LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # Adjust if your CSV file has header rows
    autodetect=True,  # Auto-detect schema based on CSV file
)

# Load data from GCS into BigQuery
load_job = client.load_table_from_uri(
    f'gs://{gcs_bucket_name}/{gcs_file_path}',
    table_ref,
    job_config=job_config,
)

# Wait for the job to complete
load_job.result()

print(f"Loaded {load_job.output_rows} rows into {dataset_id}.{table_id}")
