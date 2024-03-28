from google.cloud import bigquery

# Set your Google Cloud project ID
project_id = "your-project-id"

# Set the path to your CSV file
csv_file_path = "path/to/your/file.csv"

# Set the destination dataset and table names in BigQuery
dataset_id = "your_dataset"
table_id = "your_table"

# Create a BigQuery client
client = bigquery.Client(project=project_id)

# Define the schema for your CSV data (optional)
schema = [
    bigquery.SchemaField("column_name1", bigquery.STRING),
    bigquery.SchemaField("column_name2", bigquery.FLOAT64),
    # Add more schema fields for each column in your CSV
]

# Load data from the CSV file
job_config = bigquery.LoadJobConfig(
    schema=schema,  # Optional: If you don't define the schema, BigQuery will try to auto-detect
    skip_leading_rows=1,  # Optional: Skip the header row if it exists
    field_delimiter=","  # Optional: Specify the field delimiter (default is comma)
)

with open(csv_file_path, "rb") as source_file:
    load_job = client.load_table_from_file(
        source_file,
        f"bq://{project_id}.{dataset_id}.{table_id}",
        job_config=job_config,
    )

# Wait for the job to complete
load_job.result()

print(f"Data loaded to table {project_id}.{dataset_id}.{table_id}")
