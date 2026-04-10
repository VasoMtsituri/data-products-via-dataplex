import os
import time

import functions_framework
from google.cloud import dataplex_v1


@functions_framework.http
def automate_bq_insights(request):
    """
    Creates and runs a Dataplex Data Documentation scan to generate
    column descriptions via Gemini.
    """

    project_id = os.environ.get("PROJECT_ID")
    dataset_id = os.environ.get("DATASET_ID")
    table_name = os.environ.get("TABLE_NAME")
    location = os.environ.get("LOCATION")

    print(f'Extracted vars: {[project_id, dataset_id, table_name, location]}')

    client = dataplex_v1.DataScanServiceClient()

    # Resource paths
    parent = f"projects/{project_id}/locations/{location}"
    table_resource = f"//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_name}"
    scan_id = f"gemini-doc-{table_name.replace('_', '-')}"  # IDs must be lowercase/hyphens
    scan_name = f"{parent}/dataScans/{scan_id}"

    # 1. Define the Scan Specification
    # We enable catalog_publishing_enabled to ensure Gemini's
    # insights are pushed to the table metadata.
    doc_spec = dataplex_v1.DataDocumentationSpec(
        catalog_publishing_enabled=True
    )

    data_scan_config = {
        "data": {
            "resource": table_resource
        },
        "data_documentation_spec": {
            "catalog_publishing_enabled": True
        },
        "display_name": f"Gemini Insights {table_name}",
        "execution_spec": {
            "trigger": {
                "on_demand": {}  # This replaces ExecutionSpec and Trigger classes
            }
        }
    }

    # 2. Create the Scan (or Get if exists)
    try:
        print(f"Creating scan: {scan_id}...")
        operation = client.create_data_scan(
            parent=parent,
            data_scan=data_scan_config,
            data_scan_id=scan_id
        )
        operation.result()
    except Exception as e:
        print(f"Scan already exists or error occurred: {e}")

    # 3. Run the Scan Job
    print(f"Starting Gemini generation job for {table_name}...")
    run_request = dataplex_v1.RunDataScanRequest(name=scan_name)
    job = client.run_data_scan(request=run_request)

    # 4. Wait for Completion
    # Data Documentation scans typically take 1-3 minutes as Gemini parses the data.
    print(f"Job started: {job}. Waiting for Gemini to finish...")

    return 'ok'
