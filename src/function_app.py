import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import os

app = func.FunctionApp()

# Initialize clients at module level for reuse across function executions
blob_service_client = None
credential = DefaultAzureCredential()

def get_blob_service_client() -> BlobServiceClient:
    """Get a reusable BlobServiceClient instance."""
    global blob_service_client
    
    if not blob_service_client:
        # For local development, use the connection string directly
        connection_string = os.environ.get('PDFProcessorSTORAGE')
        
        if not connection_string:
            logging.error('Storage connection string not found. Expected PDFProcessorSTORAGE__serviceUri environment variable.')
            connection_string = os.environ.get('PDFProcessorSTORAGE__serviceUri')
        
        # Check if running locally with Azurite
        if connection_string == 'UseDevelopmentStorage=true':
            # Use Azurite connection string
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        else:
            # Create BlobServiceClient using the storage account URL and managed identity credentials
            blob_service_client = BlobServiceClient(
                connection_string,
                credential
            )
    
    return blob_service_client

@app.blob_trigger(arg_name="blob", 
                  path="unprocessed-pdf/{name}",
                  connection="PDFProcessorSTORAGE",
                  source="EventGrid")
def process_blob_upload(blob: func.InputStream) -> None:
    """
    Azure Function triggered by Event Grid when a blob is uploaded to the unprocessed-pdf container.
    Processes PDF files and copies them to the processed-pdf container.
    """
    blob_name = blob.name.split('/')[-1]  # Extract filename from full path
    blob_data = blob.read()
    file_size = len(blob_data)
    
    logging.info(f'Python Blob Trigger (using Event Grid) processed blob\n Name: {blob_name} \n Size: {file_size} bytes')

    try:
        # Copy to processed container - simple demonstration of an async operation
        copy_to_processed_container(blob_data, f"processed_{blob_name}")
        
        logging.info(f'PDF processing complete for {blob_name}')
    except Exception as error:
        logging.error(f'Error processing blob {blob_name}: {error}')
        raise error

def copy_to_processed_container(blob_data: bytes, blob_name: str) -> None:
    """
    Simple method to demonstrate uploading the processed PDF to the processed-pdf container.
    """
    logging.info(f'Starting copy operation for {blob_name}')
    
    try:
        # Get the reusable BlobServiceClient
        blob_service_client = get_blob_service_client()
        
        # Get container client for processed PDFs
        container_client = blob_service_client.get_container_client('processed-pdf')
        
        # Upload the blob
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(blob_data, overwrite=True)
        
        logging.info(f'Successfully copied {blob_name} to processed-pdf container')
    except Exception as error:
        logging.error(f'Failed to copy {blob_name} to processed container: {error}')
        raise error