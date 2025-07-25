import logging
import azure.functions as func
from azure.storage.blob import ContainerClient

app = func.FunctionApp()

@app.blob_trigger(arg_name="blob", 
                  path="unprocessed-pdf/{name}",
                  connection="PDFProcessorSTORAGE",
                  source="EventGrid")
@app.blob_input(arg_name="processed_container",
                path="processed-pdf",
                connection="PDFProcessorSTORAGE")
def process_blob_upload(blob: func.InputStream, processed_container: ContainerClient) -> None:
    """
    Azure Function triggered by Event Grid when a blob is uploaded to the unprocessed-pdf container.
    Processes PDF files and copies them to the processed-pdf container using SDK type bindings.
    """
    blob_name = blob.name.split('/')[-1]  # Extract filename from full path
    blob_data = blob.read()
    file_size = len(blob_data)
    
    logging.info(f'Python Blob Trigger (using Event Grid) processed blob\n Name: {blob_name} \n Size: {file_size} bytes')

    try:
        # Copy to processed container using the bound ContainerClient
        processed_blob_name = f"processed_{blob_name}"
        blob_client = processed_container.get_blob_client(processed_blob_name)
        blob_client.upload_blob(blob_data, overwrite=True)
        
        logging.info(f'Successfully copied {processed_blob_name} to processed-pdf container using SDK type bindings')
        logging.info(f'PDF processing complete for {blob_name}')
    except Exception as error:
        logging.error(f'Error processing blob {blob_name}: {error}')
        raise error