import logging
import azure.functions as func
import azurefunctions.extensions.bindings.blob as blob

app = func.FunctionApp()

@app.blob_trigger(arg_name="source_blob_client", 
                  path="unprocessed-pdf/{name}",
                  connection="PDFProcessorSTORAGE",
                  source=func.BlobSource.EVENT_GRID)
@app.blob_input(arg_name="processed_container",
                path="processed-pdf",
                connection="PDFProcessorSTORAGE")
def process_blob_upload(source_blob_client: blob.BlobClient, processed_container: blob.ContainerClient) -> None:
    """
    Process blob upload event from Event Grid.
    
    This function triggers when a new blob is created in the unprocessed-pdf container.
    It copies the blob to the processed-pdf container with a "processed-" prefix.
    """
    
    blob_name = source_blob_client.get_blob_properties().name
    file_size = source_blob_client.get_blob_properties().size

    logging.info(f'Python Blob Trigger (using Event Grid) processed blob\n Name: {blob_name} \n Size: {file_size} bytes')

    # Copy the blob to the processed container with a new name
    processed_blob_name = f"processed-{blob_name}"
    
    # Check if the processed blob already exists
    if processed_container.get_blob_client(processed_blob_name).exists():
        logging.info(f'Blob {processed_blob_name} already exists in the processed container. Skipping upload.')
        return

    try:
        # Here you can add any processing logic for the input blob before uploading it to the processed container.
        
        # Upload the blob to the processed container using streams. You could add processing of the input stream logic here if needed.
        blob_data = source_blob_client.download_blob()
        processed_container.upload_blob(processed_blob_name, blob_data.readall(), overwrite=True)
        logging.info(f'PDF processing complete for {blob_name}. Blob copied to processed container with new name {processed_blob_name}.')
    except Exception as error:
        logging.error(f'Error processing blob {blob_name}: {error}')
        raise error