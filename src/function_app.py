import logging
import azure.functions as func
import azurefunctions.extensions.bindings.blob as blob

app = func.FunctionApp()

@app.blob_trigger(arg_name="input_blob", 
                  path="unprocessed-pdf/{name}",
                  connection="PDFProcessorSTORAGE",
                  source=func.BlobSource.EVENT_GRID)
@app.blob_input(arg_name="processed_container",
                path="processed-pdf",
                connection="PDFProcessorSTORAGE")
def process_blob_upload(input_blob: func.InputStream, processed_container: blob.ContainerClient) -> None:

    blob_name = input_blob.name.split('/')[-1]
    file_size = input_blob.length

    logging.info(f'Python Blob Trigger (using Event Grid) processed blob\n Name: {blob_name} \n Size: {file_size} bytes')

    try:
        # Copy to processed container using the bound ContainerClient
        processed_blob_name = f"processed_{blob_name}"
        processed_container.upload_blob(processed_blob_name, input_blob.read(), overwrite=True)

        logging.info(f'Successfully copied {processed_blob_name} to processed-pdf container using SDK type bindings')
        logging.info(f'PDF processing complete for {blob_name}')
    except Exception as error:
        logging.error(f'Error processing blob {blob_name}: {error}')
        raise error