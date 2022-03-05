import sys, json
sys.path.append("../..")
import uvicorn,json
from fastapi import File, UploadFile, FastAPI
from typing import List
from helper import save_file_ext



app = FastAPI()

#reading json for user specific variables
with open("config.json", 'r') as f:
    config = json.load(f)


#configuring the required variables.
file_extension = config['file_extension']
file_size_limit = config['upload_size']
upload_file_save_path = config['upload_file_save']
converted_file_save_path = config['converted_file_save']
port = config["Port"]


@app.post("/upload")
async def upload(files: UploadFile = File(...)):
    '''

    :param files: uploaded files into directory
    :return: status on file save got saved in directory.
    '''
    response = {}
    try:
        file = files
        contents = await file.read()
        file_name = file.filename.split('.')[0]
        file_ext = file.filename.split('.')[1]
        if file_ext in file_extension:
            save_file_ext(upload_file_save_path, file_name,file_ext, contents)
            save_file_ext(converted_file_save_path, file_name, 'parquet', contents)
            response['status_code'] = 200
            response['message'] = 'Upload file got saved sucessfully '+file_name
        else:
            response['status_code'] = 300
            response['message'] = 'Please upload the csv/excel/json files'

    except Exception as e:
        response['status_code'] = 301
        response['message'] = 'Please reach out to application admin with this error : ' +str(e)


    return response


@app.get("/health")
def health_status():
    '''
    :return: to know the API status, if API is running then you will have status code 200
    '''
    return {'status_code':200,'message':'Api is active'}

if __name__ == '__main__':
    uvicorn.run("main:app", host='0.0.0.0', port=port, debug=True)
