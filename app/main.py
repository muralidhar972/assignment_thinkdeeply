import sys, json
import traceback
import pyarrow.parquet as pq
sys.path.append("../..")
import uvicorn,json
from fastapi import File, UploadFile, FastAPI
from typing import List
from helper import save_upload_file,save_parquet_file
import pandas as pd
import traceback
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
            save_upload_file(upload_file_save_path, file_name,file_ext, contents)
            if file_ext == 'csv':
                table = pd.read_csv(upload_file_save_path+file_name+'.'+file_ext)
                save_parquet_file(converted_file_save_path, file_name,table)
                table = pq.read_table(converted_file_save_path+file_name+'.parquet')
                output_table = table.to_pandas()
                response['status_code'] = 200
                response['message'] = 'Upload file got saved sucessfully '+file_name
                print(traceback.format_exc())
                response['data'] = output_table.head(100).to_json(orient='records')
            elif file_ext == 'xlsx':
                table = pd.read_excel(upload_file_save_path + file_name + '.' + file_ext)
                save_parquet_file(converted_file_save_path, file_name, table)
                table = pq.read_table(converted_file_save_path + file_name + '.parquet')
                output_table = table.to_pandas()
                response['status_code'] = 200
                response['message'] = 'Upload file got saved sucessfully ' + file_name
                print(traceback.format_exc())
                response['data'] = output_table.head(100).to_json(orient='records')
            else:
                table = pd.read_json(upload_file_save_path + file_name + '.' + file_ext)
                save_parquet_file(converted_file_save_path, file_name, table)
                table = pq.read_table(converted_file_save_path + file_name + '.parquet')
                output_table = table.to_pandas()
                response['status_code'] = 200
                response['message'] = 'Upload file got saved sucessfully ' + file_name
                print(traceback.format_exc())
                response['data'] = output_table.head(100).to_json(orient='records')
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
