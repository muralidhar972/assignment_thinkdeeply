import sys
sys.path.append("../..")
import uvicorn,json
from fastapi import File, UploadFile, FastAPI
from helper import file_preprocess
import logging

#reading json for user specific variables
with open("../config.json", 'r') as f:
    config = json.load(f)

log_file_save_path = config['log_file_save_path']
port = config["Port"]

#log file saving
#specfic logging information
logging.basicConfig(filename=log_file_save_path+'log_information.txt',level=logging.DEBUG,
                    format='%(asctime)s -%(levelname)s - %(name)s - %(message)s')

#variable declerations for logging
logging.debug('This is a debug message')
logging.info('This is a info message')
logging.warning('This is a warning message')
logging.error('this is a error message')
logging.critical('this is a critical message')

#Fast api decleration
app = FastAPI()

#post function for process the data.
@app.post("/upload")
async def upload(files: UploadFile = File(...)):
    '''
    :param files: uploaded data into directory
    :return: status on file save got saved in directory.
    '''
    response = {}
    try:
        file = files
        contents = await file.read()
        file_name = file.filename.split('.')[0]
        file_ext = file.filename.split('.')[1]
        response = file_preprocess(file_name,file_ext,contents)
    except Exception as e:
        response['Status_code'] = 515
        response['message'] = 'please reachout to admin'
    return response

#health check for app
@app.get("/health")
def health_status():
    '''
    :return: to know the API status, if API is running then you will have status code 200
    '''
    return {'status_code':200,'message':'Api is active'}

if __name__ == '__main__':
    uvicorn.run("main:app", host='0.0.0.0', port=port)
