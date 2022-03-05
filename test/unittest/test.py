import os
import sys, json
sys.path.append("../..")
from fastapi.testclient import TestClient
from app.main import app
import unittest


with open("./app/config.json", 'r') as f:
    config = json.load(f)

file_extension = config['file_extension']
file_size_limit = config['upload_size']
upload_file_save_path = config['upload_file_save']
converted_file_save_path = config['converted_file_save']




#this is sample example where we can write test cases. currently, wrote the functionality using the FastAPI .
class test(unittest.TestCase):

    client = TestClient(app)

    def test_upload(self):
        response = client.post('test_files/california_housing_train.csv')
        assert response.status_code == 200

    # def test_convert_upload_file_to_parquet_save(self):
    #     file_name = 'california_housing_train'
    #     read_file = '/test_files/california_housing_train.csv'
    #     output = save_file_ext(converted_file_save_path,file_name,'parquet',content)
    #     assert output['status_code'] == 200


if __name__=='__main__':
      unittest.main()




