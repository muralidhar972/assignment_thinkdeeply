import traceback

import pyarrow.parquet as pq
import pyarrow as pa
import os, json, pandas as pd

# reading json for user specific variables
with open("../config.json", 'r') as f:
    config = json.load(f)

# configuring the required variables.
file_extension = config['file_extension']
file_size_limit = config['upload_size']
upload_file_save_path = config['upload_file_save']
converted_file_save_path = config['converted_file_save']


# function for saving the file in local directoy with path

def save_upload_file(file_save_path, filename, file_extension, data):
    """
    :param: filename: object
    :param: upload_file_save_path : path
    :return: data get saved
    description: writing the file in directory.
    """
    with open(file_save_path + filename + '.' + file_extension, 'wb') as file:
        file.write(data)


def save_parquet_file(file_save_path, filename, data):
    """
    :param file_save_path: path where file tobe saved
    :param filename: filename
    :param table: Object
    :return:
    """
    response = {}
    try:
        table = pa.Table.from_pandas(data, preserve_index=False)
        pq.write_table(table, file_save_path + filename + '.parquet')
        response['status'] = 200
    except Exception as e:
        response['status'] = 500

    return response


def parquet_to_pandas(converted_file_save_path, file_name):
    '''
    :param converted_file_save_path: path where file to read
    :param file_name: file name
    :return: dataframe object
    '''
    table = pq.read_table(converted_file_save_path + file_name + '.parquet')
    output_dataframe = table.to_pandas()
    return output_dataframe


def file_existent(upload_file_save_path, file_name, file_ext):
    '''

    :param upload_file_save_path:
    :param file_name:
    :param file_ext:
    :return: if filename exists in location it will add number at end of the file in incremental manner
    '''
    try:
        path = upload_file_save_path + file_name + '.' + file_ext
        file_new = path
        root, ext = os.path.splitext(path)
        i = 0
        while os.path.exists(file_new):
            i += 1
            file_new = '%s_%i%s' % (root, i, ext)

        file_name_ext = file_new.split('/')[-1]
        file_name = file_name_ext.split('.')[0]
        return file_name
    except Exception as e:
        return str(e)


def file_preprocess(file_name, file_ext, contents):
    '''
    :param file_name:
    :param file_ext:
    :param contents:
    :return: process the data and converts that to parquest data and shares response.
    '''
    response_output = {}

    if os.path.exists(upload_file_save_path + file_name + '.' + file_ext):
        file_name = file_existent(upload_file_save_path, file_name, file_ext)

    try:
        if file_ext in file_extension:
            save_upload_file(upload_file_save_path, file_name, file_ext, contents)
            if file_ext == 'csv':
                table = pd.read_csv(upload_file_save_path + file_name + '.' + file_ext)
                save_parquet_file(converted_file_save_path, file_name, table)
                output_table = parquet_to_pandas(converted_file_save_path, file_name)
                response_output['status_code'] = 200
                response_output['message'] = 'Upload file got saved sucessfully ' + file_name
                response_output['data'] = output_table.head(100).to_json(orient='records')
            elif file_ext == 'xlsx':
                table = pd.read_excel(upload_file_save_path + file_name + '.' + file_ext)
                save_parquet_file(converted_file_save_path, file_name, table)
                output_table = parquet_to_pandas(converted_file_save_path, file_name)
                response_output['status_code'] = 200
                response_output['message'] = 'Upload file got saved sucessfully ' + file_name
                response_output['data'] = output_table.head(100).to_json(orient='records')
            else:
                table = pd.read_json(upload_file_save_path + file_name + '.' + file_ext)
                save_parquet_file(converted_file_save_path, file_name, table)
                output_table = parquet_to_pandas(converted_file_save_path, file_name)
                response_output['status_code'] = 200
                response_output['message'] = 'Upload file got saved sucessfully ' + file_name
                response_output['data'] = output_table.head(100).to_json(orient='records')
        else:
            response_output['status_code'] = 512
            response_output['message'] = 'Please upload the csv/excel/json data'
    except Exception as e:
        response_output['status_code'] = 513
        response_output['message'] = 'Please reach out to application admin with this error : ' + str(e)

    return response_output
