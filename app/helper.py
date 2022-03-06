
import pyarrow.parquet as pq
import pyarrow as pa

#function for saving the file in local directoy with path

def save_upload_file(file_save_path, filename, file_extension, data):
    """
    :param: filename: object
    :param: upload_file_save_path : path
    :return: files get saved
    description: writing the file in directory.
    """
    with open(file_save_path + filename + '.' +file_extension, 'wb') as file:
        file.write(data)


def save_parquet_file(file_save_path,filename,data):
    """
    :param file_save_path: path where file tobe saved
    :param filename: filename
    :param table: Object
    :return:
    """
    table = pa.Table.from_pandas(data, preserve_index=False)
    pq.write_table(table,file_save_path+filename+'.parquet')
