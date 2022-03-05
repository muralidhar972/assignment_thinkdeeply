
#function for saving the file in local directoy with path

def save_file_ext(upload_file_save_path, filename, file_extension, data):
    """
    :param: filename: object
    :param: upload_file_save_path : path
    :return: files get saved
    description: writing the file in directory.
    """
    with open(upload_file_save_path + filename + '.' +file_extension, 'wb') as f:
        f.write(data)

