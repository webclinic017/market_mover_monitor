def clean_txt_file_content(file_dir: str):
    try:
        with open(file_dir, 'r+') as file:
            file.truncate(0)
    except Exception as e:
        raise e