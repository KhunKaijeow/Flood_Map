import os.path

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from tqdm import tqdm
import pandas as pd
from datetime import datetime
import re
from io import BytesIO

def fetch_files(parent_folder_id, service=None, dt=None):
    """
    Get all the files from the parent folder
    :param parent_folder_id: The ID of the parent folder
    :param service: The service object
    :param dt: DataFrame to store the results
    :return: DataFrame with columns ["Folder Name", "File Name", "Link"]
    """

    # Create an empty DataFrame if dt is not provided
    if dt is None:
        dt = pd.DataFrame(columns=["Folder Name", "File Name", "Link"])

    # Check if service is defined
    if service is None:
        print("Service is not defined")
        return None
    try:
        query = f"'{parent_folder_id}' in parents"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        folders = results.get("files", [])

        # Check if the folder is empty
        if not folders:
            print("Folder is empty")
            return None
        else:

            #  Loop through all the folders
            for folder in tqdm(folders, desc=f"Fetching files from {parent_folder_id}", total=len(folders)):
                folder_name, folder_id = folder["name"], folder["id"]
                query = f"'{folder_id}' in parents"
                results = service.files().list(q=query, fields="files(id, name)").execute()
                files = results.get("files", [])

                # Check if the folder is empty
                if not files:
                    print(f"{folder_name} is empty")
                    return None
                else:

                    # Loop through all the files
                    for file in tqdm(files, desc=f"Fetching files from {folder_name}", total=len(files)):
                        file_name, file_id = file["name"], file["id"]
                        link = f"https://drive.google.com/file/d/{file_id}/view"
                        # Append the results to the DataFrame
                        dt = pd.concat([dt, pd.DataFrame([[folder_name, file_name, file_id, link]], columns=["folder_name", "file_name", "file_id", "link"])])
        return dt
    except Exception as error:
        print(f"An error occurred: {error}")
        return None

def upload_file(file_name, service=None, folder_id=None):
    """
    Upload a file to Google Drive
    :param file_name: File name of the file to be uploaded
    :param service: Service object
    :param folder_id: Folder ID where the file will be uploaded
    :return:
    """
    if service is None:
        print("Service is not defined")
        return None

    if folder_id is None:
        print("Folder ID is not defined")
        return None

    try:
        file_metadata = {
            "name": file_name,
            "parents": [folder_id]
        }
        media = MediaFileUpload(file_name, resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        print(f"{file_name} uploaded successfully")
    except Exception as error:
        print(f"An error occurred: {error}")
        return None

def get_address(text):
    """
    Get the address from the text
    :param text: Text to extract the address from
    :return:
    """
    try:
        address = text.split("lat")[0]
        return address
    except Exception as error:
        print(f"An error occurred: {error}")
        return None

def get_lat_long(text):
    """
    Get the latitude and longitude from the text
    :param text: Text to extract the latitude and longitude from
    :return:
    """
    try:
        pattern = r"(?:latitude longitude|lat)\s*(?::)?\s*(\d+\.\d+),?\s*(?:long|longitude)?\s*(\d+\.\d+)"
        lat_long = re.search(pattern, text, re.IGNORECASE)
        latitudes, longitudes = lat_long.group(1), lat_long.group(2)
        return latitudes, longitudes
    except Exception as error:
        print(f"An error occurred: {error}")
        return None

def get_date(text):
    """
    Get the date from the text and convert it to YYYY-MM-DD format
    :param text: Text to extract the date from
    :return:
    """
    try:
        pattern = r'\d+[-/]\d+[-/]\d+'
        date_matching = re.findall(pattern, text)[0]
        for fmt in ('%d/%m/%Y', '%m/%d/%Y'):
            try:
                return datetime.strptime(date_matching, fmt).strftime('%Y-%m-%d')
            except ValueError:
                pass
        return None
    except Exception as error:
        print(f"An error occurred: {error}")
        return None

def download_image(file_id, file_name, folder_name, service=None):
    if service is None:
        print("Service is not defined")
        return None

    folder_path = os.path.join("images", folder_name)
    os.makedirs(folder_path, exist_ok=True)
    image_path = os.path.join(folder_path, file_name)
    if os.path.exists(image_path):
        print(f"{file_name} already exists")
        return None
    else:
        request = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()

        with open(image_path, "wb") as file:
            file.write(fh.read())
            file.close()
        print(f"{file_name} downloaded successfully")
