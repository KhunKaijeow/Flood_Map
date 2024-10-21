import easyocr
import numpy as np

from functions import *
from authentication import authenticate
import os
from dotenv import load_dotenv
from PIL import Image

load_dotenv()

parent_folder_id = os.getenv("PARENT_FOLDER_ID")
service = authenticate()
dt = fetch_files(parent_folder_id, service)
for index, row in tqdm(dt.iterrows(), total=dt.shape[0], desc="Download images"):
    image_path = os.path.join("images", row["folder_name"], row["file_name"])
    if os.path.exists(image_path):
        print(f"File already exists: {image_path}")
    else:
        download_image(row["file_id"], row["file_name"], row["folder_name"], service)

reader = easyocr.Reader(['en', 'th'])
list_of_lat_long = ['latitude', 'longitude', 'lat', 'long']
parent_dir = "images"
df = pd.DataFrame(columns=["file_name", "folder_name", "latitude", "longitude", "address", "date"])

for sub_dir in tqdm(os.listdir(parent_dir), desc=f"Parsing images in {parent_dir}", total=len(os.listdir(parent_dir))):
    sub_dir_path = os.path.join(parent_dir, sub_dir)

    # Check if it's a directory
    if os.path.isdir(sub_dir_path):
        images = os.listdir(sub_dir_path)
        for image in tqdm(images, desc=f"Parsing images in {sub_dir}", total=len(images)):
            image_path = os.path.join(sub_dir_path, image)
            if os.path.exists(image_path):
                try:
                    image_array = np.array(Image.open(image_path))
                    result = reader.readtext(image_array, detail=0, paragraph=True)
                    latitude, longitude, date_time, address = None, None, None, None
                    for line in result:
                        if any(word in line.lower() for word in list_of_lat_long):
                            latitude, longitude = get_lat_long(line)
                            address = get_address(line)
                            date_time = get_date(line)
                            print(
                                f"Latitude: {latitude}\nLongitude: {longitude}\nAddress: {address}\nDate: {date_time}\n")
                    df = pd.concat([df, pd.DataFrame([[image, sub_dir, latitude, longitude, address, date_time]],
                                                     columns=["file_name", "folder_name", "latitude", "longitude",
                                                              "address", "date"])])
                except Exception as error:
                    print(f"An error occurred: {error}")
            else:
                print(f"{image_path} does not exist")

# Merge dt (Google Drive file data) with df (OCR data)
data = dt.merge(df, left_on=['folder_name', 'file_name'], right_on=['folder_name', 'file_name'], how='left', suffixes=('_google', '_ocr'))

# Remove redundant columns
data = data.drop(['file_id'], axis=1)

# Save the final dataset
data.to_excel('merged_data.xlsx', index=False, header=True)
