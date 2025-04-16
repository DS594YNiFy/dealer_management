import os
from pathlib import Path


# folder_path = r"C:\Users\CYLK_\user_data\records\archived\20241021_zippo\meeting"
folder_path = "C:/Users/CYLK_/user_data/records/archived/20241021_zippo/meeting"

subfolders = [f.name for f in Path(folder_path).iterdir() if f.is_dir()]
files = [f.name for f in Path(folder_path).iterdir() if f.is_file()]

for folder in subfolders:
    print(folder)

for file in files:
    print(file)