import kagglehub
import os
import shutil
import config

# TODO: convert it to function to run as pipeline module for real-time data pipeline

# Get the project root directory (parent of src)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(project_root, "data")

# Ensure data directory exists
os.makedirs(data_dir, exist_ok=True)

# Download dataset
path = kagglehub.dataset_download(config.KAGGLE_DATASET)

# Move files to data directory
if os.path.exists(path):
    for file_name in os.listdir(path):
        src_file = os.path.join(path, file_name)
        dst_file = os.path.join(data_dir, file_name)
        if os.path.isfile(src_file):
            shutil.copy2(src_file, dst_file)
            print(f"Copied {file_name} to data directory")

print("Dataset files downloaded to:", data_dir)
# Clean up temporary directory
if os.path.exists(path):
    shutil.rmtree(path)
    print("Temporary directory cleaned up.")