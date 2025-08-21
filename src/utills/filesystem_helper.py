import os
import pandas as pd
from datetime import datetime
from config import config
from logger import get_logger

logger = get_logger(__name__)


# Utility functions
def validate_csv_file(file_path: str) -> bool:
    """Validate that a file is a readable CSV"""
    try:
        # Try to read just the header to validate
        df = pd.read_csv(file_path, nrows=0)
        return len(df.columns) > 0
    except Exception as e:
        logger.warning(f"File validation failed for {file_path}: {e}")
        return False


def move_processed_file(file_path: str, destination_dir: str = None):
    """Move processed file to archive directory"""
    if destination_dir is None:
        destination_dir = config.PROCESSED_DIR

    try:
        import shutil

        os.makedirs(destination_dir, exist_ok=True)
        file_name = os.path.basename(file_path)
        destination_path = os.path.join(destination_dir, file_name)

        # Add timestamp to avoid conflicts
        if os.path.exists(destination_path):
            name, ext = os.path.splitext(file_name)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            destination_path = os.path.join(destination_dir, f"{name}_{timestamp}{ext}")

        shutil.move(file_path, destination_path)
        logger.info(f"Moved processed file to: {destination_path}")

    except Exception as e:
        logger.error(f"Failed to move processed file {file_path}: {e}")
