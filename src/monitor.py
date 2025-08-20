import os
import time
from logger import get_logger
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from config import config


logger = get_logger(__name__)


class DataFileHandler(FileSystemEventHandler):
    """Raw data event handler for CSV file monitoring"""

    def __init__(self, process_callback: Callable[[str], None]):
        super().__init__()
        self.process_callback = process_callback
        self.processed_files = set()  # Track processed files to avoid duplicates

    def on_created(self, event):
        """Called when a file is created"""
        if not event.is_directory:
            self.handle_file_event(event.src_path, "created")

    def on_modified(self, event):
        """Called when a file is modified"""
        if not event.is_directory:
            self.handle_file_event(event.src_path, "modified")

    def handle_file_event(self, file_path: str, event_type: str):
        """Handle file system events for CSV files"""
        try:
            # Check if file has valid extension
            if not any(file_path.lower().endswith(ext) for ext in config.FILE_EXTENSIONS):
                return

            # Avoid processing the same file multiple times
            if file_path in self.processed_files:
                return

            # Wait for file to be fully written (simple approach)
            time.sleep(2)

            # Check if file exists and is readable
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                logger.info(f"New file detected: {file_path} ({event_type})")
                self.processed_files.add(file_path)

                # Call the processing callback
                self.process_callback(file_path)

        except Exception as e:  # TODO: refine exception handling
            logger.error(f"Error handling file event for {file_path}: {e}")


class DirectoryMonitor:
    """Monitor directory for new CSV files and trigger processing"""

    def __init__(self, watch_directory: str, process_callback: Callable[[str], None]):
        self.watch_directory = watch_directory
        self.process_callback = process_callback
        self.observer = None
        self.event_handler = DataFileHandler(process_callback)
        self.is_running = False

        # Ensure watch directory exists
        os.makedirs(watch_directory, exist_ok=True)
        logger.info(f"Monitoring directory: {watch_directory}")

    def start_monitoring(self):
        """Start monitoring the directory"""
        try:
            self.observer = Observer()
            self.observer.schedule(
                self.event_handler,
                self.watch_directory,
                recursive=False
            )

            self.observer.start()
            self.is_running = True
            logger.info(f"Started monitoring {self.watch_directory}")

            # Process existing files in directory
            self.process_existing_files()

        except Exception as e:
            logger.error(f"Failed to start directory monitoring: {e}")
            raise

    def stop_monitoring(self):
        """Stop monitoring the directory"""
        if self.observer and self.is_running:
            self.observer.stop()
            self.observer.join()
            self.is_running = False
            logger.info("Stopped directory monitoring")

    def process_existing_files(self):
        """Process any existing files in the directory"""
        try:
            existing_files = []
            for ext in config.FILE_EXTENSIONS:
                pattern = f"*{ext}"
                existing_files.extend(Path(self.watch_directory).glob(pattern))

            if existing_files:
                logger.info(f"Found {len(existing_files)} existing files to process")

                for file_path in existing_files:
                    file_path_str = str(file_path)
                    if file_path_str not in self.event_handler.processed_files:
                        logger.info(f"Processing existing file: {file_path_str}")
                        self.event_handler.processed_files.add(file_path_str)
                        self.process_callback(file_path_str)
            else:
                logger.info("No existing files found in directory")

        except Exception as e:
            logger.error(f"Error processing existing files: {e}")

    def wait_for_files(self, timeout: Optional[float] = None):
        """Wait for files to be processed (for testing/development)"""
        start_time = time.time()

        try:
            while self.is_running:
                time.sleep(1)

                if timeout and (time.time() - start_time) > timeout:
                    logger.info(f"Monitor timeout reached ({timeout}s)")
                    break

        except KeyboardInterrupt:
            logger.info("Monitoring interrupted by user")
        finally:
            self.stop_monitoring()


class PollingMonitor:
    """Alternative polling-based monitor for environments where watchdog doesn't work well"""

    def __init__(self, watch_directory: str, process_callback: Callable[[str], None]):
        self.watch_directory = watch_directory
        self.process_callback = process_callback
        self.processed_files = set()
        self.last_check = datetime.now()
        self.is_running = False

        # Ensure watch directory exists
        os.makedirs(watch_directory, exist_ok=True)
        logger.info(f"Polling directory: {watch_directory}")

    def start_monitoring(self, poll_interval: int = None):
        """Start polling the directory"""
        if poll_interval is None:
            poll_interval = config.MONITOR_INTERVAL

        self.is_running = True
        logger.info(f"Started polling {self.watch_directory} every {poll_interval} seconds")

        # Process existing files first
        self.scan_directory()

        try:
            while self.is_running:
                time.sleep(poll_interval)
                self.scan_directory()

        except KeyboardInterrupt:
            logger.info("Polling interrupted by user")
        finally:
            self.stop_monitoring()

    def stop_monitoring(self):
        """Stop polling"""
        self.is_running = False
        logger.info("Stopped polling")

    def scan_directory(self):
        """Scan directory for new files"""
        try:
            current_files = set()

            # Get all files matching our extensions
            for ext in config.FILE_EXTENSIONS:
                pattern = f"*{ext}"
                files = list(Path(self.watch_directory).glob(pattern))
                current_files.update(str(f) for f in files)

            # Find new files
            new_files = current_files - self.processed_files

            for file_path in new_files:
                if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                    # Check if file was modified recently
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))

                    if file_mtime >= self.last_check:
                        logger.info(f"New file detected: {file_path}")
                        self.processed_files.add(file_path)
                        self.process_callback(file_path)

            self.last_check = datetime.now()

        except Exception as e:
            logger.error(f"Error scanning directory: {e}")


class FileMonitor:
    """Main file monitoring class that chooses the appropriate monitoring strategy"""

    def __init__(self, watch_directory: str = None, process_callback: Callable[[str], None] = None):
        self.watch_directory = watch_directory or config.DATA_DIR
        self.process_callback = process_callback or self.default_process_callback
        self.monitor = None
        self.monitor_type = "watchdog"  # or "polling"

    def default_process_callback(self, file_path: str):
        """Default callback that just logs the file detection"""
        logger.info(f"File detected for processing: {file_path}")

    def start_monitoring(self, use_polling: bool = False, poll_interval: int = None):
        """Start file monitoring using either watchdog or polling"""
        try:
            if use_polling:
                self.monitor = PollingMonitor(self.watch_directory, self.process_callback)
                self.monitor_type = "polling"
                self.monitor.start_monitoring(poll_interval)
            else:
                self.monitor = DirectoryMonitor(self.watch_directory, self.process_callback)
                self.monitor_type = "watchdog"
                self.monitor.start_monitoring()

        except Exception as e:
            logger.error(f"Failed to start monitoring: {e}")

            # Fallback to polling if watchdog fails
            if not use_polling:
                logger.info("Falling back to polling method")
                self.start_monitoring(use_polling=True, poll_interval=poll_interval)
            else:
                raise

    def stop_monitoring(self):
        """Stop monitoring"""
        if self.monitor:
            self.monitor.stop_monitoring()
            self.monitor = None

    def is_running(self) -> bool:
        """Check if monitoring is active"""
        if self.monitor:
            return getattr(self.monitor, 'is_running', False)
        return False

    def wait_for_files(self, timeout: Optional[float] = None):
        """Wait for files (only works with watchdog monitor)"""
        if self.monitor and self.monitor_type == "watchdog":
            self.monitor.wait_for_files(timeout)
        elif self.monitor_type == "polling":
            logger.warning("wait_for_files not supported with polling monitor")


# Create global monitor instance (will be initialized by main pipeline)
file_monitor = None
