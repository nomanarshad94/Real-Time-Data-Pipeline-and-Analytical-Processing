#!/usr/bin/env python3
"""
Real-Time Data Pipeline Main Application Module
========================================

Main orchestrator for the real-time sensor data processing pipeline.
Handles file monitoring, validation, transformation, analysis, and database storage.

Author: Noman Arshad
Date: 2025
"""

import os
import sys
import time
import signal
import pandas as pd
from datetime import datetime
from typing import Optional
import argparse

# Add src directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import config
from database import db_manager
from validator import validator
from transformer import transformer
from analyzer import analyzer
from monitor import FileMonitor
from logger import get_logger
from utills.filesystem_helper import move_processed_file

logger = get_logger(__name__)


class DataPipeline:
    """Main data pipeline orchestrator"""

    def __init__(self):
        self.monitor = None
        self.is_running = False
        self.processed_count = 0
        self.failed_count = 0
        self.start_time = None

    def setup_database(self):
        """Initialize database tables"""
        try:
            logger.info("Setting up database...")
            db_manager.create_tables()

            # Test database connection
            if not db_manager.health_check():
                raise Exception("Database health check failed")

            logger.info("Database setup completed successfully")

        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise

    def process_file(self, file_path: str):
        """Process a single data file through the complete pipeline"""
        file_name = os.path.basename(file_path)
        data_source = "kaggle_iot_dataset"

        start_time = time.time()
        logger.info(f"Processing file: {file_name}")

        try:
            # Step 1: Load data
            logger.info(f"Loading data from {file_name}")
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} rows from {file_name}")

            if df.empty:
                logger.warning(f"File {file_name} is empty, skipping")
                return False

            # Step 2: Validate data
            logger.info(f"Validating data from {file_name}")
            df_valid, is_valid = validator.validate_data(df, file_path)

            if not is_valid or df_valid.empty:
                logger.error(f"Validation failed for {file_name}")
                self.failed_count += 1
                return False

            logger.info(f"Validation passed for {file_name}: {len(df_valid)} valid rows")


            return True

        except Exception as e:
            logger.error(f"Error processing file {file_name}: {e}")
            self.failed_count += 1
            return False

    def retry_with_backoff(self, func, *args, max_retries: int = None, **kwargs):
        """Execute function with retry logic and exponential backoff"""
        if max_retries is None:
            max_retries = config.MAX_RETRIES

        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Function failed after {max_retries} attempts: {e}")
                    raise

                wait_time = config.RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Attempt {attempt + 1} failed: {e}, retrying in {wait_time}s")
                time.sleep(wait_time)

    def process_file_with_retry(self, file_path: str):
        """Process file with retry mechanism"""
        try:
            return self.retry_with_backoff(self.process_file, file_path)
        except Exception as e:
            logger.error(f"File processing failed permanently for {file_path}: {e}")
            return False

    def start_monitoring(self, use_polling: bool = False):
        """Start the file monitoring and processing pipeline"""
        try:
            logger.info("Starting real-time data pipeline...")
            self.start_time = datetime.utcnow()
            self.is_running = True

            # Setup database
            self.setup_database()

            # Initialize file monitor
            self.monitor = FileMonitor(
                watch_directory=config.DATA_DIR,
                process_callback=self.process_file_with_retry
            )

            # Start monitoring
            logger.info(f"Starting file monitoring on {config.DATA_DIR}")
            self.monitor.start_monitoring(use_polling=use_polling)

            # Keep main thread alive for watchdog monitoring
            if not use_polling:
                self.monitor.wait_for_files()

        except KeyboardInterrupt:
            logger.info("Pipeline stopped by user")
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            raise
        finally:
            self.stop_monitoring()

    def stop_monitoring(self):
        """Stop the monitoring pipeline"""
        logger.info("Stopping pipeline...")
        self.is_running = False

        if self.monitor:
            self.monitor.stop_monitoring()
            self.monitor = None

        # Log final statistics
        if self.start_time:
            runtime = datetime.utcnow() - self.start_time
            logger.info(f"Pipeline stopped after {runtime}")
            logger.info(f"Final statistics: {self.processed_count} processed, {self.failed_count} failed")

    def process_single_file(self, file_path: str) -> bool:
        """Process a single file (for testing/manual processing)"""
        try:
            self.setup_database()
            return self.process_file_with_retry(file_path)
        except Exception as e:
            logger.error(f"Single file processing failed: {e}")
            return False

    def get_pipeline_status(self) -> dict:
        """Get current pipeline status"""
        runtime = None
        if self.start_time:
            runtime = str(datetime.utcnow() - self.start_time)

        return {
            'is_running': self.is_running,
            'runtime': runtime,
            'processed_count': self.processed_count,
            'failed_count': self.failed_count,
            'success_rate': (self.processed_count / max(1, self.processed_count + self.failed_count)) * 100,
            'database_healthy': db_manager.health_check() if db_manager else False
        }


# Global pipeline instance
pipeline = DataPipeline()


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    pipeline.stop_monitoring()
    sys.exit(0)


def main():
    """Main entry point of data pipeline"""
    parser = argparse.ArgumentParser(description='Real-Time Data Pipeline')
    parser.add_argument('--file', '-f', help='Process a single file')
    parser.add_argument('--polling', '-p', action='store_true',
                        help='Use polling instead of watchdog for file monitoring')
    parser.add_argument('--status', '-s', action='store_true',
                        help='Show pipeline status')
    parser.add_argument('--init-db', action='store_true',
                        help='Initialize database tables only')

    args = parser.parse_args()

    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        if args.init_db:
            logger.info("Initializing database...")
            pipeline.setup_database()
            logger.info("Database initialization completed")

        elif args.status:
            status = pipeline.get_pipeline_status()
            print("\nPipeline Status:")
            print("===============")
            for key, value in status.items():
                print(f"{key}: {value}")

        elif args.file:
            if not os.path.exists(args.file):
                logger.error(f"File not found: {args.file}")
                sys.exit(1)

            logger.info(f"Processing single file: {args.file}")
            success = pipeline.process_single_file(args.file)
            sys.exit(0 if success else 1)

        else:
            # Start continuous monitoring
            pipeline.start_monitoring(use_polling=args.polling)

    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
