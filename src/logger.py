"""
Logging System for Real-Time Data Pipeline
==========================================

Centralized logging configuration with multiple handlers and formatters.
Provides structured logging for different pipeline components.
"""

import os
import logging
import logging.handlers
from datetime import datetime
from typing import Optional
from config import config


class PipelineLogger:
    """Enhanced logging system for the data pipeline"""
    
    def __init__(self):
        self.loggers = {}
        self.setup_logging()
    
    def setup_logging(self):
        """Setup comprehensive logging configuration"""
        # Create logs directory if it doesn't exist
        os.makedirs(config.LOGS_DIR, exist_ok=True)
        
        # Clear any existing handlers
        logging.getLogger().handlers.clear()
        
        # Set root logger level
        logging.getLogger().setLevel(getattr(logging, config.LOG_LEVEL))
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )
        
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        error_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s'
        )
        
        # Setup main pipeline log file
        main_log_file = os.path.join(config.LOGS_DIR, 'pipeline.log')
        main_handler = logging.handlers.RotatingFileHandler(
            main_log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        main_handler.setFormatter(detailed_formatter)
        main_handler.setLevel(logging.INFO)
        
        # Setup error log file
        error_log_file = os.path.join(config.LOGS_DIR, 'errors.log')
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_file,
            maxBytes=5*1024*1024,  # 5MB
            backupCount=3
        )
        error_handler.setFormatter(error_formatter)
        error_handler.setLevel(logging.ERROR)
        
        # Setup console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(simple_formatter)
        console_handler.setLevel(logging.INFO)
        
        # Add handlers to root logger
        root_logger = logging.getLogger()
        root_logger.addHandler(main_handler)
        root_logger.addHandler(error_handler)
        root_logger.addHandler(console_handler)
        
        # Setup component-specific loggers
        self.setup_component_loggers()
        
        logging.info("Logging system initialized successfully")
    
    def setup_component_loggers(self):
        """Setup specialized loggers for different components"""
        components = [
            'validator',
            'transformer', 
            'analyzer',
            'database',
            'monitor',
            'main'
        ]
        
        for component in components:
            logger = logging.getLogger(component)
            
            # Component-specific log file
            component_log_file = os.path.join(config.LOGS_DIR, f'{component}.log')
            component_handler = logging.handlers.RotatingFileHandler(
                component_log_file,
                maxBytes=5*1024*1024,  # 5MB
                backupCount=2
            )
            
            formatter = logging.Formatter(
                f'%(asctime)s - {component.upper()} - %(levelname)s - %(message)s'
            )
            component_handler.setFormatter(formatter)
            component_handler.setLevel(logging.DEBUG)
            
            logger.addHandler(component_handler)
            self.loggers[component] = logger
    
    def get_logger(self, name: str) -> logging.Logger:
        """Get logger for specific component"""
        return self.loggers.get(name, logging.getLogger(name))
    
    def log_pipeline_start(self):
        """Log pipeline startup information"""
        logger = logging.getLogger('main')
        logger.info("=" * 60)
        logger.info("REAL-TIME DATA PIPELINE STARTING")
        logger.info("=" * 60)
        logger.info(f"Start time: {datetime.now()}")
        logger.info(f"Watch directory: {config.DATA_DIR}")
        logger.info(f"Database URL: {config.DATABASE_URL}")
        logger.info(f"Log level: {config.LOG_LEVEL}")
        logger.info("=" * 60)
    
    def log_pipeline_stop(self):
        """Log pipeline shutdown information"""
        logger = logging.getLogger('main')
        logger.info("=" * 60)
        logger.info("REAL-TIME DATA PIPELINE STOPPING")
        logger.info(f"Stop time: {datetime.now()}")
        logger.info("=" * 60)
    
    def log_file_processing_start(self, file_name: str):
        """Log start of file processing"""
        logger = logging.getLogger('main')
        logger.info(f">>> PROCESSING FILE: {file_name}")
    
    def log_file_processing_end(self, file_name: str, success: bool, processing_time: float):
        """Log end of file processing"""
        logger = logging.getLogger('main')
        status = "SUCCESS" if success else "FAILED"
        logger.info(f"<<< PROCESSING {status}: {file_name} ({processing_time:.2f}s)")
    
    def log_validation_summary(self, file_name: str, total_rows: int, valid_rows: int):
        """Log validation summary"""
        logger = logging.getLogger('validator')
        invalid_rows = total_rows - valid_rows
        success_rate = (valid_rows / total_rows * 100) if total_rows > 0 else 0
        
        logger.info(f"Validation summary for {file_name}:")
        logger.info(f"  Total rows: {total_rows}")
        logger.info(f"  Valid rows: {valid_rows}")
        logger.info(f"  Invalid rows: {invalid_rows}")
        logger.info(f"  Success rate: {success_rate:.1f}%")
    
    def log_transformation_summary(self, file_name: str, original_shape: tuple, final_shape: tuple):
        """Log transformation summary"""
        logger = logging.getLogger('transformer')
        logger.info(f"Transformation summary for {file_name}:")
        logger.info(f"  Original shape: {original_shape}")
        logger.info(f"  Final shape: {final_shape}")
        logger.info(f"  Columns added: {final_shape[1] - original_shape[1]}")
    
    def log_analysis_summary(self, file_name: str, metrics_count: int, anomalies: dict):
        """Log analysis summary"""
        logger = logging.getLogger('analyzer')
        logger.info(f"Analysis summary for {file_name}:")
        logger.info(f"  Metrics generated: {metrics_count}")
        
        for metric, anomaly_info in anomalies.items():
            if isinstance(anomaly_info, dict):
                anomaly_count = anomaly_info.get('z_score_anomalies', 0)
                logger.info(f"  {metric} anomalies: {anomaly_count}")
    
    def log_database_operation(self, operation: str, table: str, record_count: int, success: bool):
        """Log database operations"""
        logger = logging.getLogger('database')
        status = "SUCCESS" if success else "FAILED"
        logger.info(f"Database {operation} {status}: {record_count} records to {table}")
    
    def log_error_with_context(self, component: str, error: Exception, context: dict = None):
        """Log detailed error information with context"""
        logger = logging.getLogger(component)
        logger.error(f"ERROR in {component}: {str(error)}")
        
        if context:
            logger.error(f"Error context: {context}")
        
        # Log stack trace for debugging
        import traceback
        logger.debug(f"Stack trace: {traceback.format_exc()}")
    
    def create_daily_log_file(self, component: str) -> str:
        """Create daily log file for specific component"""
        date_str = datetime.now().strftime('%Y%m%d')
        log_file = os.path.join(config.LOGS_DIR, f'{component}_{date_str}.log')
        return log_file
    
    def cleanup_old_logs(self, days_to_keep: int = 30):
        """Clean up old log files"""
        try:
            import glob
            from datetime import timedelta
            
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            log_files = glob.glob(os.path.join(config.LOGS_DIR, '*.log*'))
            cleaned_count = 0
            
            for log_file in log_files:
                file_mtime = datetime.fromtimestamp(os.path.getmtime(log_file))
                if file_mtime < cutoff_date:
                    os.remove(log_file)
                    cleaned_count += 1
            
            if cleaned_count > 0:
                logging.info(f"Cleaned up {cleaned_count} old log files")
                
        except Exception as e:
            logging.error(f"Error cleaning up log files: {e}")

# Global logger instance
pipeline_logger = PipelineLogger()

# Convenience functions
def get_logger(component: str) -> logging.Logger:
    """Get logger for specific component"""
    return pipeline_logger.get_logger(component)

def log_pipeline_event(event_type: str, message: str, component: str = 'main'):
    """Log pipeline events"""
    logger = get_logger(component)
    logger.info(f"[{event_type}] {message}")

def log_performance_metric(metric_name: str, value: float, unit: str = '', component: str = 'main'):
    """Log performance metrics"""
    logger = get_logger(component)
    logger.info(f"PERFORMANCE - {metric_name}: {value}{unit}")