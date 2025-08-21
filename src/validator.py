import pandas as pd
import os
import shutil
from datetime import datetime
from typing import Tuple, List
from config import config
from logger import get_logger

logger = get_logger(__name__)


class DataValidator:
    def __init__(self):
        self.validation_rules = {
            'temperature_celsius': {'min': config.TEMP_MIN, 'max': config.TEMP_MAX},
            'humidity_percent': {'min': config.HUMIDITY_MIN, 'max': config.HUMIDITY_MAX},
            'air_quality_index': {'min': config.AIR_QUALITY_MIN, 'max': config.AIR_QUALITY_MAX},
            'noise_level_db': {'min': config.NOISE_MIN, 'max': config.NOISE_MAX},
            'lighting_lux': {'min': config.LIGHTING_MIN, 'max': config.LIGHTING_MAX},
            'crowd_density': {'min': config.CROWD_DENSITY_MIN, 'max': config.CROWD_DENSITY_MAX},
            'stress_level': {'min': config.STRESS_LEVEL_MIN, 'max': config.STRESS_LEVEL_MAX},
            'sleep_hours': {'min': config.SLEEP_HOURS_MIN, 'max': config.SLEEP_HOURS_MAX},
            'mood_score': {'min': config.MOOD_SCORE_MIN, 'max': config.MOOD_SCORE_MAX},
            'mental_health_status': {'min': 0, 'max': 1}
        }
        self.required_columns = ['location_id', 'timestamp', 'stress_level', 'sleep_hours', 'mood_score',
                                 'mental_health_status', 'noise_level_db', 'lighting_lux', 'crowd_density',
                                 'temperature_celsius']  # left humidity

    def validate_file_structure(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate basic file structure and required columns"""
        errors = []

        # Check if DataFrame is empty
        if df.empty:
            errors.append("File is empty")
            return False, errors

        # Check for required columns
        missing_columns = [col for col in self.required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")

        # Check for at least one environmental data column
        env_columns = ['temperature_celsius', 'humidity_percent', 'air_quality_index', 'noise_level_db', 'lighting_lux']
        has_env_data = any(col in df.columns for col in env_columns)
        if not has_env_data:
            errors.append("No environmental data columns found")

        return len(errors) == 0, errors

    def validate_data_types(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Validate and convert data types"""
        errors = []
        validated_df = df.copy()

        # Validate location_id (should be string)
        if 'location_id' in validated_df.columns:
            validated_df['location_id'] = validated_df['location_id'].astype(str)

        # Validate timestamp
        if 'timestamp' in validated_df.columns:
            try:
                validated_df['timestamp'] = pd.to_datetime(validated_df['timestamp'], errors='coerce')
                invalid_timestamps = validated_df['timestamp'].isna().sum()
                if invalid_timestamps > 0:
                    errors.append(f"Found {invalid_timestamps} invalid timestamp formats")
            except Exception as e:
                errors.append(f"Error parsing timestamps: {str(e)}")

        # Validate numeric columns
        numeric_columns = ['temperature', 'humidity', 'pressure']
        for col in numeric_columns:
            if col in validated_df.columns:
                try:
                    validated_df[col] = pd.to_numeric(validated_df[col], errors='coerce')
                    invalid_count = validated_df[col].isna().sum()
                    if invalid_count > 0:
                        logger.warning(f"Found {invalid_count} invalid {col} values, converted to NaN")
                except Exception as e:
                    errors.append(f"Error converting {col} to numeric: {str(e)}")

        return validated_df, errors

    def validate_required_fields(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
        """Validate required fields and separate valid/invalid rows"""
        errors = []

        # Check for null values in required fields
        invalid_mask = pd.Series([False] * len(df), index=df.index)

        # Check all required columns for missing values
        for required_col in self.required_columns:
            if required_col in df.columns:
                if required_col == 'location_id':
                    # For location_id, check for null, empty string, or 'nan'
                    col_invalid = df[required_col].isna() | (df[required_col].astype(str) == '') | (
                                df[required_col].astype(str) == 'nan')
                elif required_col == 'timestamp':
                    # For timestamp, check for null values
                    col_invalid = df[required_col].isna()
                else:
                    # For other required columns, check for null values
                    col_invalid = df[required_col].isna()

                invalid_mask |= col_invalid
                invalid_count = col_invalid.sum()
                if invalid_count > 0:
                    errors.append(f"Found {invalid_count} rows with missing {required_col}")
            else:
                # Column is completely missing from dataset
                errors.append(f"Required column '{required_col}' is missing from dataset")
                # Mark all rows as invalid if required column is missing
                invalid_mask |= True

        # Additional check: At least one environmental reading must be present and valid
        env_columns = ['temperature_celsius', 'humidity_percent', 'air_quality_index', 'noise_level_db', 'lighting_lux']
        available_env_cols = [col for col in env_columns if col in df.columns]

        if available_env_cols and not invalid_mask.all():
            # Only check this if we have some valid rows left
            valid_rows_mask = ~invalid_mask
            valid_data_subset = df[valid_rows_mask]
            all_env_null = valid_data_subset[available_env_cols].isna().all(axis=1)

            if all_env_null.any():
                # Update invalid_mask for rows where all environmental data is missing
                env_invalid_indices = valid_data_subset[all_env_null].index
                invalid_mask.loc[env_invalid_indices] = True
                invalid_count = all_env_null.sum()
                errors.append(f"Found {invalid_count} rows with all environmental readings missing")

        valid_df = df[~invalid_mask].copy()
        invalid_df = df[invalid_mask].copy()

        return valid_df, invalid_df, errors

    def validate_sensor_ranges(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
        """Validate environmental and mental health data ranges"""
        errors = []
        valid_df = df.copy()
        invalid_rows = []

        for column, rules in self.validation_rules.items():
            if column in df.columns:
                # Check ranges for non-null values
                out_of_range = (
                        (df[column].notna()) &
                        ((df[column] < rules['min']) | (df[column] > rules['max']))
                )

                if out_of_range.any():
                    invalid_count = out_of_range.sum()
                    errors.append(
                        f"Found {invalid_count} {column} readings out of range ({rules['min']} to {rules['max']})")
                    invalid_rows.append(out_of_range)

        # Combine all range violations
        if invalid_rows:
            combined_invalid = pd.concat(invalid_rows, axis=1).any(axis=1)
            valid_df = df[~combined_invalid].copy()
            invalid_df = df[combined_invalid].copy()
        else:
            invalid_df = pd.DataFrame(columns=df.columns)

        return valid_df, invalid_df, errors

    def validate_data(self, df: pd.DataFrame, file_path: str) -> Tuple[pd.DataFrame, bool]:
        """Main validation function that orchestrates all validation steps"""
        file_name = os.path.basename(file_path)
        all_errors = []

        logger.info(f"Starting validation for file: {file_name}")

        # Remove completely empty rows first
        df_cleaned = df.dropna(how='all')
        empty_rows_removed = len(df) - len(df_cleaned)
        if empty_rows_removed > 0:
            logger.info(f"Removed {empty_rows_removed} completely empty rows from {file_name}")

        if df_cleaned.empty:
            logger.error(f"No valid data remaining after removing empty rows from {file_name}")
            return pd.DataFrame(), False

        # Step 1: Validate file structure
        structure_valid, structure_errors = self.validate_file_structure(df_cleaned)
        all_errors.extend(structure_errors)

        if not structure_valid:
            logger.error(f"File structure validation failed for {file_name}: {structure_errors}")
            self.quarantine_file(file_path, structure_errors)
            return pd.DataFrame(), False

        # Step 2: Validate and convert data types
        df_typed, type_errors = self.validate_data_types(df_cleaned)
        all_errors.extend(type_errors)

        # Step 3: Validate required fields
        df_required, invalid_required, required_errors = self.validate_required_fields(df_typed)
        all_errors.extend(required_errors)

        # Step 4: Validate sensor ranges
        df_valid, invalid_ranges, range_errors = self.validate_sensor_ranges(df_required)
        all_errors.extend(range_errors)

        # Combine all invalid data
        invalid_data_frames = []
        if not invalid_required.empty:
            invalid_data_frames.append(invalid_required)
        if not invalid_ranges.empty:
            invalid_data_frames.append(invalid_ranges)

        if invalid_data_frames:
            combined_invalid = pd.concat(invalid_data_frames, ignore_index=True)
            self.log_invalid_data(combined_invalid, file_name, all_errors)

        # Log validation summary
        total_rows = len(df)
        valid_rows = len(df_valid)
        invalid_rows = total_rows - valid_rows

        logger.info(f"Validation completed for {file_name}: {valid_rows}/{total_rows} rows valid")

        if invalid_rows > 0:
            logger.warning(f"Found {invalid_rows} invalid rows in {file_name}")

        # Quarantine file if too many invalid rows (>50%)
        if invalid_rows > (total_rows * 0.5):
            logger.error(f"Too many invalid rows ({invalid_rows}/{total_rows}) in {file_name}, quarantining file")
            self.quarantine_file(file_path, [f"More than 50% of rows invalid ({invalid_rows}/{total_rows})"])
            return pd.DataFrame(), False

        return df_valid, len(df_valid) > 0

    def quarantine_file(self, file_path: str, errors: List[str]):
        """Move invalid file to quarantine directory"""
        file_name = os.path.basename(file_path)
        quarantine_path = os.path.join(config.QUARANTINE_DIR, file_name)

        try:
            shutil.move(file_path, quarantine_path)
            logger.info(f"File {file_name} moved to quarantine")

            # Create error log
            error_log_path = os.path.join(config.QUARANTINE_DIR, f"{file_name}.error_log")
            with open(error_log_path, 'w') as f:
                f.write(f"File quarantined at: {datetime.now()}\n")
                f.write("Validation errors:\n")
                for error in errors:
                    f.write(f"- {error}\n")

        except Exception as e:
            logger.error(f"Failed to quarantine file {file_name}: {e}")

    def log_invalid_data(self, invalid_df: pd.DataFrame, file_name: str, errors: List[str]):
        """Log details about invalid data"""
        if invalid_df.empty:
            return

        log_file = os.path.join(config.LOGS_DIR, f"invalid_data_{datetime.now().strftime('%Y%m%d')}.log")

        try:
            with open(log_file, 'a') as f:
                f.write(f"\n=== Invalid data from {file_name} at {datetime.now()} ===\n")
                f.write(f"Validation errors: {errors}\n")
                f.write(f"Invalid rows count: {len(invalid_df)}\n")
                f.write("Sample invalid data:\n")
                f.write(invalid_df.head().to_string())
                f.write("\n" + "=" * 60 + "\n")
        except Exception as e:
            logger.error(f"Failed to log invalid data: {e}")


# Global validator instance
validator = DataValidator()
