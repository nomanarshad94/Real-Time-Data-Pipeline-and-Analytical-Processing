import pandas as pd
from logger import get_logger
from datetime import datetime
from typing import Dict, Any, Optional

logger = get_logger(__name__)


class DataTransformer:
    def __init__(self):
        self.unit_conversions = {
            'temperature': {'celsius_to_kelvin': lambda x: x + 273.15},
            'pressure': {'hpa_to_pa': lambda x: x * 100}
        }

    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names to lowercase with underscores"""
        df_transformed = df.copy()

        # Create column mapping
        column_mapping = {}
        for col in df_transformed.columns:
            # Convert to lowercase and replace spaces/special chars with underscores
            standardized = col.lower().strip()
            standardized = standardized.replace(' ', '_').replace('-', '_')
            standardized = ''.join(c if c.isalnum() or c == '_' else '_' for c in standardized)
            # Remove duplicate underscores
            while '__' in standardized:
                standardized = standardized.replace('__', '_')
            standardized = standardized.strip('_')
            column_mapping[col] = standardized

        df_transformed.rename(columns=column_mapping, inplace=True)
        logger.info(f"Standardized column names: {list(column_mapping.values())}")

        return df_transformed

    def add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived columns for analysis"""
        df_transformed = df.copy()

        # Add reading quality score based on data completeness for environmental metrics
        env_columns = ['temperature_celsius', 'humidity_percent', 'air_quality_index', 'noise_level_db', 'lighting_lux']
        available_env_columns = [col for col in env_columns if col in df_transformed.columns]

        if available_env_columns:
            df_transformed['data_quality_score'] = (
                    df_transformed[available_env_columns].notna().sum(axis=1) / len(available_env_columns)
            )

        # Add mental health risk indicators
        if all(col in df_transformed.columns for col in ['stress_level', 'sleep_hours', 'mood_score']):
            df_transformed['mental_health_risk'] = self.calculate_mental_health_risk(df_transformed)

        # Add environmental comfort index
        if all(col in df_transformed.columns for col in ['temperature_celsius', 'humidity_percent', 'noise_level_db']):
            df_transformed['comfort_index'] = self.calculate_comfort_index(df_transformed)

        # Add time-based features
        if 'timestamp' in df_transformed.columns:
            df_transformed['hour'] = pd.to_datetime(df_transformed['timestamp']).dt.hour
            df_transformed['day_of_week'] = pd.to_datetime(df_transformed['timestamp']).dt.dayofweek
            df_transformed['month'] = pd.to_datetime(df_transformed['timestamp']).dt.month

        return df_transformed

    def calculate_mental_health_risk(self, df: pd.DataFrame) -> pd.Series:
        """Calculate mental health risk score based on stress, sleep, and mood"""
        risk_scores = []

        for idx, row in df.iterrows():
            stress = row.get('stress_level', 0)
            sleep = row.get('sleep_hours', 8)
            mood = row.get('mood_score', 3)

            # Normalize factors (higher values = higher risk)
            stress_risk = min(stress / 100, 1.0)  # Normalize to 0-1
            sleep_risk = max(0, (8 - sleep) / 8)  # Less sleep = higher risk
            mood_risk = max(0, (3 - mood) / 3)  # Lower mood = higher risk

            # Combined risk score (0-1 scale)
            risk_score = (stress_risk + sleep_risk + mood_risk) / 3
            risk_scores.append(risk_score)

        return pd.Series(risk_scores, index=df.index)

    def calculate_comfort_index(self, df: pd.DataFrame) -> pd.Series:
        """Calculate environmental comfort index"""
        comfort_scores = []

        for idx, row in df.iterrows():
            temp = row.get('temperature_celsius', 22)
            humidity = row.get('humidity_percent', 50)
            noise = row.get('noise_level_db', 50)

            # Ideal ranges for comfort
            temp_comfort = 1 - abs(temp - 22) / 10  # Ideal: 22°C
            humidity_comfort = 1 - abs(humidity - 50) / 50  # Ideal: 50%
            noise_comfort = max(0, (70 - noise) / 70)  # Lower noise = better

            # Ensure scores are between 0 and 1
            temp_comfort = max(0, min(1, temp_comfort))
            humidity_comfort = max(0, min(1, humidity_comfort))
            noise_comfort = max(0, min(1, noise_comfort))

            comfort_index = (temp_comfort + humidity_comfort + noise_comfort) / 3
            comfort_scores.append(comfort_index)

        return pd.Series(comfort_scores, index=df.index)

    def normalize_units(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize units to standard formats"""
        df_transformed = df.copy()

        # Temperature: ensure Celsius (dataset already in Celsius)
        if 'temperature_celsius' in df_transformed.columns:
            # Check if values might be in Fahrenheit (typically > 50 for room temp)
            temp_mean = df_transformed['temperature_celsius'].mean()
            if pd.notna(temp_mean) and temp_mean > 50:
                logger.info("Temperature values appear to be in Fahrenheit, converting to Celsius")
                df_transformed['temperature_celsius'] = (df_transformed['temperature_celsius'] - 32) * 5 / 9

        # Ensure humidity is in percentage (0-100)
        if 'humidity_percent' in df_transformed.columns:
            humidity_max = df_transformed['humidity_percent'].max()
            if pd.notna(humidity_max) and humidity_max <= 1:  # Likely in decimal format
                logger.info("Humidity values appear to be in decimal format, converting to percentage")
                df_transformed['humidity_percent'] = df_transformed['humidity_percent'] * 100

        return df_transformed

    def handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle extreme outliers using statistical methods"""
        df_transformed = df.copy()

        # IoT environmental and mental health columns that can have outliers
        numeric_columns = [
            'temperature_celsius', 'humidity_percent', 'air_quality_index', 'noise_level_db', 
            'lighting_lux', 'crowd_density', 'stress_level', 'sleep_hours', 'mood_score'
        ]

        for col in numeric_columns:
            if col in df_transformed.columns:
                # Use IQR method to identify outliers
                Q1 = df_transformed[col].quantile(0.25)
                Q3 = df_transformed[col].quantile(0.75)
                IQR = Q3 - Q1

                # Define outlier bounds (1.5 * IQR is standard, but we'll use 3 * IQR for sensor data)
                lower_bound = Q1 - 3 * IQR
                upper_bound = Q3 + 3 * IQR

                # Count outliers
                outliers = ((df_transformed[col] < lower_bound) | (df_transformed[col] > upper_bound))
                outlier_count = outliers.sum()

                if outlier_count > 0:
                    logger.warning(f"Found {outlier_count} extreme outliers in {col}, capping values")

                    # Cap outliers instead of removing them
                    df_transformed.loc[df_transformed[col] < lower_bound, col] = lower_bound
                    df_transformed.loc[df_transformed[col] > upper_bound, col] = upper_bound

        return df_transformed

    def interpolate_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Interpolate missing values using time-based interpolation"""
        df_transformed = df.copy()

        # Sort by timestamp for proper interpolation
        if 'timestamp' in df_transformed.columns:
            df_transformed = df_transformed.sort_values('timestamp')

        # IoT columns suitable for interpolation (environmental data)
        numeric_columns = [
            'temperature_celsius', 'humidity_percent', 'air_quality_index', 'noise_level_db', 'lighting_lux'
        ]

        for col in numeric_columns:
            if col in df_transformed.columns:
                missing_count = df_transformed[col].isna().sum()
                if missing_count > 0:
                    logger.info(f"Interpolating {missing_count} missing values in {col}")

                    # Use linear interpolation for time series data
                    df_transformed[col] = df_transformed[col].interpolate(
                        method='linear',
                        limit_direction='both',
                        limit=5  # Limit interpolation to 5 consecutive missing values
                    )

                    # Fill remaining NaN with forward/backward fill
                    df_transformed[col] = df_transformed[col].fillna(method='ffill').fillna(method='bfill')

        return df_transformed

    def add_metadata(self, df: pd.DataFrame, file_name: str, data_source: str) -> pd.DataFrame:
        """Add metadata columns to the transformed data"""
        df_transformed = df.copy()

        # Add file metadata
        df_transformed['file_name'] = file_name
        df_transformed['data_source'] = data_source
        df_transformed['processing_timestamp'] = datetime.utcnow()

        # Add data version (for tracking transformations)
        df_transformed['data_version'] = '1.0'

        return df_transformed

    def transform_data(self, df: pd.DataFrame, file_name: str, data_source: str) -> pd.DataFrame:
        """Main transformation pipeline"""
        logger.info(f"Starting data transformation for file: {file_name}")

        # Step 1: Standardize column names
        df_transformed = self.standardize_column_names(df)

        # Step 2: Add derived columns
        df_transformed = self.add_derived_columns(df_transformed)

        # Step 3: Normalize units
        df_transformed = self.normalize_units(df_transformed)

        # Step 4: Handle outliers
        df_transformed = self.handle_outliers(df_transformed)

        # Step 5: Interpolate missing values (conservative approach)
        df_transformed = self.interpolate_missing_values(df_transformed)

        # Step 6: Add metadata
        df_transformed = self.add_metadata(df_transformed, file_name, data_source)

        logger.info(f"Data transformation completed for {file_name}")
        logger.info(f"Transformed data shape: {df_transformed.shape}")

        return df_transformed

    def get_transformation_summary(self, original_df: pd.DataFrame, transformed_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate a summary of transformations applied"""
        summary = {
            'original_shape': original_df.shape,
            'transformed_shape': transformed_df.shape,
            'columns_added': len(transformed_df.columns) - len(original_df.columns),
            'original_columns': list(original_df.columns),
            'transformed_columns': list(transformed_df.columns),
            'transformation_timestamp': datetime.utcnow().isoformat()
        }

        # Calculate data quality improvements
        numeric_columns = [
            'temperature_celsius', 'humidity_percent', 'air_quality_index', 'noise_level_db', 
            'lighting_lux', 'stress_level', 'sleep_hours', 'mood_score'
        ]
        original_missing = sum(original_df[col].isna().sum() for col in numeric_columns if col in original_df.columns)
        transformed_missing = sum(
            transformed_df[col].isna().sum() for col in numeric_columns if col in transformed_df.columns)

        summary['missing_values_reduced'] = original_missing - transformed_missing

        return summary


# Global transformer instance
transformer = DataTransformer()
