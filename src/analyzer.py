import pandas as pd
import numpy as np
from logger import get_logger
from datetime import datetime
from typing import Dict, List, Any
from config import config

logger = get_logger(__name__)


class DataAnalyzer:
    def __init__(self):
        self.metric_columns = [
            'temperature_celsius', 'humidity_percent', 'air_quality_index',
            'noise_level_db', 'lighting_lux', 'crowd_density',
            'stress_level', 'sleep_hours', 'mood_score'
        ]

    def calculate_basic_statistics(self, df: pd.DataFrame, group_by: List[str] = None) -> Dict[str, Any]:
        """Calculate basic statistical metrics for sensor data"""
        if group_by is None:
            group_by = ['location_id']

        stats_results = {}

        # Ensure we have data to analyze
        if df.empty:
            logger.warning("No data provided for statistical analysis")
            return stats_results

        # Get available metric columns
        available_metrics = [col for col in self.metric_columns if col in df.columns]

        if not available_metrics:
            logger.warning("No metric columns found for analysis")
            return stats_results

        # Group data if specified
        if group_by and all(col in df.columns for col in group_by):
            grouped = df.groupby(group_by)
        else:
            # Treat entire dataset as one group
            grouped = [(('all_data',), df)]

        for group_name, group_data in grouped:
            if isinstance(group_name, tuple):
                group_key = '_'.join(str(g) for g in group_name)
            else:
                group_key = str(group_name)

            group_stats = {}

            for metric in available_metrics:
                if metric in group_data.columns:
                    metric_data = group_data[metric].dropna()

                    if len(metric_data) > 0:
                        group_stats[metric] = {
                            'count': len(metric_data),
                            'min': float(metric_data.min()),
                            'max': float(metric_data.max()),
                            'mean': float(metric_data.mean()),
                            'median': float(metric_data.median()),
                            'std': float(metric_data.std()) if len(metric_data) > 1 else 0.0,
                            'variance': float(metric_data.var()) if len(metric_data) > 1 else 0.0,
                            'percentile_25': float(metric_data.quantile(0.25)),
                            'percentile_75': float(metric_data.quantile(0.75)),
                            'skewness': float(metric_data.skew()),
                            'kurtosis': float(metric_data.kurtosis())
                        }
                    else:
                        group_stats[metric] = self.empty_stats()

            stats_results[group_key] = group_stats

        return stats_results

    def empty_stats(self) -> Dict[str, float]:
        """Return empty statistics structure"""
        return {
            'count': 0,
            'min': 0.0,
            'max': 0.0,
            'mean': 0.0,
            'median': 0.0,
            'std': 0.0,
            'variance': 0.0,
            'percentile_25': 0.0,
            'percentile_75': 0.0,
            'skewness': 0.0,
            'kurtosis': 0.0
        }

    def calculate_correlation_matrix(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate correlation matrix for numeric sensor data"""
        available_metrics = [col for col in self.metric_columns if col in df.columns]

        if len(available_metrics) < 2:
            logger.warning("Need at least 2 metrics for correlation analysis")
            return {}

        correlation_data = df[available_metrics].corr()

        # Convert to dictionary format
        correlations = {}
        for i, col1 in enumerate(available_metrics):
            for j, col2 in enumerate(available_metrics):
                if i < j:  # Avoid duplicate pairs
                    corr_value = correlation_data.loc[col1, col2]
                    if pd.notna(corr_value):
                        correlations[f"{col1}_vs_{col2}"] = float(corr_value)

        return correlations

    def detect_anomalies(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect anomalies in sensor data using statistical methods"""
        anomaly_results = {}

        for metric in self.metric_columns:
            if metric in df.columns:
                metric_data = df[metric].dropna()

                if len(metric_data) > 10:  # Need sufficient data for anomaly detection
                    # Z-score based anomaly detection
                    z_scores = np.abs((metric_data - metric_data.mean()) / metric_data.std())
                    z_anomalies = z_scores > 3  # 3 standard deviations

                    # IQR based anomaly detection
                    Q1 = metric_data.quantile(0.25)
                    Q3 = metric_data.quantile(0.75)
                    IQR = Q3 - Q1
                    iqr_anomalies = (metric_data < (Q1 - 1.5 * IQR)) | (metric_data > (Q3 + 1.5 * IQR))

                    anomaly_results[metric] = {
                        'z_score_anomalies': int(z_anomalies.sum()),
                        'iqr_anomalies': int(iqr_anomalies.sum()),
                        'total_readings': len(metric_data),
                        'anomaly_percentage_zscore': float(z_anomalies.sum() / len(metric_data) * 100),
                        'anomaly_percentage_iqr': float(iqr_anomalies.sum() / len(metric_data) * 100)
                    }

        return anomaly_results

    def analyze_temporal_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze temporal patterns in sensor data"""
        temporal_analysis = {}

        if 'timestamp' not in df.columns:
            logger.warning("No timestamp column found for temporal analysis")
            return temporal_analysis

        # Convert timestamp to datetime if not already
        df_temp = df.copy()
        df_temp['timestamp'] = pd.to_datetime(df_temp['timestamp'])
        df_temp['hour'] = df_temp['timestamp'].dt.hour
        df_temp['day_of_week'] = df_temp['timestamp'].dt.day_name()

        for metric in self.metric_columns:
            if metric in df_temp.columns:
                metric_data = df_temp[metric].dropna()

                if len(metric_data) > 0:
                    # Hourly patterns
                    hourly_stats = df_temp.groupby('hour')[metric].agg(['mean', 'std', 'count']).round(2)

                    # Daily patterns
                    daily_stats = df_temp.groupby('day_of_week')[metric].agg(['mean', 'std', 'count']).round(2)

                    temporal_analysis[metric] = {
                        'hourly_patterns': hourly_stats.to_dict(),
                        'daily_patterns': daily_stats.to_dict(),
                        'peak_hour': int(hourly_stats['mean'].idxmax()),
                        'lowest_hour': int(hourly_stats['mean'].idxmin()),
                        'most_variable_hour': int(hourly_stats['std'].idxmax())
                    }

        return temporal_analysis

    def calculate_data_quality_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate data quality metrics"""
        quality_metrics = {}
        total_rows = len(df)

        if total_rows == 0:
            return quality_metrics

        # Overall completeness
        total_cells = total_rows * len(df.columns)
        missing_cells = df.isna().sum().sum()
        completeness = ((total_cells - missing_cells) / total_cells) * 100

        quality_metrics['overall_completeness'] = round(completeness, 2)
        quality_metrics['total_rows'] = total_rows
        quality_metrics['total_missing_values'] = int(missing_cells)

        # Column-specific quality metrics
        column_quality = {}
        for column in df.columns:
            if column in self.metric_columns:
                missing_count = df[column].isna().sum()
                column_completeness = ((total_rows - missing_count) / total_rows) * 100

                column_quality[column] = {
                    'completeness': round(column_completeness, 2),
                    'missing_count': int(missing_count),
                    'data_type': str(df[column].dtype)
                }

        quality_metrics['column_quality'] = column_quality

        return quality_metrics

    def generate_aggregated_metrics(self, df: pd.DataFrame, file_name: str, data_source: str) -> List[Dict[str, Any]]:
        """Generate aggregated metrics for database storage"""
        aggregated_metrics = []

        # Calculate statistics grouped by location
        stats = self.calculate_basic_statistics(df, ['location_id'])

        for location_id, location_stats in stats.items():
            for metric_name, metric_stats in location_stats.items():
                if metric_stats['count'] > 0:  # Only include metrics with data
                    aggregated_metric = {
                        'location_id': location_id,
                        'metric_name': metric_name,
                        'min_value': metric_stats['min'],
                        'max_value': metric_stats['max'],
                        'avg_value': metric_stats['mean'],
                        'std_value': metric_stats['std'],
                        'count': metric_stats['count'],
                        'file_name': file_name,
                        'data_source': data_source,
                        'analysis_timestamp': datetime.utcnow()
                    }
                    aggregated_metrics.append(aggregated_metric)

        return aggregated_metrics

    def analyze_data(self, df: pd.DataFrame, file_name: str, data_source: str) -> Dict[str, Any]:
        """Main analysis function that orchestrates all analysis steps"""
        logger.info(f"Starting data analysis for file: {file_name}")

        analysis_results = {
            'file_name': file_name,
            'data_source': data_source,
            'analysis_timestamp': datetime.utcnow().isoformat(),
            'basic_statistics': {},
            'correlations': {},
            'anomalies': {},
            'temporal_patterns': {},
            'data_quality': {},
            'aggregated_metrics': []
        }

        try:
            # Basic statistics
            analysis_results['basic_statistics'] = self.calculate_basic_statistics(df)
            logger.info(f"Calculated basic statistics for {len(analysis_results['basic_statistics'])} groups")

            # Correlation analysis
            analysis_results['correlations'] = self.calculate_correlation_matrix(df)
            logger.info(f"Calculated {len(analysis_results['correlations'])} correlations")

            # Anomaly detection
            analysis_results['anomalies'] = self.detect_anomalies(df)
            logger.info(f"Detected anomalies in {len(analysis_results['anomalies'])} metrics")

            # Temporal patterns
            analysis_results['temporal_patterns'] = self.analyze_temporal_patterns(df)
            logger.info(f"Analyzed temporal patterns for {len(analysis_results['temporal_patterns'])} metrics")

            # Data quality metrics
            analysis_results['data_quality'] = self.calculate_data_quality_metrics(df)
            logger.info(f"Calculated data quality metrics")

            # Generate aggregated metrics for database
            analysis_results['aggregated_metrics'] = self.generate_aggregated_metrics(df, file_name, data_source)
            logger.info(f"Generated {len(analysis_results['aggregated_metrics'])} aggregated metrics")

            logger.info(f"Data analysis completed for {file_name}")

        except Exception as e:
            logger.error(f"Error during data analysis for {file_name}: {e}")
            analysis_results['error'] = str(e)

        return analysis_results

    def save_analysis_report(self, analysis_results: Dict[str, Any]) -> str:
        """Save analysis report to logs directory"""
        try:
            import os
            import json

            report_filename = f"analysis_report_{analysis_results['file_name']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            report_path = os.path.join(config.LOGS_DIR, report_filename)

            with open(report_path, 'w') as f:
                json.dump(analysis_results, f, indent=2, default=str)

            logger.info(f"Analysis report saved to: {report_path}")
            return report_path

        except Exception as e:
            logger.error(f"Failed to save analysis report: {e}")
            return ""


# Global analyzer instance
analyzer = DataAnalyzer()
