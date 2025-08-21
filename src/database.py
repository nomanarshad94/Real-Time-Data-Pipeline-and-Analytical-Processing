from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime
import pandas as pd
from logger import get_logger
from typing import Dict, List, Optional
from config import config

logger = get_logger(__name__)
Base = declarative_base()


class RawSensorData(Base):
    __tablename__ = "raw_sensor_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(String(50), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    temperature_celsius = Column(Float)
    humidity_percent = Column(Float)
    air_quality_index = Column(Integer)
    noise_level_db = Column(Float)
    lighting_lux = Column(Float)
    crowd_density = Column(Integer)
    stress_level = Column(Integer)
    sleep_hours = Column(Float)
    mood_score = Column(Float)
    mental_health_status = Column(Integer)
    file_name = Column(String(255), nullable=False)
    data_source = Column(String(100), nullable=False)
    processed_at = Column(DateTime, default=datetime.utcnow)

    # Indexes for better query performance
    __table_args__ = (
        Index('idx_location_id', 'location_id'),
        Index('idx_timestamp', 'timestamp'),
        Index('idx_processed_at', 'processed_at'),
        Index('idx_mental_health_status', 'mental_health_status'),
    )


class AggregatedMetrics(Base):
    __tablename__ = "aggregated_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(String(50), nullable=False)
    data_source = Column(String(100), nullable=False)
    file_name = Column(String(255), nullable=False)
    metric_name = Column(String(50), nullable=False)  # temperature_celsius, humidity_percent, air_quality_index, etc.
    min_value = Column(Float)
    max_value = Column(Float)
    avg_value = Column(Float)
    std_value = Column(Float)
    count = Column(Integer)
    timestamp = Column(DateTime, default=datetime.utcnow)

    # Indexes for better query performance
    __table_args__ = (
        Index('idx_location_metric', 'location_id', 'metric_name'),
        Index('idx_data_source', 'data_source'),
        Index('idx_timestamp_agg', 'timestamp'),
    )


class DatabaseManager:
    def __init__(self):
        self.engine = None
        self.SessionLocal = None
        self.connect()

    def connect(self):
        """Establish database connection"""
        try:
            self.engine = create_engine(
                config.DATABASE_URL,
                pool_pre_ping=True,
                pool_recycle=300
            )
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def create_tables(self):
        """Create database tables"""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    def get_session(self) -> Session:
        """Get database session"""
        return self.SessionLocal()

    def insert_raw_data(self, df: pd.DataFrame, file_name: str, data_source: str) -> bool:
        """Insert raw sensor data into database"""
        session = self.get_session()
        try:
            records = []
            for _, row in df.iterrows():
                record = RawSensorData(
                    location_id=str(row.get('location_id', '')),
                    timestamp=pd.to_datetime(row['timestamp']),
                    temperature_celsius=float(row['temperature_celsius']) if pd.notna(row.get('temperature_celsius')) else None,
                    humidity_percent=float(row['humidity_percent']) if pd.notna(row.get('humidity_percent')) else None,
                    air_quality_index=int(row['air_quality_index']) if pd.notna(row.get('air_quality_index')) else None,
                    noise_level_db=float(row['noise_level_db']) if pd.notna(row.get('noise_level_db')) else None,
                    lighting_lux=float(row['lighting_lux']) if pd.notna(row.get('lighting_lux')) else None,
                    crowd_density=int(row['crowd_density']) if pd.notna(row.get('crowd_density')) else None,
                    stress_level=int(row['stress_level']) if pd.notna(row.get('stress_level')) else None,
                    sleep_hours=float(row['sleep_hours']) if pd.notna(row.get('sleep_hours')) else None,
                    mood_score=float(row['mood_score']) if pd.notna(row.get('mood_score')) else None,
                    mental_health_status=int(row['mental_health_status']) if pd.notna(row.get('mental_health_status')) else None,
                    file_name=file_name,
                    data_source=data_source
                )
                records.append(record)

            session.add_all(records)
            session.commit()
            logger.info(f"Inserted {len(records)} raw data records from {file_name}")
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to insert raw data: {e}")
            return False
        finally:
            session.close()

    def insert_aggregated_data(self, metrics: List[Dict], file_name: str, data_source: str) -> bool:
        """Insert aggregated metrics into database"""
        session = self.get_session()
        try:
            records = []
            for metric in metrics:
                record = AggregatedMetrics(
                    location_id=metric.get('location_id', 'all'),
                    data_source=data_source,
                    file_name=file_name,
                    metric_name=metric['metric_name'],
                    min_value=metric.get('min_value'),
                    max_value=metric.get('max_value'),
                    avg_value=metric.get('avg_value'),
                    std_value=metric.get('std_value'),
                    count=metric.get('count')
                )
                records.append(record)

            session.add_all(records)
            session.commit()
            logger.info(f"Inserted {len(records)} aggregated metrics from {file_name}")
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to insert aggregated data: {e}")
            return False
        finally:
            session.close()

    def get_location_statistics(self, location_id: Optional[str] = None) -> pd.DataFrame:
        """Retrieve location statistics"""
        session = self.get_session()
        try:
            query = session.query(AggregatedMetrics)
            if location_id:
                query = query.filter(AggregatedMetrics.location_id == location_id)

            results = query.all()
            data = []
            for result in results:
                data.append({
                    'location_id': result.location_id,
                    'metric_name': result.metric_name,
                    'min_value': result.min_value,
                    'max_value': result.max_value,
                    'avg_value': result.avg_value,
                    'std_value': result.std_value,
                    'count': result.count,
                    'timestamp': result.timestamp
                })

            return pd.DataFrame(data)

        except Exception as e:
            logger.error(f"Failed to retrieve statistics: {e}")
            return pd.DataFrame()
        finally:
            session.close()

    def health_check(self) -> bool:
        """Check database connection health"""
        try:
            from sqlalchemy import text
            session = self.get_session()
            session.execute(text("SELECT 1"))
            session.close()
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False


# Global database manager instance
db_manager = DatabaseManager()
