# Bosch-internship: Real-Time Data Processing Pipeline Project Structure
## Steps to create a real-time data processing pipeline project structure (brainstorming)

1. Set a project directory structure for a real-time data processing pipeline.
2. Include directories for data, quarantine, processed files, logs, source code, tests, and configuration.
3. Create data_connector to ingest raw data from kaggle and store it in data directory


## Real-Time Data Processing Pipeline Project Structure
real-time-pipeline/<br>
├── data/               # Monitored raw data folder<br>
├── quarantine/         # Invalid files<br>
├── processed/          # Archive processed files<br>
├── logs/               # Error and process logs<br>
├── src/<br>
│   ├── monitor.py      # File monitoring<br>
│   ├── validator.py    # Data validation<br>
│   ├── transformer.py  # Data transformation<br>
│   ├── analyzer.py     # Statistical analysis<br>
│   ├── database.py     # Database operations<br>
│   └── config.py       # Configuration<br>
├── tests/<br>
├── requirements.txt<br>
└── README.md