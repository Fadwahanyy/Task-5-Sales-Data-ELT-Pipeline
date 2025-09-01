# PostgreSQL Sales ELT Pipeline

##  Overview
This repository provides a **production-ready ELT (Extract–Load–Transform) pipeline** for processing sales data with Python and PostgreSQL.  
The pipeline is fully modular, secure, and idempotent—making it suitable for both learning and real-world data integration projects.

---

##  Key Features
- **Extract** → Cleanly read CSV input with Pandas.  
- **Transform** → Normalize schema, enforce data types, apply defaults, and deduplicate records.  
- **Load** → Create target table if not exists, safely insert only new rows, and maintain primary key integrity.  
- **Database Abstraction** → SQLAlchemy + psycopg2 for robust PostgreSQL integration.  
- **Environment-Driven Config** → `.env` file ensures secure, portable configuration.  
- **Incremental Loads** → Avoids duplicates by checking composite keys before insert.  

---

##  Project Structure
```
Sales-Data-ELT-Pipeline/
│── .env # DB credentials and CSV file path
│── README.md # Documentation
│── Sales_DataSet.csv # Input data
│── screenshots/ # Example run outputs & screenshots
│ ├── Screenshot1.jpg
│ └── Screenshot2.jpg
│── elt/
│ ├── db.py # Database connection setup
│ ├── extract.py # Extract and initial cleanup
│ ├── transform.py # Data transformations & deduplication
│ ├── load.py # Create table & load data
│ └── main.py # Orchestrator script

```
