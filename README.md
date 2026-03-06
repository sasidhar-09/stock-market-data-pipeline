# 📈 Stock Market Data Pipeline

> An end-to-end data engineering pipeline that ingests real-time Indian stock market data,
> transforms it through a Medallion Architecture, and serves analytics-ready data for reporting.

![Status](https://img.shields.io/badge/Status-In%20Progress-yellow)
![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-green)
![dbt](https://img.shields.io/badge/dbt-1.x-orange)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-lightblue)

---

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│   Data Source   │────▶│  Orchestration   │────▶│   Data Warehouse    │
│                 │     │                  │     │                     │
│  yfinance API   │     │ Apache Airflow   │     │     Snowflake        │
│ (NSE/BSE Data)  │     │   (DAG Runs      │     │  ┌───────────────┐  │
│                 │     │    Daily)        │     │  │  RAW Layer    │  │
└─────────────────┘     └──────────────────┘     │  ├───────────────┤  │
                                                  │  │STAGING Layer  │  │
┌─────────────────┐     ┌──────────────────┐     │  ├───────────────┤  │
│  Visualization  │◀────│  Transformation  │◀────│  │  MARTS Layer  │  │
│                 │     │                  │     │  └───────────────┘  │
│    Power BI     │     │      dbt         │     └─────────────────────┘
│   Dashboard     │     │  (SQL Models)    │
└─────────────────┘     └──────────────────┘
```

**Data Flow:**
`yfinance API` → `Python Ingestion Script` → `Airflow DAG` → `Snowflake RAW` → `dbt Transformations` → `Snowflake MARTS` → `Power BI`

---

## 🎯 Project Objective

**Business Problem:** Stock market analysts spend hours manually downloading, cleaning, and preparing data for reporting. This pipeline automates the entire process — from raw data ingestion to analytics-ready output.

**Solution:**
- Automated daily ingestion of Indian stock data (NIFTY 50 stocks)
- Medallion Architecture ensuring data quality at every layer
- Analytics-ready tables for instant Power BI reporting

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python + yfinance | Fetch daily stock data from NSE/BSE |
| Orchestration | Apache Airflow | Schedule and monitor pipeline DAGs |
| Storage | Snowflake | Cloud data warehouse |
| Transformation | dbt | SQL-based data modeling |
| Visualization | Power BI | Business intelligence dashboard |
| Version Control | Git + GitHub | Code management |

---

## 📁 Project Structure

```
stock-market-data-pipeline/
│
├── dags/                          # Airflow DAG definitions
│   └── stock_pipeline_dag.py      # Main pipeline DAG
│
├── src/
│   ├── ingestion/
│   │   └── fetch_stock_data.py    # yfinance data extraction
│   ├── utils/
│   │   └── helpers.py             # Reusable utility functions
│   └── config/
│       └── stocks.py              # Stock symbols config (NIFTY 50)
│
├── dbt/
│   ├── models/
│   │   ├── staging/               # STAGING layer — cleaned data
│   │   │   └── stg_stock_prices.sql
│   │   └── marts/                 # MARTS layer — business logic
│   │       ├── daily_returns.sql
│   │       ├── moving_averages.sql
│   │       └── volume_trends.sql
│   └── dbt_project.yml
│
├── tests/                         # Unit tests
│   └── test_fetch_data.py
│
├── requirements.txt               # Python dependencies
├── docker-compose.yml             # Airflow local setup
└── README.md
```

---

## 📊 Medallion Architecture

| Layer | Location | Description |
|---|---|---|
| **Bronze (RAW)** | `STOCK_DB.RAW` | Raw data as-is from yfinance API — no transformations |
| **Silver (STAGING)** | `STOCK_DB.STAGING` | Cleaned, typed, renamed — null checks applied |
| **Gold (MARTS)** | `STOCK_DB.MARTS` | Aggregated, business-ready — daily returns, moving averages |

---

## 🚀 Getting Started

### Prerequisites
```bash
Python 3.10+
Docker Desktop
Snowflake account (free trial: signup.snowflake.com)
```

### Installation
```bash
# Clone the repository
git clone https://github.com/sasidhar-09/stock-market-data-pipeline.git
cd stock-market-data-pipeline

# Install Python dependencies
pip install -r requirements.txt

# Start Airflow locally
docker-compose up -d
```

### Configuration
```bash
# Set environment variables
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
```

---

## 📈 Sample Data

```python
# Stocks tracked — NIFTY 50 Top 10
STOCKS = [
    "RELIANCE.NS",   # Reliance Industries
    "TCS.NS",        # Tata Consultancy Services
    "HDFCBANK.NS",   # HDFC Bank
    "INFY.NS",       # Infosys
    "ICICIBANK.NS",  # ICICI Bank
    "HINDUNILVR.NS", # Hindustan Unilever
    "ITC.NS",        # ITC Limited
    "SBIN.NS",       # State Bank of India
    "BHARTIARTL.NS", # Bharti Airtel
    "KOTAKBANK.NS",  # Kotak Mahindra Bank
]
```

---

## 🗺️ Roadmap

- [x] Project setup + repository structure
- [ ] Python ingestion script (yfinance)
- [ ] Airflow DAG — daily stock data fetch
- [ ] Snowflake setup — RAW schema + tables
- [ ] dbt STAGING models — data cleaning
- [ ] dbt MARTS models — business aggregations
- [ ] Power BI dashboard
- [ ] Unit tests
- [ ] CI/CD pipeline with GitHub Actions

---

## 🧠 Key Learnings

This project demonstrates:
- End-to-end data pipeline development
- Medallion Architecture implementation (Bronze/Silver/Gold)
- Workflow orchestration with Apache Airflow
- Cloud data warehousing with Snowflake
- SQL-based transformations with dbt
- Data modeling best practices

---

## 👤 Author

**Sasidhar Reddy**
- LinkedIn: [linkedin.com/in/Sasi09](https://linkedin.com/in/Sasi09)
- GitHub: [github.com/sasidhar-09](https://github.com/sasidhar-09)
- Email: sasidhar150rvr@gmail.com

---

## 📝 License

This project is open source and available under the [MIT License](LICENSE).

---

> ⭐ If you found this project useful, please consider starring the repository!
