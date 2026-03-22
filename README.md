# Stock Market Data Pipeline

An automated system that downloads Indian stock prices every day, stores them in a cloud database, and calculates useful metrics like moving averages and daily returns.

**Tech used:** Python, Apache Airflow, Snowflake, dbt, Docker

---

## What This Does

Every weekday at 6 PM, this pipeline:
1. Downloads stock prices for 10 Indian companies (Reliance, TCS, HDFC Bank, etc.)
2. Saves the data to CSV files
3. Loads it into Snowflake (a cloud database)
4. Calculates metrics like daily returns, moving averages, and volume patterns
5. Runs quality checks to make sure the data is good

The whole thing takes about 2-3 minutes and runs completely on its own.

---

## Architecture

![Architecture](Downloads/Stock_Market_Pipeline_Architecture.jpgx)

The pipeline follows the "medallion architecture" pattern:
 
**Bronze Layer** - Raw data exactly as it comes from the API  
**Silver Layer** - Cleaned data with duplicates removed  
**Gold Layer** - Ready-to-use metrics for analysis

---

## What It Calculates

**Daily Returns**
- How much each stock went up or down
- Categories: Large Gain, Moderate Gain, Flat, Moderate Loss, Large Loss

**Moving Averages**
- 7-day and 30-day averages
- Golden Cross signals (when short-term average crosses above long-term = bullish)
- Death Cross signals (opposite = bearish)

**Volume Analysis**
- Is today's volume higher or lower than usual?
- Which stocks are seeing unusual activity?

---

## How to Run It

### What You Need
- Docker Desktop installed
- A Snowflake account (free trial is fine)

### Quick Start

**1. Get the code**
```bash
git clone https://github.com/YOUR_USERNAME/stock-market-data-pipeline.git
cd stock-market-data-pipeline
```

**2. Set up your database**

Go to Snowflake and run this:
```sql
CREATE DATABASE STOCK_MARKET;
CREATE SCHEMA STOCK_MARKET.RAW;
CREATE SCHEMA STOCK_MARKET.STAGING;
CREATE SCHEMA STOCK_MARKET.MARTS;

CREATE TABLE STOCK_MARKET.RAW.STOCK_PRICES_RAW (
    symbol VARCHAR(50),
    date DATE,
    open DECIMAL(18,2),
    high DECIMAL(18,2),
    low DECIMAL(18,2),
    close DECIMAL(18,2),
    volume BIGINT,
    ingestion_timestamp TIMESTAMP,
    source_system VARCHAR(100),
    pipeline_run_id VARCHAR(100)
);
```

**3. Add your credentials**
```bash
cp .env.example .env
# Edit .env and add your Snowflake username/password
```

**4. Start it**
```bash
cd airflow
docker-compose build    # Takes 5-10 minutes first time
docker-compose up -d    # Starts everything
```

**5. Open the dashboard**

Go to http://localhost:8080

Login with username `airflow` and password `airflow`

Click on the `stock_market_etl_pipeline` and turn it on.

---

## Project Structure
```
stock-market-data-pipeline/
├── airflow/              # Orchestration
│   ├── dags/            # The pipeline code
│   └── Dockerfile       # Custom setup
├── src/                 # Data fetching logic
│   ├── ingestion/       # Gets data from Yahoo Finance
│   └── utils/           # Helper functions
├── stock_dbt/           # Data transformations
│   └── models/          # SQL that creates metrics
└── data/
    └── raw/             # CSV files saved here
```

---

## Example Queries

Once data is loaded, you can run these in Snowflake:

**See recent gainers**
```sql
SELECT symbol, date, daily_return_pct
FROM MARTS.DAILY_RETURNS
WHERE date >= CURRENT_DATE - 7
ORDER BY daily_return_pct DESC
LIMIT 10;
```

**Find golden cross signals**
```sql
SELECT symbol, date, close, ma_signal
FROM MARTS.MOVING_AVERAGES
WHERE ma_signal = 'Golden Cross'
  AND date >= CURRENT_DATE - 30;
```

---

## Things I Learned

**Technical stuff:**
- How to use Airflow to schedule jobs
- Bulk loading data into Snowflake with COPY INTO
- Writing SQL transformations with dbt
- Managing dependencies with Docker

**Problems I solved:**
- NumPy version conflicts (some packages don't work with NumPy 2.x)
- Parsing Snowflake's response format (row count isn't where you'd expect)
- File corruption during writes (fixed with atomic writes)
- Docker volume mounts and Python module caching

---

## Customization

**Change which stocks to track**

Edit `src/config/stocks.py`:
```python
INDIAN_STOCKS = [
    'RELIANCE.NS',
    'TCS.NS',
    # Add more here
]
```

**Use real data instead of mock**

By default it uses fake data for testing. To use real Yahoo Finance data:
```yaml
# In airflow/docker-compose.yml
USE_MOCK_DATA: "False"
```

---

## Troubleshooting

**Port 8080 already in use?**
- Something else is using that port
- Either stop that program or change the port in docker-compose.yml

**Snowflake connection fails?**
- Double-check your credentials in .env
- Make sure you created the database and table

**Pipeline runs but no data?**
- Check the Airflow logs for errors
- Try running with `USE_MOCK_DATA: "True"` first to test

---

## What's Next

Things I might add:
- Real-time data updates (instead of once per day)
- Email alerts when stocks hit certain conditions
- A Power BI dashboard
- Machine learning to predict price movements

---

## Files You Shouldn't Commit

Make sure these are in your `.gitignore`:
- `.env` (has your passwords!)
- `__pycache__/` (Python cache files)
- `logs/` (Airflow logs)
- `data/raw/*.csv` (unless you want to keep examples)

---

## Tech Stack Details

| What | Technology | Why |
|------|-----------|-----|
| Scheduling | Apache Airflow 2.9.0 | Industry standard for data pipelines |
| Database | Snowflake | Cloud data warehouse, separates storage and compute |
| Transformations | dbt 1.8.0 | Version control for SQL, built-in testing |
| Containers | Docker | Same environment everywhere, easy setup |
| Language | Python 3.12 | Good libraries for data work |

**Key packages:**
- pandas 2.2.3 - data manipulation
- numpy 1.26.4 - numerical operations (pinned to 1.x for compatibility)
- snowflake-connector-python 3.12.3 - talks to Snowflake
- yfinance - gets stock data

---

## License

MIT - feel free to use this for learning

---

## Contact

If you have questions or want to connect:
- LinkedIn: www.linkedin.com/in/sasi09
- Email: your.email@example.com
- GitHub: sasidhar-09
