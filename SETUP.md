# Setup Guide

This guide will walk you through setting up the Stock Market Data Pipeline from scratch. Follow each step carefully.

---

## Prerequisites

Before starting, you need:

### 1. Docker Desktop
**Why:** Runs all the services (Airflow, Postgres) in containers

**Install:**
- **Windows:** https://docs.docker.com/desktop/install/windows-install/
- **Mac:** https://docs.docker.com/desktop/install/mac-install/
- **Linux:** https://docs.docker.com/desktop/install/linux-install/

**Verify installation:**
```bash
docker --version
docker-compose --version
```

Should show Docker version 20+ and Docker Compose version 2+

### 2. Snowflake Account
**Why:** Cloud database where we store the data

**Get free trial:**
1. Go to https://signup.snowflake.com/
2. Sign up (no credit card needed for trial)
3. Choose any cloud provider (AWS, Azure, GCP)
4. Note your account identifier (looks like `abc12345` or `abc12345.us-east-1`)

### 3. Git (Optional but recommended)
```bash
git --version
```

---

## Step 1: Get the Code

### If you have Git:
```bash
git clone https://github.com/YOUR_USERNAME/stock-market-data-pipeline.git
cd stock-market-data-pipeline
```

### If you don't have Git:
1. Download ZIP from GitHub
2. Extract it
3. Open terminal/command prompt in that folder

---

## Step 2: Set Up Snowflake

### A. Login to Snowflake

1. Go to your Snowflake account URL (you got this when signing up)
2. Login with your username and password

### B. Create Database and Schemas

Click on "Worksheets" in Snowflake, then run these commands one by one:
```sql
-- Create the database
CREATE DATABASE IF NOT EXISTS STOCK_MARKET;

-- Use it
USE DATABASE STOCK_MARKET;

-- Create schemas for each layer
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;
```

**Verify it worked:**
```sql
SHOW SCHEMAS IN DATABASE STOCK_MARKET;
```

You should see RAW, STAGING, MARTS, and some default schemas.

### C. Create Warehouse
```sql
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
```

**What this does:**
- Creates a tiny warehouse (cheapest option)
- Auto-suspends after 5 minutes of inactivity (saves money)
- Auto-resumes when you query

### D. Create the Raw Table
```sql
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS STOCK_PRICES_RAW (
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

**Verify:**
```sql
SELECT * FROM STOCK_PRICES_RAW LIMIT 1;
```

Should return "Query produced no results" (table is empty, which is correct).

---

## Step 3: Configure Environment Variables

### A. Copy the Template

**Windows:**
```bash
copy .env.example .env
```

**Mac/Linux:**
```bash
cp .env.example .env
```

### B. Edit .env File

Open `.env` in any text editor (Notepad, VS Code, etc.)

**Find your Snowflake account identifier:**
1. In Snowflake, click your name (top right)
2. Hover over your account name
3. Copy the account identifier (might look like `abc12345.us-east-1` or just `abc12345`)

**Update these lines:**
```bash
SNOWFLAKE_ACCOUNT=abc12345.us-east-1    # Your account ID
SNOWFLAKE_USER=YOUR_USERNAME            # Your Snowflake username
SNOWFLAKE_PASSWORD=YOUR_PASSWORD        # Your Snowflake password
SNOWFLAKE_DATABASE=STOCK_MARKET         # Leave as-is
SNOWFLAKE_WAREHOUSE=COMPUTE_WH          # Leave as-is
SNOWFLAKE_SCHEMA=RAW                    # Leave as-is
SNOWFLAKE_ROLE=ACCOUNTADMIN             # Leave as-is

USE_MOCK_DATA=True                      # Leave as True for testing
LOG_LEVEL=INFO                          # Leave as-is
```

**Save the file.**

**IMPORTANT:** Never commit this .env file to Git

---

## Step 4: Build Docker Image

This step installs all the Python packages and sets up Airflow.
```bash
cd airflow
docker-compose build
```

**What to expect:**
- Takes 5-10 minutes the first time
- Downloads Python packages (pandas, snowflake, dbt, etc.)
- You'll see lots of text scrolling

**If it succeeds:** You'll see "Successfully built" or "Successfully tagged stock-market-airflow:latest"

**If it fails:** See Troubleshooting section below

---

## Step 5: Start the Services
```bash
docker-compose up -d
```

**What this does:**
- Starts Airflow scheduler (runs your DAGs)
- Starts Airflow webserver (the UI)
- Starts Postgres (stores Airflow metadata)

**Wait 60 seconds** for everything to initialize.

**Check if it's running:**
```bash
docker-compose ps
```

Should show 3 containers running:
- airflow-scheduler
- airflow-webserver
- postgres

---

## Step 6: Access Airflow

### A. Open Airflow UI

1. Open your browser
2. Go to http://localhost:8080
3. Login:
   - Username: `airflow`
   - Password: `airflow`

### B. Enable the DAG

1. You should see `stock_market_etl_pipeline` in the list
2. Toggle the switch on the left to enable it (it turns blue/green)

**Don't trigger it yet!** We need to set up dbt first.

---

## Step 7: Configure dbt

dbt needs to know how to connect to Snowflake.

### A. Enter the Scheduler Container

**Windows:**
```bash
docker-compose exec airflow-scheduler bash
```

**Mac/Linux:**
```bash
docker-compose exec airflow-scheduler bash
```

You're now inside the Docker container (you'll see a different prompt).

### B. Create dbt Directory
```bash
mkdir -p /home/airflow/.dbt
```

### C. Create dbt Profile

**Copy and paste this entire block:**
```bash
cat > /home/airflow/.dbt/profiles.yml << 'EOF'
stock_dbt:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: YOUR_ACCOUNT_ID
      user: YOUR_USERNAME
      password: YOUR_PASSWORD
      role: ACCOUNTADMIN
      database: STOCK_MARKET
      warehouse: COMPUTE_WH
      schema: dbt
      threads: 4
EOF
```

### D. Edit the Profile

**Still in the container**, edit the file:
```bash
nano /home/airflow/.dbt/profiles.yml
```

**Replace:**
- `YOUR_ACCOUNT_ID` with your Snowflake account (e.g., `abc12345.us-east-1`)
- `YOUR_USERNAME` with your Snowflake username
- `YOUR_PASSWORD` with your Snowflake password

**Save and exit:**
- Press `Ctrl + O` (to save)
- Press `Enter` (confirm filename)
- Press `Ctrl + X` (to exit)

### E. Test dbt Connection
```bash
cd /opt/airflow/stock_dbt
dbt debug
```

**Should show:**
```
Connection test: [OK connection ok]
```

If you see errors, double-check your credentials in profiles.yml

### F. Exit Container
```bash
exit
```

You're back in your local terminal.

---

## Step 8: Run the Pipeline

### A. Trigger the DAG

1. Go back to Airflow UI (http://localhost:8080)
2. Click on `stock_market_etl_pipeline`
3. Click the "Play" button (trigger DAG) on the top right
4. Click "Trigger DAG" in the popup

### B. Watch It Run

You'll see the tasks turn:
- **Light green** - Running
- **Dark green** - Success
- **Red** - Failed

**Expected:**
- Extract: ~10 seconds
- Validate: ~5 seconds
- Load: ~15 seconds
- Verify: ~5 seconds
- dbt_transformations: ~30 seconds
- Total: ~2 minutes

### C. Check Each Task

Click on a task box to see:
- Logs (what happened)
- Duration
- Try number

If anything fails, check the logs.

---

## Step 9: Verify Data in Snowflake

Go back to Snowflake and run:
```sql
-- Check raw data
SELECT COUNT(*) FROM RAW.STOCK_PRICES_RAW;
-- Should show 210 rows (10 stocks × 21 business days)

-- Check Silver layer
SELECT COUNT(*) FROM STAGING.STG_STOCK_PRICES;
-- Should show ~351 rows (after cleaning)

-- Check Gold layer
SELECT * FROM MARTS.DAILY_RETURNS LIMIT 10;
SELECT * FROM MARTS.MOVING_AVERAGES LIMIT 10;
SELECT * FROM MARTS.VOLUME_ANALYSIS LIMIT 10;
```

**If you see data:** Success! Everything works!

---

## Step 10: Schedule It (Optional)

The DAG is set to run automatically at 6 PM on weekdays.

If you want to change the schedule:
1. Edit `airflow/dags/stock_market_pipeline.py`
2. Find this line: `schedule_interval='0 18 * * 1-5'`
3. Change it (format: minute hour day month weekday)
4. Restart: `docker-compose restart airflow-scheduler`

**Examples:**
- Every day at 9 AM: `'0 9 * * *'`
- Every hour: `'0 * * * *'`
- Every 30 minutes: `'*/30 * * * *'`

---

## Troubleshooting

### Issue: "docker-compose: command not found"

**Solution:**

Try `docker compose` (without hyphen):
```bash
docker compose up -d
```

Or install Docker Compose separately.

---

### Issue: Port 8080 already in use

**Solution:**

Something else is using port 8080.

**Option 1:** Stop the other program

**Option 2:** Change Airflow's port

Edit `airflow/docker-compose.yml`:
```yaml
ports:
  - "8081:8080"  # Change 8080 to 8081
```

Then access Airflow at http://localhost:8081

---

### Issue: "Connection refused" to Snowflake

**Causes:**
1. Wrong account identifier
2. Wrong username/password
3. Firewall blocking connection

**Solution:**

1. Verify credentials in `.env`
2. Try logging into Snowflake web UI with same credentials
3. Check your account identifier:
   - In Snowflake, hover over your account name (top right)
   - Copy the exact identifier shown

---

### Issue: dbt debug fails

**Error:** `Database Error: Invalid account`

**Solution:**

Your account identifier is wrong. It should be:
- Full format: `abc12345.us-east-1`
- OR short format: `abc12345` (if same region)

Don't include `.snowflakecomputing.com`

---

### Issue: Extract task fails with "ModuleNotFoundError"

**Solution:**

Docker image wasn't built correctly.
```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

---

### Issue: Tasks stuck in "running" state

**Solution:**

Scheduler might be stuck.
```bash
docker-compose restart airflow-scheduler
```

Wait 30 seconds, then check again.

---

### Issue: "NumPy version conflict" errors

**This shouldn't happen** if you built from the Dockerfile, but if it does:
```bash
docker-compose exec airflow-scheduler pip list | grep numpy
```

Should show `numpy 1.26.4`

If it shows `2.x`:
```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

---

## Stopping Everything

### Stop containers (keeps data):
```bash
docker-compose stop
```

### Stop and remove containers (keeps images):
```bash
docker-compose down
```

### Full cleanup (removes everything):
```bash
docker-compose down -v
docker rmi stock-market-airflow:latest
```

---

## Restarting After Stopping
```bash
cd airflow
docker-compose up -d
```

No need to rebuild unless you changed requirements.txt or Dockerfile.

---

## Next Steps

Once everything works:

1. **Switch to real data:**
   - Change `USE_MOCK_DATA: "False"` in docker-compose.yml
   - Restart: `docker-compose restart`

2. **View dbt docs:**
```bash
   docker-compose exec airflow-scheduler bash
   cd /opt/airflow/stock_dbt
   dbt docs generate
   dbt docs serve --port 8081
```
   Then visit http://localhost:8081

3. **Add more stocks:**
   - Edit `src/config/stocks.py`
   - Add stock symbols (must end in .NS for NSE stocks)

4. **Create Power BI dashboard:**
   - Connect Power BI to Snowflake
   - Use tables from MARTS schema

---

## Getting Help

**Check logs:**
```bash
# View all logs
docker-compose logs

# View specific service
docker-compose logs airflow-scheduler

# Follow logs live
docker-compose logs -f airflow-scheduler
```

**Common log locations:**
- Airflow UI: Click on a task → View Log
- Container logs: `docker-compose logs [service-name]`
- Snowflake query history: In Snowflake UI → Activity → Query History

---

## Summary Checklist

Before running the pipeline, make sure:

- [ ] Docker Desktop is running
- [ ] Snowflake database, schemas, and table created
- [ ] `.env` file configured with your credentials
- [ ] Docker image built: `docker-compose build`
- [ ] Services started: `docker-compose up -d`
- [ ] dbt profile configured in container
- [ ] Airflow UI accessible at localhost:8080
- [ ] DAG enabled in Airflow UI

Then trigger the pipeline and verify data in Snowflake!

---

**Still stuck?** Check:
1. All services running: `docker-compose ps`
2. Logs for errors: `docker-compose logs`
3. Can login to Snowflake web UI
4. `.env` file has correct credentials (no quotes around values)