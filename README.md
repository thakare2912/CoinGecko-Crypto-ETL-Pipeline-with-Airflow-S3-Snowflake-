# CoinGecko Crypto Data Pipeline

This project is an **Apache Airflow pipeline** that extracts cryptocurrency market data from the **CoinGecko API**, stores raw JSON data in **Amazon S3**, transforms it into clean CSV format, uploads the processed CSV back to S3, and then loads it automatically into **Snowflake** using **Snowpipe**.

---

##  Pipeline Steps

**1️) Fetch Data**  
Pulls crypto market data from the CoinGecko public API, handles pagination to fetch all pages.

 **2️) Store Raw Data to S3**  
Uploads the raw JSON response to your S3 bucket in `coin_data/raw_data/`.

 **3️) Read Raw Data from S3**  
Downloads all JSON files from the `raw_data` folder.

 **4️) Transform Data to CSV**  
Parses JSON → extracts useful fields → removes duplicates → converts to CSV.

 **5️) Store Processed CSV to S3**  
Uploads the clean CSV file to `coin_data/coin_process data/` in S3 with a timestamp.

**6️) Load to Snowflake via Snowpipe**  
New CSV files in S3 automatically trigger Snowpipe to ingest the data into your Snowflake table for analytics.

---

##  Technologies Used

- **Apache Airflow**
- **Python**
- **AWS S3**
- **Snowflake**
- **Snowpipe**
- **Pandas**
- **CoinGecko API**

---

