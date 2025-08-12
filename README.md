# 🎵 Last.fm Azure Streaming Data Analysis

## 📌 Project Overview
This project demonstrates an **end-to-end real-time data pipeline** built on Azure for streaming Last.fm user listening data, processing it in Databricks, and creating analytical KPIs stored as Delta tables for visualization.

We use the **Last.fm API** to fetch a user’s live listening history and ingest it into **Azure Event Hubs**. Databricks Structured Streaming consumes the data, cleans and transforms it, calculates KPIs, and stores them in Delta tables for reporting.

---

## 🏗️ Architecture

**Data Flow:**
1. **Last.fm API** → Python ingestion script (`ingest_streaming_data_from_LastFM.py`)
2. **Azure Event Hubs** → Real-time message broker
3. **Databricks Structured Streaming** → Data processing, cleaning, and KPI calculations (`process_streaming_data.py`)
4. **Delta Tables** → Storage layer for analytics
5. **Power BI / Databricks SQL** → Visualization

## 📊 Sample Visual Outputs

1) Top 10 Artists in the Last 1 Hour
<img width="320" height="512" alt="chart" src="https://github.com/user-attachments/assets/c523ebcd-fcc4-45fa-8c1f-27cde7bab26a" />

2) Total Unique Tracks Played
<img width="337" height="123" alt="image" src="https://github.com/user-attachments/assets/588e0875-b504-4381-be96-5bf6dc74be03" />

3) Most Active Users
<img width="320" height="512" alt="chart (1)" src="https://github.com/user-attachments/assets/999a91e0-606e-4a26-a28f-1df7b5f56794" />
