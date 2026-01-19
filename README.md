# 🌦️ Automated Weather Data ETL Pipeline

## 📌 Overview

This project is a simplified **Data Engineering Pipeline** that automates the collection, transformation, and storage of real-time weather data. It demonstrates core DE concepts including **API Integration, Workflow Orchestration, Containerization, and Data Modeling.**

## 🏗️ System Architecture

The pipeline follows a classic ETL (Extract, Transform, Load) pattern:

1.  **Extract**: Fetch real-time weather data from **OpenWeatherMap REST API**.
2.  **Transform**: Process raw JSON data, handle units, and structure it into a relational format using **Python & Pandas**.
3.  **Load**: Ingest the processed data into a **PostgreSQL** database.
4.  **Orchestration**: Managed by **Apache Airflow** to ensure reliability and scheduling.
5.  **Infrastructure**: Entirely containerized using **Docker Compose**.

## 🛠️ Tech Stack

- **Language:** Python 3.9
- **Orchestration:** Apache Airflow
- **Database:** PostgreSQL 13
- **Infrastructure:** Docker, Docker Compose
- **Libraries:** Pandas, SQLAlchemy, Requests

## 🚀 Key Features

- **Automated Scheduling**: Runs every hour to track weather changes.
- **Error Handling & Retries**: Configured Airflow retry logic to handle API timeouts or network issues.
- **Environment Isolation**: One-command setup using Docker, ensuring consistency across different machines.
- **Relational Data Modeling**: Structured data into Fact tables for efficient analytical querying.

## 📊 Data Schema (Fact Weather)

| Column       | Type      | Description                               |
| :----------- | :-------- | :---------------------------------------- |
| city         | String    | Name of the city (Hanoi)                  |
| temp         | Float     | Current temperature in Celsius            |
| humidity     | Integer   | Humidity percentage                       |
| pressure     | Integer   | Atmospheric pressure                      |
| description  | String    | Weather condition (e.g., overcast clouds) |
| collected_at | Timestamp | Time of data ingestion                    |

## ⚙️ Setup & Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/YOUR_USERNAME/weather-etl-pipeline.git
   cd weather-etl-pipeline
   ```
