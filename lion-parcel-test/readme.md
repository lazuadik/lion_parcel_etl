# Data Analyst Technical Test - Lion Parcel

This project consists of two main tasks: an **ETL Pipeline** using Apache Airflow and a **Blur Detection AI API** using FastAPI and OpenAI Vision.

## 🚀 Project Overview

1. **ETL Pipeline**: Extracts retail transaction data from PostgreSQL, transforms it into Parquet format, and loads it into Google BigQuery.
2. **AI API**: A FastAPI-based service that detects if an image is blurry using the Laplacian Variance method and provides an AI description for clear images via OpenAI GPT-4o.

## 🛠️ Tech Stack
* **Orchestration**: Apache Airflow
* **Database**: PostgreSQL (Source) & Google BigQuery (Destination)
* **Backend**: FastAPI, Uvicorn
* **Computer Vision**: OpenCV (Laplacian Method)
* **LLM**: OpenAI GPT-4o Vision

## 📋 Prerequisites
* Docker and Docker Compose installed.
* Google Cloud Service Account Key (JSON) for BigQuery access.

## ⚙️ How to Run

1. **Clone the repository**:
   ```bash
   git clone <your-repository-url>
   cd lion-parcel-test