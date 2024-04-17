# AdaptiveAds - A Dynamic Ad Insertion System for OTT Platforms

A real-time Dynamic Ad Insertion data pipeline designed for OTT services, which intelligently inserts ads based on user behaviors and content context, maximizing ad relevance and user satisfaction. This pipeline uses a combination of Kafka, Spark Streaming, and machine learning, orchestrated with Airflow and managed via Terraform on GCP.

## Objective

The project will stream events generated from a simulated OTT platform (similar to Netflix) using EventSim, and create a sophisticated data pipeline that consumes this real-time data. The incoming data will simulate user interactions like watching videos, pausing content, and reacting to inserted ads. This data will be processed in real-time and stored to a data lake at regular intervals (every two minutes). An hourly batch job will then consume this data, apply transformations using dbt, and create the necessary tables for our dashboard powered by Looker to generate advanced analytics. We will aim to analyze metrics such as ad engagement rates, user viewing patterns, and the effectiveness of different ad formats. The project will utilize Kafka for data ingestion, Spark Streaming for real-time processing, and machine learning to dynamically insert relevant ads, all orchestrated by Airflow and managed via Docker and Terraform on GCP.

## Motivation

The motivation behind this project is to enhance user engagement and increase advertising revenue on OTT platforms by providing timely and relevant advertisements without disrupting the viewing experience. Traditional ad insertion strategies often lead to poor user experience due to irrelevant or poorly timed ads. By using real-time data processing and machine learning, this project aims to solve these issues, enhancing both viewer satisfaction and ad effectiveness.


## Features

What makes this project stand out?
- **Real-time Data Ingestion**: Using Kafka to handle live streaming data of user interactions.
- **Dynamic Ad Matching**: Utilizing Spark Streaming and machine learning algorithms to insert ads that match user preferences and context.
- **Scalable Infrastructure**: Provisioned and managed with Terraform on GCP.
- **Visualization**: Using Looker to create dashboards for real-time analytics of ad performance and user engagement.

## Architecture

The system architecture comprises several components, each responsible for a part of the data processing pipeline:

- **EventSim**: Simulates user data.
- **Kafka**: Manages the ingestion and streaming of real-time data.
- **Spark Streaming**: Processes data in real-time to make ad insertion decisions.
- **Ad Server**: Handles the logistics of serving ads to the user.
- **Airflow**: Orchestrates the overall data pipeline and scheduled tasks.
- **Terraform + GCP**: Provides the infrastructure.

### Built with
- [Apache Kafka](https://kafka.apache.org/)
- [Apache Spark](https://spark.apache.org/)
- [Apache Airflow](https://airflow.apache.org/)
- [Docker](https://www.docker.com/)
- [Terraform](https://www.terraform.io/)
- [Google Cloud Platform (GCP)](https://cloud.google.com/)
- [Looker](https://looker.com/)

![System Architecture](link-to-architecture-diagram.png)  <!-- Link to your architecture diagram -->

## Prerequisites

Before you begin, ensure you have the following:
- Access to a Google Cloud account with billing enabled.
- Local installations of Terraform, Docker, and the Google Cloud SDK.
- Basic knowledge of Kafka, Spark, Airflow, and Looker.

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/chrisdamba/adaptive-ads.git
cd dynamic-ad-insertion-ott
```

### 2. Set Up GCP Environment

- Configure your GCP credentials:
  ```bash
  gcloud auth login
  gcloud config set project your-project-id
  ```

- Initialize and apply Terraform configuration:
  ```bash
  terraform init
  terraform apply
  ```

### 3. Start Local Development Environment

- Build and run Docker containers:
  ```bash
  docker-compose up --build
  ```

### 4. Accessing the Web Interface

- **Airflow**: Navigate to `http://localhost:8080` to access the Airflow dashboard.
- **Looker**: Set up Looker to connect to your BigQuery datasets for visualizing data.

## Usage



