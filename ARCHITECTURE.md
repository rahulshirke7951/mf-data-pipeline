# 🏗️ Architecture Overview

This document describes the high-level architecture of the **MF Data Pipeline**, including data flow, components, and system design.

---

## 🔷 High-Level Architecture

```mermaid
flowchart TD
    A[Data Sources / APIs] --> B[Ingestion Layer]
    B --> C[Raw Data Storage]
    C --> D[Processing & Transformation]
    D --> E[Processed Data Storage]
    E --> F[Analytics / Consumption]
    D --> G[Logging & Monitoring]
```

---

## 🔑 Components

### 1. Data Sources
- External APIs
- Financial datasets
- Scheduled fetch jobs

### 2. Ingestion Layer (`scripts/`)
- Fetches raw mutual fund data
- Handles retries and failures
- Can be triggered manually or scheduled

### 3. Raw Data Storage (`data/raw/`)
- Stores unprocessed data
- Used for auditing and reprocessing

### 4. Processing Layer (`src/`)
- Cleans and validates data
- Transforms into structured format
- Handles business logic

### 5. Processed Storage (`data/processed/`)
- Clean datasets ready for analysis
- Optimized for querying and usage

### 6. Analytics / Consumption
- Reporting tools
- Dashboards (future scope)
- Data exports

### 7. Logging & Monitoring (`logs/`)
- Tracks pipeline runs
- Error logging
- Debugging support

---

## 🔄 Detailed Data Flow

```mermaid
sequenceDiagram
    participant Scheduler
    participant Ingestion
    participant StorageRaw
    participant Processor
    participant StorageProcessed

    Scheduler->>Ingestion: Trigger job
    Ingestion->>StorageRaw: Save raw data
    StorageRaw->>Processor: Load raw data
    Processor->>Processor: Clean & transform
    Processor->>StorageProcessed: Save processed data
```

---

## ⏰ Scheduling Architecture

```mermaid
flowchart LR
    A[Cron / GitHub Actions] --> B[Pipeline Trigger]
    B --> C[Run Ingestion]
    C --> D[Run Processing]
    D --> E[Store Results]
    E --> F[Logs Generated]
```

---

## 🧹 Maintenance Architecture (Releases Cleanup)

```mermaid
flowchart TD
    A[Scheduler] --> B[Cleanup Script]
    B --> C[Fetch Releases]
    C --> D{Filter Criteria}
    D -->|Older than X days| E[Delete]
    D -->|Keep Latest N| F[Skip]
    D -->|Dry Run| G[Log Only]
```

---

## 📦 Tech Stack (Typical)

- Python
- Pandas / NumPy
- REST APIs
- Cron / GitHub Actions
- File-based or DB storage

---

## 🚀 Future Improvements

- Real-time streaming (Kafka / PubSub)
- Data validation layer
- Alerting system (Slack / Email)
- Dashboard integration
- Scalable storage (Data Warehouse)

---

> This architecture is designed to be modular, scalable, and easy to extend.
