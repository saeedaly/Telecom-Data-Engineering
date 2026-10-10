# Telecom Charging CDR (Call Details Recored) Streaming Pipeline

A lightweight Apache Kafka project that simulates real-time Telecom Charging CDR using a Python producer and consumer. It mimics telecom systems that generate cdrs such as calls/data.

## Overview

This project contains two independent microservices:

- `producer.py`: generates realistic charging cdrs and publishes them to a Kafka topic
- `consumer.py`: consumes those cdrs and prints them in real time

The producer simulates a telecom network producing events like:
- mobile numbers
- service type such as `VOICE/DATA/SMS`
- timestamps

## Features

- Realistic event simulation using Egyptian MSISDN examples
- Real-style CDRs
- Weighted status handling:
  - `Call`: 30%
  - `Data`: 55%
  - `SMS`: 15%
- JSON-based payloads for easy downstream integration
- Simple local Kafka testing setup using Docker

## Tech Stack

- Python 3.8+
- Confluent Kafka
- Apache Kafka
- Docker

## Prerequisites

Before starting, make sure you have:

- Python 3.8 or newer
- Docker installed and running

## Installation

1. Clone the repository.
2. Install the Kafka client:
   ```bash
   pip install confluent-kafka
   ```
3. Start Kafka locally with Docker:
   ```bash
   docker compose up -d
   ```

## Usage

Start the consumer first:

```bash
python consumer.py
```

Then start the producer in a second terminal:

```bash
python producer.py
```

## Example Output

### Producer
```json
{"LOCAL_TIME":"2025-01-01 14:30:12","SERVICE_CLASS_ID":"1234","SERVIED_MSISDN":"201201234567","SERVICE_IDENTIFIER_ID":"1","CALLED_NUMBER":"01022345468","CALLING_NUMBER":"201201234567","DURATION":"105","VOLUME":"NULL"}
```

### Consumer
```text
🟢 709 Received CDR: 2026-10-10 14:52:43 from 201214879353
```

## Project Structure

```text
.
├── producer.py
├── consumer.py
├── docker-compose.yml
└── sql/
    ├── createTable_charging_cdr.sql
    └── insert_charging_cdr.sql
├── README.md
└── requirements.txt
```

