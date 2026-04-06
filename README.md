# Real-Time Fraud Detection System Using Kafka

## Overview

This project simulates a real-time fraud detection system using streaming financial transaction data.

- Producer generates transaction data.
- Kafka streams data through a topic.
- Consumer analyzes each transaction and flags fraud using multiple rules and a risk score.
- Dashboard visualizes transactions and fraud alerts live in the browser.

## Architecture

Producer -> Kafka topic (`transactions`) -> Consumer (rules + score) -> Kafka topic (`fraud_alerts`) -> Dashboard

## Tech Stack

- Apache Kafka (KRaft mode, via Docker)
- Python 3
- kafka-python
- Flask (live monitoring dashboard)

## Project Files

- `docker-compose.yml`: Runs Kafka in KRaft mode on `localhost:9092`.
- `requirements.txt`: Python dependency list.
- `Makefile`: One-command automation for setup and runtime tasks.
- `create_topic.py`: Creates Kafka topics `transactions` and `fraud_alerts`.
- `producer.py`: Generates and sends transaction events every 2 seconds.
- `consumer.py`: Consumes events and applies advanced fraud detection.
- `dashboard.py`: Streams transaction and alert events to a web dashboard.

## Step-by-Step Working

## Quick Start (Recommended)

```bash
make run
```

Then open 3 terminals:

```bash
make consumer
make producer
make dashboard
```

Open dashboard at `http://localhost:8080`.

## Step-by-Step Working

### Step 1: Start Kafka (KRaft Mode)

From the project root, run:

```bash
docker compose up -d
```

Kafka starts as the streaming broker and listens on `localhost:9092`.

### Step 2: Install Python Dependencies

```bash
python3 -m pip install -r requirements.txt
```

### Step 3: Create Topic

```bash
python3 create_topic.py
```

This creates topics `transactions` and `fraud_alerts` with 3 partitions.

### Step 4: Run Consumer (Fraud Detection Engine)

Open terminal 1:

```bash
python3 consumer.py
```

### Step 5: Run Producer (Transaction Data Generator)

Open terminal 2:

```bash
python3 producer.py
```

The producer sends one transaction every 2 seconds.

### Step 6: Run Dashboard

Open terminal 3:

```bash
python3 dashboard.py
```

Open browser:

```text
http://localhost:8080
```

## Sample Output

Producer:

```text
Sent: {'amount': 75000, 'location': 'Mumbai', ...}
```

Consumer:

```text
Received: {'amount': 75000, 'location': 'Mumbai', ...}
FRAUD ALERT: reasons=['high_amount', 'high_risk_score'], risk_score=55
```

## Fraud Rules

- High amount: `amount > 50000`
- Rapid transaction burst: same user has 3+ transactions within 20 seconds
- Impossible location jump: same user changes location in under 60 seconds
- Risk score model:
	- high amount: +55
	- burst behavior: +30
	- location jump: +35
	- final alert when score >= 70

## Useful Commands

Stop Kafka:

```bash
docker compose down
```

Run producer with custom interval:

```bash
python3 producer.py --interval 1
```

Run consumer from earliest messages:

```bash
python3 consumer.py --offset-reset earliest
```

Run dashboard on a custom port:

```bash
python3 dashboard.py --port 8090
```

One-command helpers:

```bash
make up
make setup
make topic
make consumer
make producer
make dashboard
make down
```

## Why Kafka

- Handles high-throughput streaming data
- Supports real-time processing
- Decouples producer and consumer for scalability

## Conclusion

This system demonstrates how real-time financial transactions can be streamed using Kafka, scored by multiple fraud rules, and visualized instantly in a live dashboard. It can be further enhanced by replacing rule-based scoring with machine learning models.
