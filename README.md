📌 Overview

This project simulates a real-time fraud detection system using streaming financial transaction data.
We use Apache Kafka as a messaging system, where:

Producer → generates transaction data
Kafka → streams data
Consumer → analyzes and detects fraud
⚙️ Step-by-Step Working
🟢 STEP 1: Start Kafka (KRaft Mode)
First, we start the Kafka server using KRaft mode.
Kafka acts as a broker that handles real-time data streaming.
It listens on localhost:9092.

👉 In simple terms:
Kafka is the middle system that connects producer and consumer.

🟢 STEP 2: Create Topic
We create a topic named transactions.
A topic is like a channel where data is sent and received.

👉 Analogy:
Topic = WhatsApp group
Producer sends messages → Consumer reads messages

🟢 STEP 3: Install Kafka Python Library
We install kafka-python to interact with Kafka using Python.
This allows us to build producer and consumer scripts.

🟢 STEP 4: Producer (Data Generator)
The producer simulates financial transactions.
It generates random data like:
transaction_id
user_id
amount
location
Every 2 seconds, it sends data to Kafka topic.

👉 Example:

Sent: {'amount': 75000, 'location': 'Mumbai'}

👉 Concept:
Producer = Data generator / sender

🟢 STEP 5: Consumer (Fraud Detection Engine)
The consumer reads data from the transactions topic.
It processes each transaction in real-time.
🚨 Fraud Logic:
If amount > 50000 → Fraud Alert

👉 Example output:

Received: {'amount': 75000}
🚨 FRAUD ALERT: High Transaction Amount Detected!

👉 Concept:
Consumer = Analyzer / decision maker

🔄 Complete Data Flow
Producer → Kafka Topic → Consumer → Fraud Detection
Producer generates transaction
Kafka stores & streams it
Consumer reads instantly
Fraud rule is applied
⚡ Key Features
Real-time streaming
Scalable system
Decoupled architecture (producer & consumer independent)
Can be extended with ML models
🧠 Why Kafka is Used
Handles high-volume streaming data
Works in real-time
Reliable and scalable
🚀 Conclusion 

“This system demonstrates how real-time financial transactions can be streamed using Kafka and analyzed instantly to detect fraud, and it can be further enhanced using machine learning for more advanced fraud detection.”
