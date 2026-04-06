PYTHON ?= python3
PIP ?= $(PYTHON) -m pip

BOOTSTRAP_SERVER ?= localhost:9092
TOPIC ?= transactions
ALERTS_TOPIC ?= fraud_alerts

.PHONY: setup up down topic producer consumer dashboard run

setup:
	$(PIP) install -r requirements.txt

up:
	docker compose up -d

down:
	docker compose down

topic:
	$(PYTHON) create_topic.py --bootstrap-server $(BOOTSTRAP_SERVER) --topic $(TOPIC) --alerts-topic $(ALERTS_TOPIC)

producer:
	$(PYTHON) producer.py --bootstrap-server $(BOOTSTRAP_SERVER) --topic $(TOPIC)

consumer:
	$(PYTHON) consumer.py --bootstrap-server $(BOOTSTRAP_SERVER) --topic $(TOPIC) --alerts-topic $(ALERTS_TOPIC)

dashboard:
	$(PYTHON) dashboard.py --bootstrap-server $(BOOTSTRAP_SERVER) --topic $(TOPIC) --alerts-topic $(ALERTS_TOPIC)

run: up setup topic
	@echo "Start in separate terminals: make consumer | make producer | make dashboard"
