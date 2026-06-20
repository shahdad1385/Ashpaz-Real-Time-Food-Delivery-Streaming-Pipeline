#!/usr/bin/env python3
"""
Kafka Stateless Validator Component
- Consumes raw orders from 'ashpaz.order'
- Validates data formatting, routing conflicts, and mathematical logic
- Routes valid records to 'ashpaz.valid'
- Routes faulty records to 'ashpaz.error_log' with error descriptions
"""

import os
import json
import logging
from confluent_kafka import Consumer, Producer, KafkaError

# Logging Configuration
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")

# Environment Configurations
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
SRC_TOPIC = "ashpaz.order"
VALID_TOPIC = "ashpaz.valid"
ERROR_TOPIC = "ashpaz.error_log"
GROUP_ID = "ashpaz_validator_group"

def validate_order(order: dict) -> list:
    """
    Validates a single order payload against business domain constraints.
    Returns a list of error strings. If list is empty, order is valid.
    """
    errors = []
    
    # Rule 1: Phone Format Validation
    phone = order.get("phone_number", "")
    if not (phone.startswith("+91") or phone.startswith("080")):
        errors.append("INVALID_PHONE")
        
    # Rule 2: Order Mode Conflict Validation
    req_online = order.get("request_online", False)
    req_table = order.get("request_table", False)
    if req_online and req_table:
        errors.append("MODE_CONFLICT")
        
    # Rule 3: Price Calculation Integrity Check
    reported_price = order.get("order_price", 0.0)
    calculated_price = 0.0
    items = order.get("items", [])
    
    for item in items:
        unit_price = item.get("unit_price", 0.0)
        quantity = item.get("quantity", 0)
        calculated_price += unit_price * quantity
        
    # Rounding to 2 decimal places to protect against floating-point drift
    calculated_price = round(calculated_price, 2)
    
    if abs(reported_price - calculated_price) > 0.01:
        errors.append("PRICE_MISMATCH")
        
    return errors

def delivery_report(err, msg):
    """Callback execution hook to log routing validation statuses."""
    if err is not None:
        logging.error(f"Failed to deliver message: {err}")

def main():
    # Initialize Kafka Consumer Configuration
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([SRC_TOPIC])
    
    # Initialize Kafka Producer Configuration
    producer_conf = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(producer_conf)
    
    logging.info(f"Validator started. Listening to stream: '{SRC_TOPIC}'...")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Consumer tracking error: {msg.error()}")
                    break
            
            # Extract JSON payload content
            try:
                order_payload = json.loads(msg.value().decode('utf-8'))
            except Exception as parse_err:
                logging.warning(f"Malformed JSON data dropped. Error: {parse_err}")
                continue
            
            order_id = order_payload.get("order_id", "UNKNOWN_ID")
            
            # Execute Business Rule Engine
            found_errors = validate_order(order_payload)
            
            if not found_errors:
                # Payload is sound -> Push to Valid Queue
                producer.produce(
                    VALID_TOPIC,
                    key=order_id,
                    value=json.dumps(order_payload),
                    callback=delivery_report
                )
                logging.info(f"Processed successfully: Order ID {order_id} -> '{VALID_TOPIC}'")
            else:
                # Payload is corrupt -> Modify data structures and push to error log
                error_payload = {
                    "original_order": order_payload,
                    "errors": found_errors,
                    "error_count_type": "MULTI" if len(found_errors) > 1 else found_errors[0]
                }
                producer.produce(
                    ERROR_TOPIC,
                    key=order_id,
                    value=json.dumps(error_payload),
                    callback=delivery_report
                )
                logging.warning(f"Validation Failure: Order ID {order_id} via rules {found_errors} -> '{ERROR_TOPIC}'")
                
            # Flush pipeline to keep memory footprint lean
            producer.poll(0)
            
    except KeyboardInterrupt:
        logging.info("Shutting down validator processes gracefully...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()