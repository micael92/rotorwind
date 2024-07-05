import time
import random
import json
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

print("Starting producer...")

# Konfiguriere den Producer
conf = {
    'bootstrap.servers': 'broker:29092',
    'schema.registry.url': 'http://schema-registry:8081',
    'client.id': 'sensor-data-producer',
    'acks': 'all'
}

print("Configuring producer...")

# Lade das neue AVRO-Schema
value_schema = avro.load('extended_sensor_data_2.avsc')

producer = AvroProducer(conf, default_value_schema=value_schema)
print("Producer configured.")

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def generate_machine_data(machine_id):
    temperatures = [round(random.uniform(25.0, 30.0), 2) for _ in range(5)]
    utilization = round(random.uniform(60.0, 80.0), 2)
    return {
        "machine_id": machine_id,
        "temperature_1": temperatures[0],
        "temperature_2": temperatures[1],
        "temperature_3": temperatures[2],
        "temperature_4": temperatures[3],
        "temperature_5": temperatures[4],
        "utilization": utilization
    }

def generate_sensor_data():
    return [generate_machine_data(machine_id) for machine_id in range(1, 6)]

new_topic = 'adjusted_temperature_data'

try:
    while True:
        sensor_data = generate_sensor_data()
        for data in sensor_data:
            print(f"Generated sensor data: {data}")
            producer.produce(topic=new_topic, value=data, callback=delivery_report)
        producer.poll(0)
        time.sleep(1)  # Warte 1 Sekunde bis zur nächsten Messung
except KeyboardInterrupt:
    pass
finally:
    producer.flush()

print("Temperature data generation and transmission ended.")
