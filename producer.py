import time
import random
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_sensor_data(sensor_id):
    """Simulate temperature data between 60°C and 120°C."""
    temperature = random.randint(60, 120)
    return {'sensor': sensor_id, 'temp': temperature}

sensor_ids = ['S1', 'S2', 'S3']

if __name__ == "__main__":
    try:
        while True:
            for sensor_id in sensor_ids:
                data = generate_sensor_data(sensor_id)
                producer.send('sensor-suhu', data)
                print(f"sensor: {data['sensor']}, temp: {data['temp']}°C")
            time.sleep(1) 
    except KeyboardInterrupt:
        producer.close()
        print("Producer has been stopped.")
