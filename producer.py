import time
import random
import json
from kafka import KafkaProducer

# Initialize connection to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_sensor_data(sensor_id):
    """Simulate temperature data between 60°C and 100°C."""
    temperature = random.randint(60, 100)  # Generate random temperature
    return {'sensor': sensor_id, 'temp': temperature}

sensor_ids = ['S1', 'S2', 'S3']  # List of sensor IDs

if __name__ == "__main__":
    try:
        while True:
            for sensor_id in sensor_ids:
                data = generate_sensor_data(sensor_id)
                producer.send('sensor-suhu', data)  # Send data to Kafka
                print(f"sensor: {data['sensor']}, temp: {data['temp']}°C")  # Display sent data
            time.sleep(1)  # Wait before sending the next batch
    except KeyboardInterrupt:
        producer.close()  # Close the producer on interruption
        print("Producer has been stopped.")
