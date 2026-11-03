import json
import random
import time 
from datetime import datetime, timezone  
from kafka import KafkaProducer


TOPIC = 'campus-sensors'
buildings = ["library", "technology", "science"]


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

try:
    for event_id in range(1, 101):
        event = {
            'event_id': event_id,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'sensor_id': f"sensor-{random.randint(1, 4):02d}",
            'temperature_c': round(random.uniform(18, 42), 2),
            'humidity_pct': round(random.uniform(25, 85), 2),
            'building': random.choice(buildings)
        }
        producer.send(TOPIC, value=event)
        print(f"Sent: {event}")
        time.sleep(0.5)
        producer.flush()
    
except Exception as e:
    print(f"Error occurred: {e}")
finally:
    producer.close()