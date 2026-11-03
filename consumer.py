import json
from kafka import KafkaConsumer


TOPIC = "campus-sensors"
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tp05-campus-monitor",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)
print(f"Listening to topic: {TOPIC}")
try:
    tem_total_all_valus = []
    for message in consumer:
        event = message.value
        temperature = event["temperature_c"]
        tem_total_all_valus.append(temperature)
        print(
            f'{event["timestamp"]} | {event["sensor_id"]} | '
            f'T={temperature}°C | RH={event["humidity_pct"]}%'
        )
        if temperature >= 35:
            print("ALERT: High temperature detected!")
    
except KeyboardInterrupt:
    print("Consumer stopped by user.")
finally:
    print(f"average temperature: {sum(tem_total_all_valus)/len(tem_total_all_valus):.2f}°C")
    consumer.close()