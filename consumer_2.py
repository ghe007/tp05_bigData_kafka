import json
from kafka import KafkaConsumer

TOPIC = "campus-sensors"
building = "science"
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tp05-campus-monitor-2",
    client_id="consumer-2",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Listening to topic: {TOPIC}")
temps = []
try:
    for message in consumer:
        event = message.value
        if event['building'] != building:
            continue

        temps.append(event["temperature_c"])
        print(f'partition={message.partition} | {event["sensor_id"]} | '
              f'T={event["temperature_c"]}°C | RH={event["humidity_pct"]}%')
except KeyboardInterrupt:
    print("Consumer stopped by user.")
finally:
    if temps:
        print(f"average temperature: {sum(temps)/len(temps):.2f}°C ({len(temps)} messages)")
    consumer.close()