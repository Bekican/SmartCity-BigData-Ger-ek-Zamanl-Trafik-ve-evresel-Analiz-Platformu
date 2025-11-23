import time
import random
import simplejson as json
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

conf = {
    'bootstrap.servers': 'localhost:29092',
    'client.id': 'traffic-sensor-01',
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Mesaj iletilemedi: {err}')
    else:
    
        print(f'GÖNDERİLDİ -> Topic: {msg.topic()} | Partition: {msg.partition()}')

def generate_traffic_data():
    lat = 41.0082 + random.uniform(-0.05, 0.05)
    lon = 28.9784 + random.uniform(-0.05, 0.05)
    

    data = {
        "sensor_id": str(random.randint(1000, 9999)),
        "timestamp": float(time.time()),            
        "lat": lat,                                 
        "lon": lon,                                
        "vehicle_count": random.randint(0, 50), 
        "avg_speed": float(random.randint(5, 120)),  
        "traffic_intensity": random.choice(["LOW", "MEDIUM", "HIGH", "JAM"]),
        "city": "Istanbul"
    }
    return data

print("Producer çalışıyor. Veri pompalanıyor. (Durdurmak için CTRL+C)")

try:
    while True:
        traffic_data = generate_traffic_data()
        message_value = json.dumps(traffic_data)

      
        producer.produce(
            'traffic-data', 
            key=traffic_data["sensor_id"],
            value=message_value, 
            callback=delivery_report
        )
        
        producer.poll(0)
        time.sleep(1) 

except KeyboardInterrupt:
    print("Veri akışı durduruldu.")
finally:
    producer.flush()