from confluent_kafka import Producer
from datetime import datetime
import time
import json
import random


producer_config ={
    'bootstrap.servers': 'localhost:9092'
    }

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivered failed: {err}")
    else:
        print(f"✅ Session #{msg.offset()} sent successfully.")
        # print(f"✅ Session #{msg.offset()} sent {msg.value().decode('utf-8')}")
        # print(f"✅ Session sent to: {msg.topic()} > partition {msg.partition()} > at offset {msg.offset()}")


def generate_charging_cdr():
    """
    Generate one simulated telecom charging CDR.
    Service identifiers:
        0 = Voice
        1 = Data
        2 = SMS
    Duration: seconds
    Volume: KB
    """
    
    # Example service classes (replace with your actual IDs)
    service_class_ids = [000,111,222,333,444]

    timestamp = datetime.now()

    # Select the CDR service type
    service_identifier_id = random.choice([0,1])

    # Generate an Egyptian mobile number
    def generate_msisdn():
        return "2012" + f"{random.randint(0, 99999999):08d}"

    servied_msisdn = generate_msisdn()

    # Default values for fields not applicable to a service
    called_number = None
    calling_number = None
    duration = 0
    volume = 0

    if service_identifier_id == 0: #Voice
        calling_number = servied_msisdn
        called_number = generate_msisdn()
        duration = random.randint(1,1800)
        volume = None
    elif service_identifier_id == 1: #Data
        volume = random.randint(1,500_000)
        duration = None

    cdr = {
        "LOCAL_TIME": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "SERVICE_CLASS_ID": random.choice(service_class_ids),
        "SERVIED_MSISDN": servied_msisdn,
        "SERVICE_IDENTIFIER_ID":service_identifier_id,
        "CALLED_NUMBER": called_number,
        "CALLING_NUMBER": calling_number,
        "DURATION": duration,
        "VOLUME": volume,
    }
    value = json.dumps(cdr).encode("utf-8")
    producer.produce(topic="charging_cdrs", value=value,callback=delivery_report)


try:
    while True:
        generate_charging_cdr()
        producer.poll(0)
        time.sleep(1)

except KeyboardInterrupt:
    print("\n Stopping Producer")        
finally:
    producer.flush()
