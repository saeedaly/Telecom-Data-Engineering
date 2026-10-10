from confluent_kafka import Consumer
import json
import psycopg2


consumer_config={
    "bootstrap.servers": "localhost:9092",
    "group.id": "charging-cdrs",
    "auto.offset.reset": "earliest" 
}

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "telecom",
    "user": "postgres",
    "password": "postgres",
}

def get_db_connection():
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

def create_table(conn):
    cursor = conn.cursor()
    with open("sql/createTable_charging_cdr.sql","r") as file:
        createSQL = file.read()
    cursor.execute(createSQL)
    conn.commit()
    cursor.close()
    print("Table 'CHARGING_CDRs' is ready.")

def insert_events(conn, event):
    cursor =conn.cursor()
    with open("sql/insert_charging_cdr.sql", "r") as file:
        sql = file.read()
    cursor.execute(sql,
        (event["LOCAL_TIME"],
        event["SERVICE_CLASS_ID"],
        event["SERVIED_MSISDN"],
        event["SERVICE_IDENTIFIER_ID"],
        event["CALLED_NUMBER"],
        event["CALLING_NUMBER"],
        event["DURATION"],
        event["VOLUME"])
        )
    rows_inserted = cursor.rowcount
    conn.commit()
    cursor.close()
    return rows_inserted

conn = get_db_connection()
create_table(conn)

consumer = Consumer(consumer_config)

consumer.subscribe(["charging_cdrs"])

print("🟢 Consusmer is running and subscribed to charging_cdrs topic")

try:
    while True:
        msg=consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ Error", msg.error())
            continue
        value = msg.value().decode("utf-8")
        cdr = json.loads(value)
        print(f"✅ {msg.offset()} Received CDR: {cdr['LOCAL_TIME']} from {cdr['SERVIED_MSISDN']} and it was {cdr['SERVICE_IDENTIFIER_ID']}")
        inserted = insert_events(conn,cdr)
        if inserted == 1:
            print("PostgresSQL: CDR inserted")
        else:
            print("PostgresSQL: duplicated event","ignored")
        consumer.commit(message=msg, asynchronous=False)
        print("Kafka: offset commited")

except KeyboardInterrupt:
    print("\n Stopping Consumer")

finally:
    consumer.close()

