from kafka import KafkaConsumer
import psycopg2
import json

class CryptoDataConsumer:
    def __init__(self, bootstrap_servers, kafka_topic, db_connection_string):
        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='crypto_consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.db_connection_string = db_connection_string

    def init_db(self):
        conn = psycopg2.connect(self.db_connection_string)
        cursor = conn.cursor()

        # Create a table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crypto_data (
                id VARCHAR PRIMARY KEY,
                symbol VARCHAR,
                name VARCHAR,
                price NUMERIC,
                last_updated VARCHAR,
                commits INTEGER
            )
        ''')

        conn.commit()
        conn.close()

    def consume_and_store_data(self):
        self.init_db()

        for message in self.consumer:
            crypto_data = message.value
            self.store_data_in_db(crypto_data)
            print(f"Inserted data into PostgreSQL: {crypto_data['id']}")
            
    def store_data_in_db(self, data):
        conn = psycopg2.connect(self.db_connection_string)
        cursor = conn.cursor()

        # Check if record with the same id already exists
        cursor.execute("SELECT 1 FROM crypto_data WHERE id = %s", (data['id'],))
        existing_record = cursor.fetchone()

        if existing_record:
            # Update the existing record
            cursor.execute('''
                UPDATE crypto_data
                SET symbol = %s, name = %s, price = %s, last_updated = %s, commits = %s
                WHERE id = %s
            ''', (data['symbol'], data['name'], data['price'], data['last_updated'], data['commits'], data['id']))
        else:
            # Insert a new record
            cursor.execute('''
                INSERT INTO crypto_data (id, symbol, name, price, last_updated, commits)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (data['id'], data['symbol'], data['name'], data['price'], data['last_updated'], data['commits']))

        conn.commit()
        conn.close()

if __name__ == "__main__":
    # Define your Kafka and PostgreSQL configurations
    bootstrap_servers = 'localhost:9092'
    kafka_topic = 'selected_crypto_data'
    db_connection_string = 'postgresql://postgres:postgres@localhost:5432/crypto_db'

    # Create an instance of CryptoDataConsumer
    crypto_consumer = CryptoDataConsumer(bootstrap_servers, kafka_topic, db_connection_string)

    # Start consuming and storing data
    crypto_consumer.consume_and_store_data()