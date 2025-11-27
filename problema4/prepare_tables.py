import psycopg2
from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis

def create_postgres():
    print("postgres criando tabelas")
    commands = [
        "DROP TABLE IF EXISTS sensors;",
        
        """
        CREATE TABLE sensors (
            sensor_id       VARCHAR(50),
            timestamp       TIMESTAMP,
            temperature     REAL,
            humidity        REAL,
            PRIMARY KEY (sensor_id, timestamp)
        );
        """
    ]
    
    try:
        conn = psycopg2.connect(host="localhost", port="5432", database="trabalho_bd", user="admin", password="admin")
        cursor = conn.cursor()
        for command in commands:
            cursor.execute(command)
        conn.commit()
        cursor.close()
        conn.close()
        print("tabela postgres recriada com sucesso.")
    except Exception as e:
        print(f"erro --> PostgreSQL: {e}")

def create_cassandra():
    print("cassandra -> criando tabelas ")
    try:
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect()

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS trabalho_bd 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)
        
        session.set_keyspace('trabalho_bd')

        queries = [
            "DROP TABLE IF EXISTS sensors;",
            """
            CREATE TABLE sensors (
                sensor_id TEXT,
                timestamp TIMESTAMP,
                temperature FLOAT,
                humidity FLOAT,
                PRIMARY KEY (sensor_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC);
            """
        ]
        for q in queries:
            session.execute(q)

        cluster.shutdown()
        print("cassandra tabelas recriadas com sucesso.")
    except Exception as e:
        print(f"erro --> Cassandra: {e}")

def prepare_mongo():
    print("mongo criando tabelas")
    try:
        client = MongoClient("mongodb://admin:admin@localhost:27017/")
        db = client["trabalho_bd"]
        
        # Drop collection if exists
        db["sensors"].drop()
        
        # Create index for efficient querying by sensor and time
        db["sensors"].create_index([("sensor_id", 1), ("timestamp", -1)])
        
        print("mongo pronto.")
    except Exception as e:
        print(f"erro --> MongoDB: {e}")

def prepare_redis():
    print("redis criando")
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.flushdb() 
        print("redis banco limpo.")
    except Exception as e:
        print(f"erro --> Redis: {e}")

def main():
    print("iniciando criação das estruturas do Problema 4 (IoT)...\n")
    create_postgres()
    create_cassandra()
    prepare_mongo()
    prepare_redis()
    print("\nfim!")

if __name__ == "__main__":
    main()