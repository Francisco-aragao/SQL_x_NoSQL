
import time
import psycopg2
import psycopg2.extras
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import redis
import sys
import os
import argparse
from tqdm import tqdm
from datetime import datetime, timedelta
import random

def connect_postgres():
    print("conectando postgres...")
    return psycopg2.connect(host="localhost", port="5432", database="trabalho_bd", user="admin", password="admin")

def connect_mongo():
    print("conectando mongo...")
    client = MongoClient("mongodb://admin:admin@localhost:27017/")
    return client

def connect_cassandra():
    print("conectando cassandra...")
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()
    session.set_keyspace('trabalho_bd')
    return cluster, session

def connect_redis():
    print("conectando redis...")
    r = redis.Redis(host='localhost', port=6379, db=0)
    return r

def generate_data(num_sensors, entries_per_sensor):
    print(f"gerando dados para {num_sensors} sensores com {entries_per_sensor} entradas cada...")
    data = []
    base_time = datetime.now()
    
    for i in range(num_sensors):
        sensor_id = f"sensor_{i}"
        for j in range(entries_per_sensor):
            # timestamps espaçados de 1 minuto
            ts = base_time - timedelta(minutes=j)
            temp = round(random.uniform(20.0, 30.0), 2)
            hum = round(random.uniform(40.0, 80.0), 2)
            data.append({
                "sensor_id": sensor_id,
                "timestamp": ts,
                "temperature": temp,
                "humidity": hum
            })
    
    print(f"total de registros gerados: {len(data)}")
    return data

def load_into_postgres(cursor, data):
    print("\ncarregando dados postgres...")
    
    pg_data = []
    for row in data:
        pg_data.append((
            row['sensor_id'], 
            row['timestamp'], 
            row['temperature'], 
            row['humidity']
        ))
    
    query = """
    INSERT INTO sensors (sensor_id, timestamp, temperature, humidity) 
    VALUES %s ON CONFLICT DO NOTHING
    """
    # batch size de 1000 para não estourar memória do driver
    batch_size = 1000
    for i in tqdm(range(0, len(pg_data), batch_size), desc="Postgres Load"):
        batch = pg_data[i:i+batch_size]
        psycopg2.extras.execute_values(cursor, query, batch)
    
    cursor.connection.commit()
    print("postgres-> dados carregados.")

def load_into_mongo(db, data):
    print("carregando dados mongo...")
    
    docs = []
    for row in data:
        docs.append(row)
    
    if docs:
        batch_size = 5000
        for i in tqdm(range(0, len(docs), batch_size), desc="Mongo Load"):
            db["sensors"].insert_many(docs[i:i+batch_size])
            
    print("mongo-> dados carregados.")

def load_into_cassandra(session, data):
    print("Carregando dados no Cassandra...")

    insert_stmt = session.prepare("""
        INSERT INTO sensors (sensor_id, timestamp, temperature, humidity)
        VALUES (?, ?, ?, ?)
    """)
    
    batch_size = 500
    batch = BatchStatement()
    count = 0
    
    for row in tqdm(data, desc="Cassandra Load"):
        batch.add(insert_stmt, (
            row['sensor_id'], 
            row['timestamp'], 
            row['temperature'], 
            row['humidity']
        ))
        
        count += 1
        
        # Se atingir o tamanho do batch, execute e resete
        if count >= batch_size:
            session.execute(batch)
            batch = BatchStatement()  # Reseta o batch
            count = 0
    
    # Executar o último batch, caso haja dados restantes
    if count > 0:
        session.execute(batch)

    print("Cassandra -> Dados carregados.")

def load_into_redis(conn, data):
    print("carregando dados redis...")
    
    pipe = conn.pipeline()
    count = 0
    
    for row in tqdm(data, desc="Redis Load"):
        key = f"sensor:{row['sensor_id']}"
        score = row['timestamp'].timestamp()
        # Armazenamos como JSON string no membro do Sorted Set
        # Formato compacto: "temp,hum" ou json
        import json
        member = json.dumps({
            "t": row['temperature'],
            "h": row['humidity'],
            "ts": row['timestamp'].isoformat() # redundante mas util
        })
        
        pipe.zadd(key, {member: score})
        
        # Adiciona ao set de sensores para saber quais existem
        pipe.sadd("sensors:all", row['sensor_id'])
        
        count += 1
        if count % 1000 == 0:
            pipe.execute()
            pipe = conn.pipeline()
            
    pipe.execute()
    print("redis-> dados carregados.")

def main(args):
    pg_conn = None
    mongo_client = None
    cassandra_cluster = None
    redis_conn = None

    try:
        # Gerar dados
        data = generate_data(args.sensors, args.entries)
        
        # Conectar e Inserir
        pg_conn = connect_postgres()
        load_into_postgres(pg_conn.cursor(), data)
        
        mongo_client = connect_mongo()
        load_into_mongo(mongo_client["trabalho_bd"], data)
        
        cassandra_cluster, cassandra_session = connect_cassandra()
        load_into_cassandra(cassandra_session, data)
        
        redis_conn = connect_redis()
        load_into_redis(redis_conn, data)
        
    except Exception as e:
        print(f"Erro: {e}")
        import traceback
        traceback.print_exc()
            
    finally:
        print("fechando conexoes")
        if pg_conn: pg_conn.close()
        if mongo_client: mongo_client.close()
        if cassandra_cluster: cassandra_cluster.shutdown()
        if redis_conn: redis_conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Popula dados de IoT")
    parser.add_argument("--sensors", type=int, default=10, help="Número de sensores")
    parser.add_argument("--entries", type=int, default=100, help="Entradas por sensor")
    args = parser.parse_args()
    
    main(args)