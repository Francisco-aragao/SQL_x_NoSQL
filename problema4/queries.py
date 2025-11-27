import psycopg2
import psycopg2.extras
from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis
import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any
import traceback
import time
import argparse
import random
import concurrent.futures

from abstract_queries import AbstractIoTDb, SensorData

class PostgresDb(AbstractIoTDb):
    def connect(self):
        try:
            self.conn = psycopg2.connect(host="localhost", port="5432", database="trabalho_bd", user="admin", password="admin", cursor_factory=psycopg2.extras.RealDictCursor)
            # print("postgres conectado")
        except Exception as e:
            print(f"erro ao conectar ao Postgres: {e}")

    def close(self):
        if self.conn: 
            self.conn.close()
            # print("postgres desconectado")

    def insert_reading(self, data: SensorData) -> bool:
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO sensors (sensor_id, timestamp, temperature, humidity) VALUES (%s, %s, %s, %s)",
                    (data.sensor_id, data.timestamp, data.temperature, data.humidity)
                )
                self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"Postgres Insert Error: {e}")
            return False

    def get_latest_reading(self, sensor_id: str) -> Dict[str, Any]:
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM sensors WHERE sensor_id = %s ORDER BY timestamp DESC LIMIT 1",
                (sensor_id,)
            )
            return cursor.fetchone()

    def get_readings_by_range(self, sensor_id: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM sensors WHERE sensor_id = %s AND timestamp BETWEEN %s AND %s",
                (sensor_id, start_time, end_time)
            )
            return cursor.fetchall()
            
    def get_all_readings(self, sensor_id: str) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM sensors WHERE sensor_id = %s",
                (sensor_id,)
            )
            return cursor.fetchall()

    def get_average_temperature(self, sensor_id: str, start_time: datetime, end_time: datetime) -> float:
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT AVG(temperature) as avg_temp FROM sensors WHERE sensor_id = %s AND timestamp BETWEEN %s AND %s",
                (sensor_id, start_time, end_time)
            )
            res = cursor.fetchone()
            return res['avg_temp'] if res and res['avg_temp'] else 0.0

class MongoDb(AbstractIoTDb):
    def connect(self):
        try:
            self.client = MongoClient("mongodb://admin:admin@localhost:27017/")
            self.db = self.client["trabalho_bd"]
            # print("mongo conectado")
        except Exception as e:
            print(f"erro ao conectar ao MongoDB: {e}")

    def close(self):
        if self.client:
            self.client.close()
            # print("mongo desconectado")

    def insert_reading(self, data: SensorData) -> bool:
        doc = {
            "sensor_id": data.sensor_id,
            "timestamp": data.timestamp,
            "temperature": data.temperature,
            "humidity": data.humidity
        }
        self.db.sensors.insert_one(doc)
        return True

    def get_latest_reading(self, sensor_id: str) -> Dict[str, Any]:
        return self.db.sensors.find_one(
            {"sensor_id": sensor_id},
            sort=[("timestamp", -1)]
        )

    def get_readings_by_range(self, sensor_id: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        return list(self.db.sensors.find({
            "sensor_id": sensor_id,
            "timestamp": {"$gte": start_time, "$lte": end_time}
        }))
        
    def get_all_readings(self, sensor_id: str) -> List[Dict[str, Any]]:
        return list(self.db.sensors.find({"sensor_id": sensor_id}))

    def get_average_temperature(self, sensor_id: str, start_time: datetime, end_time: datetime) -> float:
        pipeline = [
            {"$match": {
                "sensor_id": sensor_id,
                "timestamp": {"$gte": start_time, "$lte": end_time}
            }},
            {"$group": {
                "_id": None,
                "avg_temp": {"$avg": "$temperature"}
            }}
        ]
        res = list(self.db.sensors.aggregate(pipeline))
        return res[0]['avg_temp'] if res else 0.0

class CassandraDb(AbstractIoTDb):
    def connect(self):
        try:
            self.conn = Cluster(['localhost'], port=9042)
            self.session = self.conn.connect('trabalho_bd')
            # print("cassandra conectado")
        except Exception as e:
            print(f"erro ao conectar ao Cassandra: {e}")

    def close(self):
        if self.conn:
            self.conn.shutdown()
            # print("cassandra desconectado")

    def insert_reading(self, data: SensorData) -> bool:
        if not hasattr(self, 'session') or not self.session: return False
        self.session.execute(
            "INSERT INTO sensors (sensor_id, timestamp, temperature, humidity) VALUES (%s, %s, %s, %s)",
            (data.sensor_id, data.timestamp, data.temperature, data.humidity)
        )
        return True

    def get_latest_reading(self, sensor_id: str) -> Dict[str, Any]:
        if not hasattr(self, 'session') or not self.session: return None
        # Como definimos CLUSTERING ORDER BY (timestamp DESC), o primeiro é o mais recente
        row = self.session.execute(
            "SELECT * FROM sensors WHERE sensor_id = %s LIMIT 1",
            (sensor_id,)
        ).one()
        return row._asdict() if row else None

    def get_readings_by_range(self, sensor_id: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        if not hasattr(self, 'session') or not self.session: return []
        rows = self.session.execute(
            "SELECT * FROM sensors WHERE sensor_id = %s AND timestamp >= %s AND timestamp <= %s",
            (sensor_id, start_time, end_time)
        )
        return [r._asdict() for r in rows]
        
    def get_all_readings(self, sensor_id: str) -> List[Dict[str, Any]]:
        if not hasattr(self, 'session') or not self.session: return []
        rows = self.session.execute(
            "SELECT * FROM sensors WHERE sensor_id = %s",
            (sensor_id,)
        )
        return [r._asdict() for r in rows]

    def get_average_temperature(self, sensor_id: str, start_time: datetime, end_time: datetime) -> float:
        if not hasattr(self, 'session') or not self.session: return 0.0
        # Cassandra suporta AVG em partition key
        row = self.session.execute(
            "SELECT AVG(temperature) as avg_temp FROM sensors WHERE sensor_id = %s AND timestamp >= %s AND timestamp <= %s",
            (sensor_id, start_time, end_time)
        ).one()
        return row.avg_temp if row and row.avg_temp else 0.0

class RedisDb(AbstractIoTDb):
    def connect(self):
        try:
            self.conn = redis.Redis(host='localhost', port=6379, db=0)
            # print("redis conectado")
        except Exception as e:
            print(f"erro ao conectar ao redis: {e}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            # print("redis desconectado")

    def insert_reading(self, data: SensorData) -> bool:
        key = f"sensor:{data.sensor_id}"
        score = data.timestamp.timestamp()
        member = json.dumps({
            "t": data.temperature,
            "h": data.humidity,
            "ts": data.timestamp.isoformat()
        })
        self.conn.zadd(key, {member: score})
        return True

    def get_latest_reading(self, sensor_id: str) -> Dict[str, Any]:
        key = f"sensor:{sensor_id}"
        # Pega o ultimo (maior score)
        res = self.conn.zrange(key, -1, -1)
        if res:
            return json.loads(res[0])
        return None

    def get_readings_by_range(self, sensor_id: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        key = f"sensor:{sensor_id}"
        min_score = start_time.timestamp()
        max_score = end_time.timestamp()
        res = self.conn.zrangebyscore(key, min_score, max_score)
        return [json.loads(r) for r in res]
        
    def get_all_readings(self, sensor_id: str) -> List[Dict[str, Any]]:
        key = f"sensor:{sensor_id}"
        res = self.conn.zrange(key, 0, -1)
        return [json.loads(r) for r in res]

    def get_average_temperature(self, sensor_id: str, start_time: datetime, end_time: datetime) -> float:
        # Client side aggregation
        readings = self.get_readings_by_range(sensor_id, start_time, end_time)
        if not readings: return 0.0
        total = sum(r['t'] for r in readings)
        return total / len(readings)

def worker_thread(db_class, sensor_id, operations_list, start_time, end_time):
    """
    Worker thread that maintains a single connection for multiple operations.
    """
    # Add jitter to prevent thundering herd on connect
    time.sleep(random.uniform(0.0, 1.0))
    
    db = db_class()
    db.connect()
    try:
        for operation in operations_list:
            try:
                if operation == "insert":
                    data = SensorData(sensor_id, datetime.now(), random.uniform(20, 30), random.uniform(40, 80))
                    db.insert_reading(data)
                elif operation == "get_latest":
                    db.get_latest_reading(sensor_id)
                elif operation == "get_range":
                    db.get_readings_by_range(sensor_id, start_time, end_time)
                elif operation == "get_avg":
                    db.get_average_temperature(sensor_id, start_time, end_time)
                elif operation == "get_all":
                    db.get_all_readings(sensor_id)
            except Exception as e:
                print(f"Error in op {operation}: {e}")
    finally:
        db.close()

def run_parallel_benchmark(db_class, concurrency, operations, num_sensors):
    print(f"\n--- Benchmarking {db_class.__name__} (Concurrency: {concurrency}, Ops: {operations}) ---")
    
    start_global = time.perf_counter()
    
    # Definir mix de operações
    ops_list = []
    for _ in range(operations):
        r = random.random()
        if r < 0.2: ops_list.append("insert")
        elif r < 0.4: ops_list.append("get_latest")
        elif r < 0.6: ops_list.append("get_range")
        elif r < 0.8: ops_list.append("get_avg")
        else: ops_list.append("get_all")
        
    NOW = datetime.now()
    START_TIME = NOW - timedelta(minutes=30)
    
    # Split operations among threads
    chunk_size = operations // concurrency
    chunks = [ops_list[i:i + chunk_size] for i in range(0, len(ops_list), chunk_size)]
    
    # Handle remainder
    if len(chunks) > concurrency:
        chunks[-2].extend(chunks[-1])
        chunks.pop()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for chunk in chunks:
            # Randomize sensor for each worker thread
            sensor_id = f"sensor_{random.randint(0, num_sensors - 1)}"
            futures.append(executor.submit(worker_thread, db_class, sensor_id, chunk, START_TIME, NOW))
        
        concurrent.futures.wait(futures)
        
    total_time = time.perf_counter() - start_global
    throughput = operations / total_time
    print(f"Completed in {total_time:.4f}s. Throughput: {throughput:.2f} ops/sec")
    return throughput

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark IoT Queries")
    parser.add_argument("--concurrency", type=int, default=1, help="Number of parallel threads")
    parser.add_argument("--operations", type=int, default=100, help="Total number of operations to perform")
    parser.add_argument("--sensors", type=int, default=10, help="Number of sensors available")
    args = parser.parse_args()

    results = {}
    
    for db_cls in [PostgresDb, MongoDb, CassandraDb, RedisDb]:
        try:
            t = run_parallel_benchmark(db_cls, args.concurrency, args.operations, args.sensors)
            results[db_cls.__name__] = t
        except Exception as e:
            print(f"Failed to benchmark {db_cls.__name__}: {e}")
            traceback.print_exc()

    print("\n--- Final Results (Ops/Sec) ---")
    for k, v in results.items():
        print(f"{k}: {v:.2f}")