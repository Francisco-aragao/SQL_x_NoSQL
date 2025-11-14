import psycopg2
from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis
import sys

def check_postgres():
    print("teste postgres")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="trabalho_bd",
            user="admin",
            password="admin"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        print(f"conectado")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"erro --> PostgreSQL: {e}")
    print("-" * 30)

def check_mongo():
    print("teste mongo")
    try:
        client = MongoClient("mongodb://admin:admin@localhost:27017/")
        client.admin.command('ping')
        print("conectado")
    except Exception as e:
        print(f"erro --> MongoDB: {e}")
    print("-" * 30)

def check_cassandra():
    print("teste cassandra")
    try:
        cluster = Cluster(['localhost'], port=9042)
        print(f"conectado")
        cluster.shutdown()
    except Exception as e:
        print(f"erro --> Cassandra: {e}")
        print("   (Dica: O Cassandra demora 1-2 min para subir. Tente de novo em breve.)")
    print("-" * 30)

def check_redis():
    print("teste redis")
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        response = r.ping()
        if response:
            print("conectado")
        else:
            print("erro --> Redis: Sem resposta do PING.")
    except Exception as e:
        print(f"erro --> Redis: {e}")
    print("-" * 30)

def main():
    print("init health checks")
    
    check_postgres()
    check_mongo()
    check_cassandra()
    check_redis()

if __name__ == "__main__":
    main()