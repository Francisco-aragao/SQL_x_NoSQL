import psycopg2
from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis

def create_postgres():
    print("postgres criando tabelas")
    commands = [
        "DROP TABLE IF EXISTS produto;",
        
        """
        CREATE TABLE produto (
            id              VARCHAR(50) PRIMARY KEY,
            nome            VARCHAR(255),
            marca           VARCHAR(255),
            categoria       VARCHAR(255),
            energia         REAL,
            gordura         REAL,
            carboidratos    REAL,
            proteinas       REAL,
            fibras          REAL,
            sodio           REAL, 
            data_atualizacao TIMESTAMP
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
            "DROP TABLE IF EXISTS produtos;",
            """
            CREATE TABLE produtos (
                produto_id TEXT PRIMARY KEY,
                nome TEXT,
                marca TEXT,
                categoria TEXT,
                nutrientes MAP<TEXT, FLOAT>, 
                data_atualizacao TIMESTAMP
            );
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
        
        db["produtos"].drop()

        # não tem nada pra fazer, só apago o que tinha antes mesmo pois mongo usa lazy creation
        
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
    print("iniciando criação das estruturas do Problema 2...\n")
    create_postgres()
    create_cassandra()
    prepare_mongo()
    prepare_redis()
    print("\nfim!")

if __name__ == "__main__":
    main()