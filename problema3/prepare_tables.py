import psycopg2
from pymongo import MongoClient, ASCENDING, DESCENDING
from cassandra.cluster import Cluster
import redis

def create_postgres():
    print("postgres -> criando tabelas...")
    commands = [
        "DROP TABLE IF EXISTS activities;",
        "DROP TABLE IF EXISTS users;",
        
        """
        CREATE TABLE users (
            user_id       VARCHAR(255) PRIMARY KEY,
            handle        TEXT NOT NULL,
            title         TEXT,
            bio           TEXT,
            created_at    BIGINT,
            followers     INTEGER DEFAULT 0,
            following     INTEGER DEFAULT 0,
            posts_count   INTEGER DEFAULT 0
        );
        """,
        
        """
        CREATE TABLE activities (
            activity_id   VARCHAR(255) PRIMARY KEY,
            user_id       VARCHAR(255) NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            ts            BIGINT NOT NULL,
            type          VARCHAR(50) NOT NULL,
            payload       JSONB,
            created_at    BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())
        );
        """,
        
        "CREATE INDEX idx_users_handle ON users(handle);",
        "CREATE INDEX idx_activities_user ON activities(user_id);",
        "CREATE INDEX idx_activities_ts ON activities(ts DESC);",
        "CREATE INDEX idx_users_followers ON users(followers);"
    ]
    
    try:
        conn = psycopg2.connect(host="localhost", port="5432", database="trabalho_bd", user="admin", password="admin")
        cursor = conn.cursor()
        for command in commands:
            cursor.execute(command)
        conn.commit()
        cursor.close()
        conn.close()
        print("postgres -> tabelas users e activities recriadas com sucesso.")
    except Exception as e:
        print(f"erro --> PostgreSQL: {e}")

def create_cassandra():
    print("cassandra -> criando tabelas...")
    try:
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect()

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS trabalho_bd 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)
        
        session.set_keyspace('trabalho_bd')

        queries = [
            "DROP TABLE IF EXISTS activities;",
            "DROP TABLE IF EXISTS user_by_handle;",
            "DROP TABLE IF EXISTS users;",
            
            """
            CREATE TABLE users (
                user_id       TEXT PRIMARY KEY,
                handle        TEXT,
                title         TEXT,
                bio           TEXT,
                created_at    BIGINT,
                followers     INT,
                following     INT,
                posts_count   INT
            );
            """,
            
            """
            CREATE TABLE activities (
                user_id       TEXT,
                ts            BIGINT,
                activity_id   TEXT,
                type          TEXT,
                payload       TEXT,
                PRIMARY KEY ((user_id), ts, activity_id)
            ) WITH CLUSTERING ORDER BY (ts DESC);
            """,
            
            """
            CREATE TABLE user_by_handle (
              handle TEXT PRIMARY KEY,
              user_id TEXT
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_users_followers ON users (followers);"
        ]
        for q in queries:
            session.execute(q)

        cluster.shutdown()
        print("cassandra -> tabelas recriadas com sucesso.")
    except Exception as e:
        print(f"erro --> Cassandra: {e}")

def prepare_mongo():
    print("mongo -> preparando coleções e índices...")
    try:
        client = MongoClient("mongodb://admin:admin@localhost:27017/")
        db = client["trabalho_bd"]
        
        # Drop para limpar estado anterior
        db["users"].drop()
        db["activities"].drop()

        # Criando índices conforme solicitado na modelagem
        db["users"].create_index([("handle", ASCENDING)], unique=True)
        db["activities"].create_index([("userId", ASCENDING), ("ts", DESCENDING)])
        db["users"].create_index([("stats.followers", DESCENDING)])
        
        print("mongo -> coleções limpas e índices criados.")
    except Exception as e:
        print(f"erro --> MongoDB: {e}")

def prepare_redis():
    print("redis -> limpando base...")
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.flushdb() 
        print("redis -> banco limpo.")
    except Exception as e:
        print(f"erro --> Redis: {e}")

def main():
    print("--- Iniciando criação das estruturas do Problema 3 ---\n")
    create_postgres()
    create_cassandra()
    prepare_mongo()
    prepare_redis()
    print("\n--- Fim! ---")

if __name__ == "__main__":
    main()