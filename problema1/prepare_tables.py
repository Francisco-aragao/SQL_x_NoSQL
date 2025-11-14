import psycopg2
from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis

def create_postgres():
    print("postgres criando tabelas")
    commands = [
        "DROP TABLE IF EXISTS pedido_item;",
        "DROP TABLE IF EXISTS pedido;",
        "DROP TABLE IF EXISTS item;",
        "DROP TABLE IF EXISTS cliente;",
        
        """
        CREATE TABLE cliente (
            id            VARCHAR(32) PRIMARY KEY,
            nome          VARCHAR(120),
            email         VARCHAR(160), 
            data          TIMESTAMP DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE item (
            id    VARCHAR(32) PRIMARY KEY,
            nome  VARCHAR(120),
            valor NUMERIC(12,2) CHECK (valor >= 0)
        );
        """,
        """
        CREATE TABLE pedido (
            id          VARCHAR(32) PRIMARY KEY,
            cliente_id  VARCHAR(32) NOT NULL REFERENCES cliente(id) ON DELETE CASCADE,
            data        TIMESTAMP DEFAULT NOW(),
            status      VARCHAR(24) DEFAULT 'pendente'
        );
        """,
        """
        CREATE TABLE pedido_item (
            pedido_id  VARCHAR(32) NOT NULL REFERENCES pedido(id) ON DELETE CASCADE,
            item_id    VARCHAR(32) NOT NULL REFERENCES item(id),
            quantidade INT    NOT NULL CHECK (quantidade > 0),
            preco_unit NUMERIC(12,2) NOT NULL CHECK (preco_unit >= 0),
            PRIMARY KEY (pedido_id, item_id)
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
        print("tabela postgres recriadas com sucesso.")
    except Exception as e:
        print(f"erro --> PostgreSQL: {e}")

def create_cassandra():
    print("cassandra -> criando tabelas")
    try:
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect()

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS trabalho_bd 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)
        
        session.set_keyspace('trabalho_bd')

        queries = [
            "DROP TABLE IF EXISTS clientes;",
            """
            CREATE TABLE clientes (
                cliente_id TEXT PRIMARY KEY,
                nome TEXT,
                email TEXT,
                data_cadastro TIMESTAMP
            );
            """,
            "DROP TABLE IF EXISTS pedidos_por_cliente;",
            """
            CREATE TABLE pedidos_por_cliente (
                cliente_id TEXT,
                pedido_id TEXT,
                data_pedido TIMESTAMP,
                valor_total DECIMAL,
                status TEXT,
                PRIMARY KEY (cliente_id, pedido_id)
            ) WITH CLUSTERING ORDER BY (pedido_id DESC);
            """,
            "DROP TABLE IF EXISTS itens_por_pedido;",
            """
            CREATE TABLE itens_por_pedido (
                pedido_id TEXT,
                item_id TEXT,
                produto_nome TEXT,
                quantidade INT,
                preco_unitario DECIMAL,
                PRIMARY KEY (pedido_id, item_id)
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
    print("mongo preparando coleções")
    try:
        client = MongoClient("mongodb://admin:admin@localhost:27017/")
        db = client["trabalho_bd"]
        
        db["clientes"].drop()
        db["itens"].drop()
        db["pedidos"].drop()
        
        # não tem nada pra fazer, só apago o que tinha antes mesmo pois mongo usa lazy creation
        
        print("mongo coleções limpas e prontas.")
    except Exception as e:
        print(f"erro --> MongoDB: {e}")

def prepare_redis():
    print("redis limpando banco")
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.flushdb()
        print("redis banco limpo.")
    except Exception as e:
        print(f"erro --> Redis: {e}")

def main():
    print("iniciando criação ...\n")
    create_postgres()
    prepare_mongo()
    create_cassandra()
    prepare_redis()
    print("fim!")

if __name__ == "__main__":
    main()