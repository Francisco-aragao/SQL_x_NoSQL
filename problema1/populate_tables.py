import pandas as pd
import psycopg2
import psycopg2.extras
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import redis
import sys
import os
import json
import argparse
from tqdm import tqdm
from datetime import datetime

DATA_DIR = './data'

def connect_postgres():
    print("conectando postgres")
    return psycopg2.connect(host="localhost", port="5432", database="trabalho_bd", user="admin", password="admin")

def connect_mongo():
    print("conectando mongo")
    client = MongoClient("mongodb://admin:admin@localhost:27017/")
    client.admin.command('ping') 
    return client

def connect_cassandra():
    print("conectando cassandra")
    cluster = Cluster(['localhost'], port=9042, load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'))
    session = cluster.connect()
    session.set_keyspace('trabalho_bd')
    return cluster, session

def connect_redis():
    print("conectando redis")
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.ping()
    return r

def load_source_data(limit_rows=None):
    """
    Função pra carregar todos os CSVs, limpar  e retornar os arquivos prontos.
    """
    print(f"lendo de '{DATA_DIR}' ---")
    
    df_customers = pd.read_csv(os.path.join(DATA_DIR, 'customers.csv'))
    df_customers = df_customers.dropna(subset=['customer_id'])
    df_customers['name'] = df_customers['name'].fillna('Nome Indisponível')
    df_customers['email'] = df_customers['email'].fillna('email@indisponivel.com')
    df_customers['registration_date'] = pd.to_datetime(df_customers['registration_date'], errors='coerce').fillna(datetime.now())
    df_customers['customer_id'] = df_customers['customer_id'].astype(int).astype(str)
    
    # precisei fazer isso pois tinha clientes inválidos nos pedidos
    valid_customer_ids = set(df_customers['customer_id'])
    print(f"clientes: {len(df_customers)}, clientes válidos: ({len(valid_customer_ids)}).")
    
    df_products = pd.read_csv(os.path.join(DATA_DIR, 'products.csv')).dropna(subset=['product_id'])
    df_products['price'] = pd.to_numeric(df_products['price'], errors='coerce').fillna(0)
    df_products['product_name'] = df_products['product_name'].fillna('Nome Indisponível').astype(str)
    df_products['product_id'] = df_products['product_id'].astype(str)
    valid_product_ids = set(df_products['product_id'])
    print(f"produtos: {len(df_products)}, produtos válidos: ({len(valid_product_ids)}).")

    df_orders = pd.read_csv(os.path.join(DATA_DIR, 'orders.csv'))
    df_order_items = pd.read_csv(os.path.join(DATA_DIR, 'order_items.csv'), nrows=limit_rows)

    df_full_order = pd.merge(df_order_items, df_orders, on='order_id')
    
    df_full_order = df_full_order.dropna(subset=[
        'order_id', 'customer_id', 'product_id', 
        'order_date', 'order_status', 'quantity', 'total_price'
    ])
    
    df_full_order['order_date'] = pd.to_datetime(df_full_order['order_date'])
    df_full_order['order_status'] = df_full_order['order_status'].astype(str)
    df_full_order['order_id'] = df_full_order['order_id'].astype(str)
    df_full_order['product_id'] = df_full_order['product_id'].astype(str)
    df_full_order['customer_id'] = df_full_order['customer_id'].astype(int).astype(str)

    initial_count = len(df_full_order)
    df_full_order = df_full_order[df_full_order['product_id'].isin(valid_product_ids)]
    removed_count = initial_count - len(df_full_order)
    
    print(f"pedidos: {len(df_full_order)} itens de pedido lidos.")
    if removed_count > 0:
        print(f"removidos: {removed_count} itens invalidos")
        
    initial_count_cust = len(df_full_order)
    df_full_order = df_full_order[df_full_order['customer_id'].isin(valid_customer_ids)]
    removed_count_cust = initial_count_cust - len(df_full_order)
    
    if removed_count_cust > 0:
        print(f"removidos: {removed_count_cust} itens invalidos")
            
    return df_customers, df_products, df_full_order

def load_into_postgres(cursor, df_customers, df_products, df_full_order):
    """
    Insiro dados no postgres. Faço load em massa pois banco trabalha com isso
    """
    print("\ncarregando dados postgres")
    
    pg_data = [
        (row['customer_id'], row['name'], row['email'], row['registration_date']) 
        for _, row in df_customers.iterrows()
    ]
    psycopg2.extras.execute_values(cursor,
        "INSERT INTO cliente (id, nome, email, data) VALUES %s ON CONFLICT (id) DO NOTHING", pg_data)
    print(f"postgres-> {len(pg_data)} clientes carregados.")

    pg_data = [
        (row['product_id'], row['product_name'], row['price']) 
        for _, row in df_products.iterrows()
    ]
    psycopg2.extras.execute_values(cursor,
        "INSERT INTO item (id, nome, valor) VALUES %s ON CONFLICT (id) DO NOTHING", pg_data)
    print(f"postgres-> {len(pg_data)} itens carregados.")

    pg_pedidos_data = [
        (row['order_id'], row['customer_id'], row['order_date'], row['order_status'])
        for _, row in df_full_order.iterrows()
    ]
    psycopg2.extras.execute_values(cursor,
        "INSERT INTO pedido (id, cliente_id, data, status) VALUES %s ON CONFLICT (id) DO NOTHING", 
        list(set(pg_pedidos_data))) # 'set' remove duplicatas de pedidos
    
    pg_pedido_items_data = []
    for _, row in df_full_order.iterrows():
        quantidade = int(row['quantity'])
        total_preco = float(row['total_price'])
        preco_unit = (total_preco / quantidade) if (quantidade > 0 and total_preco > 0) else 0
        pg_pedido_items_data.append((row['order_id'], row['product_id'], quantidade, preco_unit))
        
    psycopg2.extras.execute_values(cursor,
        "INSERT INTO pedido_item (pedido_id, item_id, quantidade, preco_unit) VALUES %s ON CONFLICT (pedido_id, item_id) DO NOTHING", 
        pg_pedido_items_data)
    print(f"postgres-> {len(pg_pedido_items_data)} itens de pedido carregados.")

    cursor.connection.commit()

def load_into_mongo(db, df_customers, df_products, df_full_order):
    """
    insiro dados mongo. Faço insert em massa para eficiência.
    """
    print("carregando dados mongo")

    cliente_docs = [
        {"_id": row['customer_id'], "nome": row['name'], "email": row['email'], "data_cadastro": row['registration_date']}
        for _, row in df_customers.iterrows()
    ]
    if cliente_docs:
        db["clientes"].delete_many({}) # Limpa antes de inserir
        db["clientes"].insert_many(cliente_docs)
        print(f"mongo-> {len(cliente_docs)} clientes carregados.")

    # Itens (Bulk)
    item_docs = [
        {"_id": row['product_id'], "nome": row['product_name'], "valor": row['price']}
        for _, row in df_products.iterrows()
    ]
    if item_docs:
        db["itens"].delete_many({}) 
        db["itens"].insert_many(item_docs)
        print(f"mongo-> {len(item_docs)} itens carregados.")
        
    mongo_pedidos_map = {}
    for _, row in df_full_order.iterrows():
        quantidade = int(row['quantity'])
        total_preco = float(row['total_price'])
        preco_unit = (total_preco / quantidade) if (quantidade > 0 and total_preco > 0) else 0
        
        item_doc = { "item_id": row['product_id'], "quantidade": quantidade, "preco_unit": preco_unit }
        
        if row['order_id'] not in mongo_pedidos_map:
            mongo_pedidos_map[row['order_id']] = {
                "_id": row['order_id'],
                "cliente_id": row['customer_id'],
                "data_pedido": row['order_date'],
                "status": row['order_status'],
                "itens": [item_doc]
            }
        else:
            mongo_pedidos_map[row['order_id']]["itens"].append(item_doc)
            
    if mongo_pedidos_map:
        db["pedidos"].delete_many({}) # Limpa antes de inserir
        db["pedidos"].insert_many(mongo_pedidos_map.values())
        print(f"mongo-> {len(mongo_pedidos_map)} pedidos (com itens embutidos) carregados.")
        

def load_into_cassandra(session, df_customers, df_full_order):
    """
    Insiro dados no Cassandra. Não faço em batch pois no Cassandra, isso não é recomendado. -> gera sobrecarga no nó coordenador.
    conferir: https://github.com/thingsboard/thingsboard/issues/8512

    Não precisa de 'df_products' pois o modelo de dados é denormalizado  e focado nas queries. Não temos uma tabela 'produtos' separada; 
    todos os dados necessários (como 'item_id', 'quantidade' e 'preco_unit') já vêm do 'df_full_order' (o merge de pedidos e itens).
    """
    print("carregando dados cassandra")
    
    customer_stmt = session.prepare("INSERT INTO clientes (cliente_id, nome, email, data_cadastro) VALUES (?, ?, ?, ?)")
    pedido_stmt = session.prepare("INSERT INTO pedidos_por_cliente (cliente_id, pedido_id, data_pedido, status) VALUES (?, ?, ?, ?)")
    item_stmt = session.prepare("INSERT INTO itens_por_pedido (pedido_id, item_id, quantidade, preco_unitario) VALUES (?, ?, ?, ?)")

    for _, row in tqdm(df_customers.iterrows(), total=len(df_customers), desc="Cassandra Clientes"):
        session.execute(customer_stmt, (row['customer_id'], row['name'], row['email'], row['registration_date']))
        
    for _, row in tqdm(df_full_order.iterrows(), total=len(df_full_order), desc="Cassandra Pedidos"):
        quantidade = int(row['quantity'])
        total_preco = float(row['total_price'])
        preco_unit = (total_preco / quantidade) if (quantidade > 0 and total_preco > 0) else 0
        
        session.execute(pedido_stmt, (row['customer_id'], row['order_id'], row['order_date'], row['order_status']))
        session.execute(item_stmt, (row['order_id'], row['product_id'], quantidade, preco_unit))
    
def load_into_redis(conn, df_customers, df_products, df_full_order):
    """
    Carrega os DataFrames no Redis usando pipelines para eficiência. Junto varios comandos e mando todos de uma vez só.
    cada entidade foi mapeada como hash (mapa chave-valor) para facilitar buscas.
    Relacionamento foi simulado via chaves compostas.

    """
    print("carregando dados redis")
    
    pipe = conn.pipeline()
    for _, row in tqdm(df_customers.iterrows(), total=len(df_customers), desc="Redis Clientes"):
        key = f"cliente:{row['customer_id']}"
        pipe.hmset(key, {
            "nome": row['name'], 
            "email": row['email'], 
            "data_cadastro": row['registration_date'].isoformat()
        })
    pipe.execute()
    
    pipe = conn.pipeline()
    for _, row in tqdm(df_products.iterrows(), total=len(df_products), desc="Redis Itens"):
        key = f"item:{row['product_id']}"
        pipe.hmset(key, {
            "nome": row['product_name'], 
            "valor": row['price']
        })
    pipe.execute()
    
    pipe = conn.pipeline()
    for _, row in tqdm(df_full_order.iterrows(), total=len(df_full_order), desc="Redis Pedidos"):
        quantidade = int(row['quantity'])
        total_preco = float(row['total_price'])
        preco_unit = (total_preco / quantidade) if (quantidade > 0 and total_preco > 0) else 0
        
        key_pedido = f"pedido:{row['order_id']}"
        pipe.hmset(key_pedido, {
            "cliente_id": row['customer_id'],
            "data_pedido": row['order_date'].isoformat(),
            "status": row['order_status']
        })
        
        key_item = f"pedido_item:{row['order_id']}"
        item_json = json.dumps({"quantidade": quantidade, "preco_unit": preco_unit})
        pipe.hset(key_item, row['product_id'], item_json)
    pipe.execute()
    

def main(args):
    if not os.path.exists(DATA_DIR):
        print(f"Erro: Pasta '{DATA_DIR}' não encontrada.")
        sys.exit(1)
        
    pg_conn = None
    mongo_client = None
    cassandra_cluster = None
    redis_conn = None

    try:
        pg_conn = connect_postgres()
        mongo_client = connect_mongo()
        mongo_db = mongo_client["trabalho_bd"]
        cassandra_cluster, cassandra_session = connect_cassandra()
        redis_conn = connect_redis()
        
        dfs = load_source_data(limit_rows=args.limit_rows)
        df_customers, df_products, df_full_order = dfs
        
        load_into_postgres(pg_conn.cursor(), df_customers, df_products, df_full_order)
        load_into_mongo(mongo_db, df_customers, df_products, df_full_order)
        load_into_cassandra(cassandra_session, df_customers, df_full_order) # Não precisa de produtos
        load_into_redis(redis_conn, df_customers, df_products, df_full_order)
        
    except Exception as e:
        print(f"Erro: {e}")
        import traceback
        traceback.print_exc()
            
    finally:
        print("fechando conexoes")
        if pg_conn:
            pg_conn.close()
            print("fechou postgres.")
        if mongo_client:
            mongo_client.close()
            print("fechou mongo.")
        if cassandra_cluster:
            cassandra_cluster.shutdown()
            print("fechou cassandra.")
        if redis_conn:
            redis_conn.close()
            print("fechou redis.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Carrega dados CSV em 4 bancos de dados (SQL vs NoSQL)")
    parser.add_argument("--limit-rows", type=int, default=None,
                        help="Limita o número de linhas lidas do arquivo 'order_items.csv'")
    args = parser.parse_args()
    
    main(args)