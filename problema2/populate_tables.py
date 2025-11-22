import pandas as pd
import psycopg2
import psycopg2.extras
from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis
import sys
import os
import json
import argparse
from tqdm import tqdm
from datetime import datetime
import numpy as np

DATA_DIR = './data'
FILENAME = 'en.openfoodfacts.org.products.tsv' # nome do dataset

def connect_postgres():
    print("conectando postgres...")
    return psycopg2.connect(host="localhost", port="5432", database="trabalho_bd", user="admin", password="admin")

def connect_mongo():
    print("conectando mongo...")
    client = MongoClient("mongodb://admin:admin@localhost:27017/")
    client.admin.command('ping') 
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
    r.ping()
    return r

def clean_float(value):
    try:
        if pd.isna(value) or value == '':
            return None
        return float(value)
    except:
        return None

def load_source_data(limit_rows=None):
    """
    Função pra carregar todos os CSVs, limpar  e retornar os arquivos prontos.
    """
    file_path = os.path.join(DATA_DIR, FILENAME)
    print(f"lendo de '{file_path}'")
    
    # so uso algumas colunas relevantes
    use_cols = [
        'code', 'product_name', 'brands', 'categories_en', 
        'energy_100g', 'fat_100g', 'carbohydrates_100g', 
        'proteins_100g', 'fiber_100g', 'sodium_100g', 
        'last_modified_datetime'
    ]

    df = pd.read_csv(
        file_path, 
        sep='\t', 
        usecols=use_cols, 
        nrows=limit_rows,
        dtype={'code': str}, 
        on_bad_lines='skip'
    )

    df = df.rename(columns={
        'code': 'id',
        'product_name': 'nome',
        'categories_en': 'categoria',
        'brands': 'marca',
        'energy_100g': 'energia',
        'fat_100g': 'gordura',
        'carbohydrates_100g': 'carboidratos',
        'proteins_100g': 'proteinas',
        'fiber_100g': 'fibras',
        'sodium_100g': 'sodio',
        'last_modified_datetime': 'data_atualizacao'
    })

    df = df.dropna(subset=['id']) 
    df['nome'] = df['nome'].fillna('Desconhecido')
    
    df['categoria'] = df['categoria'].fillna('Outros').astype(str).apply(lambda x: x.split(',')[0].strip())
    df['marca'] = df['marca'].fillna('Genérico').astype(str).apply(lambda x: x.split(',')[0].strip())
    
    df['data_atualizacao'] = pd.to_datetime(df['data_atualizacao'], errors='coerce').fillna(datetime.now())

    numeric_cols = ['energia', 'gordura', 'carboidratos', 'proteinas', 'fibras', 'sodio']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.astype(object).where(pd.notnull(df), None)

    print(f"dados prontos -> {len(df)} produtos.")
    return df

def load_into_postgres(cursor, df):
    """
    Insiro dados no postgres. Faço load em massa pois banco trabalha com isso
    Muitos campos serão NULL.
    """
    print("\ncarregando dados postgres...")
    
    pg_data = []
    for _, row in df.iterrows():
        pg_data.append((
            row['id'], row['nome'], row['marca'], row['categoria'],
            row['energia'], row['gordura'], row['carboidratos'], 
            row['proteinas'], row['fibras'], row['sodio'], 
            row['data_atualizacao']
        ))
    
    query = """
    INSERT INTO produto 
    (id, nome, marca, categoria, energia, gordura, carboidratos, proteinas, fibras, sodio, data_atualizacao) 
    VALUES %s ON CONFLICT (id) DO NOTHING
    """
    psycopg2.extras.execute_values(cursor, query, pg_data)
    print(f"postgres-> {len(pg_data)} produtos carregados.")
    cursor.connection.commit()

def load_into_mongo(db, df):
    """
    insiro dados no mongo. Simples pois tem esquema flexível.
    Apenas não insiro campos nulos.
    """
    print("carregando dados mongo...")
    
    docs = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Mongo Prep"):
        doc = {
            "_id": row['id'],
            "nome": row['nome'],
            "marca": row['marca'],
            "categoria": row['categoria'],
            "data_atualizacao": row['data_atualizacao'],
            "nutrientes": {}
        }
        
        # Só adiciona nutrientes se eles existirem (não insere null)
        nutrientes = ['energia', 'gordura', 'carboidratos', 'proteinas', 'fibras', 'sodio']
        for nutri in nutrientes:
            if row[nutri] is not None:
                doc['nutrientes'][nutri] = row[nutri]
        
        docs.append(doc)

    if docs:
        db["produtos"].delete_many({})
        db["produtos"].insert_many(docs)
        print(f"mongo-> {len(docs)} produtos carregados.")

def load_into_cassandra(session, df):
    """
    Insiro dados no cassandra. Uso Map para nutrientes pois fica bem fácil de trabalhar.
    """
    print("carregando dados cassandra...")
    
    insert_stmt = session.prepare("""
        INSERT INTO produtos (produto_id, nome, marca, categoria, nutrientes, data_atualizacao)
        VALUES (?, ?, ?, ?, ?, ?)
    """)
    
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Cassandra Load"):
        nutri_map = {}
        nutrientes = ['energia', 'gordura', 'carboidratos', 'proteinas', 'fibras', 'sodio']
        for nutri in nutrientes:
            if row[nutri] is not None:
                nutri_map[nutri] = float(row[nutri])
        
        session.execute(insert_stmt, (
            row['id'], row['nome'], row['marca'], row['categoria'],
            nutri_map, row['data_atualizacao']
        ))

def load_into_redis(conn, df):
    """
    Insiro dados no redis. Crio índices invertidos (SETS) para marca e categoria.
    Sem isso fica dificil fazer algumas consultas.
    """
    print("carregando dados redis...")
    
    pipe = conn.pipeline()
    
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Redis Load"):
        key = f"item:{row['id']}"
        hash_data = {
            "nome": row['nome'],
            "marca": row['marca'],
            "categoria": row['categoria'],
            "data_atualizacao": row['data_atualizacao'].isoformat()
        }
        nutrientes = ['energia', 'gordura', 'carboidratos', 'proteinas', 'fibras', 'sodio']
        for nutri in nutrientes:
            if row[nutri] is not None:
                hash_data[nutri] = str(row[nutri])
                
        pipe.hmset(key, hash_data)
        
        marca_idx = f"idx:marca:{row['marca'].lower()}"
        cat_idx = f"idx:categoria:{row['categoria'].lower()}"
        
        pipe.sadd(marca_idx, row['id'])
        pipe.sadd(cat_idx, row['id'])
        
        if row['energia'] is not None:
            pipe.zadd("idx:energia", {row['id']: row['energia']})
            
    pipe.execute()
    print("redis-> dados e índices criados.")

def main(args):
    file_path = os.path.join(DATA_DIR, FILENAME)
    if not os.path.exists(file_path):
        print(f"erro -> Arquivo '{file_path}' não encontrado.")
        raise Exception()
        
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
        
        df = load_source_data(limit_rows=args.limit_rows)
        
        load_into_postgres(pg_conn.cursor(), df)
        load_into_mongo(mongo_db, df)
        load_into_cassandra(cassandra_session, df)
        load_into_redis(redis_conn, df)
        
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

    parser = argparse.ArgumentParser(description="Carrega dados do Open Food Facts em 4 bancos")
    parser.add_argument("--limit-rows", type=int, help="Limita o número de linhas lidas (só para teste)")
    args = parser.parse_args()
    
    main(args)