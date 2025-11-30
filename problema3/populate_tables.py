import psycopg2
import psycopg2.extras
from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis
import sys
import os
import json
import glob
import time
import ijson 

# --- Configurações ---
DATA_DIR = './data'
DB_NAME = 'trabalho_bd'
BATCH_SIZE = 100000  
MAX_ACTIVITIES_PER_FILE = 1_500_000

# --- Conexões ---

def connect_postgres():
    return psycopg2.connect(host="localhost", port="5432", database=DB_NAME, user="admin", password="admin")

def connect_mongo():
    client = MongoClient("mongodb://admin:admin@localhost:27017/")
    return client

def connect_cassandra():
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {DB_NAME} 
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(DB_NAME)
    return cluster, session

def connect_redis():
    return redis.Redis(host='localhost', port=6379, db=0)

def insert_batch_postgres(conn, users_batch, activities_batch):
    cursor = conn.cursor()
    
    # Inserir Usuários
    if users_batch:
        users_batch = [u for u in users_batch if u.get('user_id') is not None]

        if users_batch:
            user_tuples = [
                (u['user_id'], u['handle'], u['title'], u['bio'], u['created_at'], u['posts_count']) 
                for u in users_batch
            ]
            q_user = """
            INSERT INTO users (user_id, handle, title, bio, created_at, posts_count) 
            VALUES %s 
            ON CONFLICT (user_id) DO NOTHING
            """
            psycopg2.extras.execute_values(cursor, q_user, user_tuples)

    # Inserir Atividades
    if activities_batch:
        activities_batch = [
            a for a in activities_batch 
            if a.get('activity_id') is not None and a.get('user_id') is not None
        ]

        if activities_batch:
            act_tuples = [
                (a['activity_id'], a['user_id'], a['ts'], a['type'], a['payload'])
                for a in activities_batch
            ]
            q_act = """
            INSERT INTO activities (activity_id, user_id, ts, type, payload)
            VALUES %s ON CONFLICT (activity_id) DO NOTHING
            """
            psycopg2.extras.execute_values(cursor, q_act, act_tuples)
    
    conn.commit()

def insert_batch_mongo(db, users_batch, activities_batch):
    if users_batch:
        # Adapta estrutura para MongoDB
        docs = []
        # Usuários
        for u in users_batch:
            docs.append({
                "_id": u['user_id'],
                "handle": u['handle'],
                "title": u['title'],
                "profile": { "bio": u['bio'] },
                "createdAt": u['created_at'],
                "stats": json.loads(u['stats_json'])
            })
        try: db.users.insert_many(docs, ordered=False)
        except: pass

    # Atividades
    if activities_batch:
        docs = []
        for a in activities_batch:
            docs.append({
                "_id": a['activity_id'],
                "userId": a['user_id'],
                "ts": a['ts'],
                "type": a['type'],
                "payload": json.loads(a['payload'])
            })
        try: db.activities.insert_many(docs, ordered=False)
        except: pass

def insert_batch_cassandra(session, prepared_stmts, users_batch, activities_batch):
    stmt_user, stmt_act = prepared_stmts
    
    # Usuários
    for u in users_batch:
        session.execute(stmt_user, (
            str(u['user_id']), str(u['handle']), str(u['title']), 
            str(u['bio']), int(u['created_at']) if u['created_at'] else 0, 
            int(u['posts_count'])
        ))

    # Atividades
    for a in activities_batch:
        session.execute(stmt_act, (
            str(a['activity_id']), str(a['user_id']), int(a['ts']), 
            str(a['type']), str(a['payload'])
        ))

def insert_batch_redis(pipe, users_batch, activities_batch):
    # Usuários
    for u in users_batch:
        key = f"user:{u['user_id']}"
        mapping = {
            "handle": str(u['handle']),
            "created_at": str(u['created_at']),
            "posts": u['posts_count']
        }
        if u['title']: mapping['title'] = str(u['title'])
        if u['bio']: mapping['bio'] = str(u['bio'])
        
        pipe.hset(key, mapping=mapping)
        pipe.set(f"user:handle:{u['handle']}", u['user_id'])

    # Atividades
    for a in activities_batch:
        key = f"activity:{a['activity_id']}"
        mapping = {
            "user_id": str(a['user_id']),
            "ts": str(a['ts']),
            "type": str(a['type']),
            "payload": str(a['payload'])
        }
        pipe.hset(key, mapping=mapping)
        pipe.lpush(f"timeline:{a['user_id']}", a['activity_id'])
    
    pipe.execute()

def count_posts_first_pass(post_files):
    """
    Lê apenas os posts rapidamente usando ijson para contar quantos cada user tem.
    Isso economiza memória pois não carrega o conteúdo.
    """
    print(">> Contando posts por usuário...")
    user_counts = {}
    
    for fpath in post_files:
        print(f"   Lendo {os.path.basename(fpath)}...")
        with open(fpath, 'rb') as f:
            # ijson.items lê objeto por objeto sem carregar a lista toda
            # 'item' significa cada elemento do array raiz
            for item in ijson.items(f, 'item'):
                uid = item.get('creatorId')
                if uid:
                    user_counts[uid] = user_counts.get(uid, 0) + 1
    return user_counts

def process_users_stream(users_file, user_counts, db_conns):
    """
    Passo 2: Lê arquivo de usuários em stream, prepara lotes e insere.
    Retorna o conjunto de user_ids válidos inseridos.
    """
    print(">> Processando Usuários em Batches...")
    
    if not os.path.exists(users_file):
        print("Arquivo de usuários não encontrado.")
        return set()

    batch = []
    total_processed = 0
    valid_user_ids = set()  # <- guardamos todos os users válidos
    
    pg_conn, mongo_db, cass_sess, cass_stmts, redis_pipe = db_conns

    with open(users_file, 'rb') as f:
        for item in ijson.items(f, 'item'):
            uid = item.get('id')

            # se não tiver id, pula o registro
            if not uid:
                continue

            count = user_counts.get(uid, 0)
            
            user_dict = {
                'user_id': uid,
                'handle': item.get('handle'),
                'title': item.get('title'),
                'bio': item.get('description'),
                'created_at': item.get('createdAt'),
                'posts_count': count,
                'stats_json': json.dumps({'followers': 0, 'following': 0, 'posts': count})
            }
            batch.append(user_dict)
            valid_user_ids.add(uid)  # <- marca user como existente
            
            if len(batch) >= BATCH_SIZE:
                insert_batch_postgres(pg_conn, batch, [])
                insert_batch_mongo(mongo_db, batch, [])
                insert_batch_cassandra(cass_sess, cass_stmts, batch, [])
                insert_batch_redis(redis_pipe, batch, [])
                
                total_processed += len(batch)
                print(f"   Usuários processados: {total_processed}...", end='\r')
                batch = []

    # Processa restante
    if batch:
        insert_batch_postgres(pg_conn, batch, [])
        insert_batch_mongo(mongo_db, batch, [])
        insert_batch_cassandra(cass_sess, cass_stmts, batch, [])
        insert_batch_redis(redis_pipe, batch, [])
        print(f"   Usuários finalizados: {total_processed + len(batch)}")

    return valid_user_ids

def process_activities_stream(files, activity_type, db_conns, valid_user_ids, max_records=None):
    """
    Passo 3: Lê arquivos de atividades em stream, prepara lotes e insere.
    Só insere activities cujo user_id exista em valid_user_ids.
    Se max_records for informado, para de ler após atingir esse número de registros lidos.
    """
    print(f">> Processando {activity_type} em Batches...")
    
    pg_conn, mongo_db, cass_sess, cass_stmts, redis_pipe = db_conns
    batch = []
    total_processed = 0
    total_read = 0  # quantos registros foram LIDOS do JSON
    stop = False    # flag para parar os dois loops (arquivo e item)

    for fpath in files:
        if stop:
            break

        print(f"   Lendo {os.path.basename(fpath)}...")
        with open(fpath, 'rb') as f:
            for item in ijson.items(f, 'item'):
                total_read += 1

                # Se atingiu o limite de registros lidos, para.
                if max_records is not None and total_read > max_records:
                    stop = True
                    break

                act = None

                try:
                    if activity_type == 'POST':
                        user_id = item.get('creatorId')
                        if not user_id:
                            continue
                        if user_id not in valid_user_ids:
                            continue  # não existe user -> não salva activity

                        act = {
                            'activity_id': item['id'],
                            'user_id': user_id,
                            'ts': item['createdAt'],
                            'type': 'POST',
                            'payload': json.dumps({'content': item.get('title', '')})
                        }

                    elif activity_type == 'LIKE':
                        user_id = item.get('liker_id')
                        if not user_id:
                            continue
                        if user_id not in valid_user_ids:
                            continue

                        act = {
                            'activity_id': f"{item['id']}_{user_id}_like",
                            'user_id': user_id,
                            'ts': item['createdAt'],
                            'type': 'LIKE',
                            'payload': json.dumps({'targetId': item['id']})
                        }

                    elif activity_type == 'COMMENT':
                        user_id = item.get('commenter_id')
                        if not user_id:
                            continue
                        if user_id not in valid_user_ids:
                            continue

                        act = {
                            'activity_id': f"{item['id']}_{user_id}_{item['createdAt']}_comment",
                            'user_id': user_id,
                            'ts': item['createdAt'],
                            'type': 'COMMENT',
                            'payload': json.dumps({
                                'content': item.get('title', ''), 
                                'targetId': item['id']
                            })
                        }

                    elif activity_type == 'SHARE':
                        user_id = item.get('sharer_id')
                        if not user_id:
                            continue
                        if user_id not in valid_user_ids:
                            continue

                        act = {
                            'activity_id': f"{item['id']}_{user_id}_share",
                            'user_id': user_id,
                            'ts': item['createdAt'],
                            'type': 'SHARE',
                            'payload': json.dumps({'targetId': item['id']})
                        }

                except KeyError:
                    # Dados corrompidos/incompletos
                    continue

                if not act:
                    continue

                batch.append(act)

                if len(batch) >= BATCH_SIZE:
                    insert_batch_postgres(pg_conn, [], batch)
                    insert_batch_mongo(mongo_db, [], batch)
                    insert_batch_cassandra(cass_sess, cass_stmts, [], batch)
                    insert_batch_redis(redis_pipe, [], batch)
                    
                    total_processed += len(batch)
                    print(f"   {activity_type}s processados: {total_processed} (lidos: {total_read})...", end='\r')
                    batch = []

    # Flush final dos registros que ficaram no batch
    if batch:
        insert_batch_postgres(pg_conn, [], batch)
        insert_batch_mongo(mongo_db, [], batch)
        insert_batch_cassandra(cass_sess, cass_stmts, [], batch)
        insert_batch_redis(redis_pipe, [], batch)

    print(f"\n   {activity_type}s - total lidos: {total_read}, total inseridos: {total_processed}")

def main():
    start_global = time.time()
    
    # 1. Identificar arquivos
    users_file = os.path.join(DATA_DIR, 'koo_users.json')
    post_files = glob.glob(os.path.join(DATA_DIR, 'pt_posts.json'))
    like_files = glob.glob(os.path.join(DATA_DIR, 'pt_likes.json'))
    comment_files = glob.glob(os.path.join(DATA_DIR, 'pt_comments.json'))
    share_files = glob.glob(os.path.join(DATA_DIR, 'pt_shares.json'))

    # 2. Conectar Bancos
    print("Conectando aos bancos...")
    try:
        pg_conn = connect_postgres()
        mongo_client = connect_mongo()
        mongo_db = mongo_client[DB_NAME]
        
        cass_cluster, cass_sess = connect_cassandra()
        # tatements do Cassandra 
        c_stmt_user = cass_sess.prepare("INSERT INTO users (user_id, handle, title, bio, created_at, posts_count) VALUES (?, ?, ?, ?, ?, ?)")
        c_stmt_act = cass_sess.prepare("INSERT INTO activities (activity_id, user_id, ts, type, payload) VALUES (?, ?, ?, ?, ?)")
        
        redis_conn = connect_redis()
        redis_pipe = redis_conn.pipeline()

        db_conns = (pg_conn, mongo_db, cass_sess, (c_stmt_user, c_stmt_act), redis_pipe)
        
        # Passo 1: Contar posts (rápido, só leitura)
        user_counts = count_posts_first_pass(post_files)
        
        # Passo 2: Processar Usuários (retorna ids válidos)
        valid_user_ids = process_users_stream(users_file, user_counts, db_conns)
        
        # Passo 3: Processar Atividades (apenas para users válidos)
        if post_files:    process_activities_stream(post_files, 'POST', db_conns, valid_user_ids, MAX_ACTIVITIES_PER_FILE)
        if like_files:    process_activities_stream(like_files, 'LIKE', db_conns, valid_user_ids, MAX_ACTIVITIES_PER_FILE)
        if comment_files: process_activities_stream(comment_files, 'COMMENT', db_conns, valid_user_ids, MAX_ACTIVITIES_PER_FILE)
        if share_files:   process_activities_stream(share_files, 'SHARE', db_conns, valid_user_ids, MAX_ACTIVITIES_PER_FILE)

    except Exception as e:
        print(f"\nERRO FATAL: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\nFechando conexões...")
        try: pg_conn.close()
        except: pass
        try: mongo_client.close()
        except: pass
        try: cass_cluster.shutdown()
        except: pass
        try: redis_conn.close()
        except: pass
        
        print(f"Tempo Total Global: {time.time() - start_global:.2f}s")

if __name__ == "__main__":
    main()