import psycopg2
import psycopg2.extras
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import redis
import json
import os
import time
import uuid
from typing import List, Dict, Any
import traceback
from datetime import datetime

from abstract_queries import AbstractSocialDb, SocialUserData

# Helper para Redis
def _decode_redis(d):
    return {k.decode('utf-8'): v.decode('utf-8') for k, v in d.items()}

class PostgresDb(AbstractSocialDb):
    def connect(self):
        self.conn = psycopg2.connect(host="localhost", port="5432", database="trabalho_bd", user="admin", password="admin", cursor_factory=psycopg2.extras.RealDictCursor)
        print("PG conectado")

    def close(self):
        if self.conn: self.conn.close()

    def op1_create_user(self, data: SocialUserData) -> str:
        with self.conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO users (user_id, handle, title, bio, created_at) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING",
                (data.user_id, data.handle, data.title, data.bio, data.created_at)
            )
            self.conn.commit()
        return data.user_id

    def op2_read_user(self, user_id: str) -> Dict[str, Any]:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
            return cursor.fetchone()

    def op3_update_user_stats(self, user_id: str) -> bool:
        with self.conn.cursor() as cursor:
            cursor.execute("UPDATE users SET followers = followers + 1 WHERE user_id = %s", (user_id,))
            self.conn.commit()
            return cursor.rowcount > 0

    def op4_delete_activity(self, activity_id: str, user_id: str) -> bool:
        with self.conn.cursor() as cursor:
            cursor.execute("DELETE FROM activities WHERE activity_id = %s", (activity_id,))
            self.conn.commit()
            return cursor.rowcount > 0

    def op5_create_post_update_stats(self, user_id: str, content: str) -> str:
        new_id = str(uuid.uuid4())
        ts = int(time.time())
        payload = json.dumps({"content": content})
        
        # Transação: Insere activity e atualiza user
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO activities (activity_id, user_id, ts, type, payload) VALUES (%s, %s, %s, 'POST', %s)",
                    (new_id, user_id, ts, payload)
                )
                cursor.execute("UPDATE users SET posts_count = posts_count + 1 WHERE user_id = %s", (user_id,))
            self.conn.commit()
            return new_id
        except:
            self.conn.rollback()
            return None

    def op6_get_feed(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM activities WHERE user_id = %s ORDER BY ts DESC LIMIT %s", 
                (user_id, limit)
            )
            return cursor.fetchall()

    def op7_get_user_likes(self, user_id: str) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM activities WHERE user_id = %s AND type = 'LIKE' LIMIT 50", (user_id,))
            return cursor.fetchall()

    def op8_search_hashtag(self, hashtag: str) -> List[Dict[str, Any]]:
        # Busca textual dentro do JSONB (tem operadores específicos do Postgres para isso)
        with self.conn.cursor() as cursor:
            term = f"%{hashtag}%"
            cursor.execute("SELECT * FROM activities WHERE type IN ('POST','COMMENT') AND payload::text ILIKE %s LIMIT 20", (term,))
            return cursor.fetchall()

    def op9_aggregate_type_count(self, user_id: str) -> Dict[str, int]:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT type, COUNT(*) as qtd FROM activities WHERE user_id = %s GROUP BY type", (user_id,))
            return {row['type']: row['qtd'] for row in cursor.fetchall()}

    def op10_schema_evolution(self) -> int:
        with self.conn.cursor() as cursor:
            try:
                cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS verified BOOLEAN DEFAULT FALSE")
            except:
                self.conn.rollback()
            
            # Faz o update
            cursor.execute("UPDATE users SET verified = TRUE WHERE followers > 10000")
            count = cursor.rowcount
            self.conn.commit()
            return count


class MongoDb(AbstractSocialDb):
    def connect(self):
        self.client = MongoClient("mongodb://admin:admin@localhost:27017/")
        self.db = self.client["trabalho_bd"]
        print("Mongo conectado")

    def close(self): self.client.close()

    def op1_create_user(self, data: SocialUserData) -> str:
        doc = {
            "_id": data.user_id, "handle": data.handle, "title": data.title, 
            "profile": {"bio": data.bio}, "createdAt": data.created_at, 
            "stats": {"followers": 0, "following": 0, "posts": 0}
        }
        try: self.db.users.insert_one(doc)
        except: pass
        return data.user_id

    def op2_read_user(self, user_id: str) -> Dict[str, Any]:
        return self.db.users.find_one({"_id": user_id})

    def op3_update_user_stats(self, user_id: str) -> bool:
        # $inc é atômico
        res = self.db.users.update_one({"_id": user_id}, {"$inc": {"stats.followers": 1}})
        return res.modified_count > 0

    def op4_delete_activity(self, activity_id: str, user_id: str) -> bool:
        res = self.db.activities.delete_one({"_id": activity_id})
        return res.deleted_count > 0

    def op5_create_post_update_stats(self, user_id: str, content: str) -> str:
        new_id = str(uuid.uuid4())
        doc = {
            "_id": new_id, "userId": user_id, "ts": int(time.time()), 
            "type": "POST", "payload": {"content": content}
        }
        self.db.activities.insert_one(doc)
        self.db.users.update_one({"_id": user_id}, {"$inc": {"stats.posts": 1}})
        return new_id

    def op6_get_feed(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        return list(self.db.activities.find({"userId": user_id}).sort("ts", -1).limit(limit))

    def op7_get_user_likes(self, user_id: str) -> List[Dict[str, Any]]:
        return list(self.db.activities.find({"userId": user_id, "type": "LIKE"}).limit(50))

    def op8_search_hashtag(self, hashtag: str) -> List[Dict[str, Any]]:
        # Busca regex dentro do subdocumento payload
        return list(self.db.activities.find({
            "type": {"$in": ["POST", "COMMENT"]},
            "payload.content": {"$regex": hashtag, "$options": "i"}
        }).limit(20))

    def op9_aggregate_type_count(self, user_id: str) -> Dict[str, int]:
        # AMongo tem um aggregation pipeline nativo
        pipeline = [
            {"$match": {"userId": user_id}},
            {"$group": {"_id": "$type", "count": {"$sum": 1}}}
        ]
        res = list(self.db.activities.aggregate(pipeline))
        return {r["_id"]: r["count"] for r in res}

    def op10_schema_evolution(self) -> int:
        res = self.db.users.update_many(
            {"stats.followers": {"$gt": 10000}},
            {"$set": {"verified": True}}
        )
        return res.modified_count


class CassandraDb(AbstractSocialDb):
    def connect(self):
        self.cluster = Cluster(['localhost'], port=9042)
        self.session = self.cluster.connect('trabalho_bd')
        # Aumentar timeout para queries pesadas (scans)
        self.session.default_timeout = 60.0 
        print("Cassandra conectado")

    def close(self): self.cluster.shutdown()

    def op1_create_user(self, data: SocialUserData) -> str:
        self.session.execute(
            "INSERT INTO users (user_id, handle, title, bio, created_at, posts_count) VALUES (%s, %s, %s, %s, %s, 0)",
            (data.user_id, data.handle, data.title, data.bio, data.created_at)
        )
        return data.user_id

    def op2_read_user(self, user_id: str) -> Dict[str, Any]:
        row = self.session.execute("SELECT * FROM users WHERE user_id = %s", (user_id,)).one()
        return row._asdict() if row else None

    def op3_update_user_stats(self, user_id: str) -> bool:
        row = self.session.execute("SELECT followers FROM users WHERE user_id = %s", (user_id,)).one()
        if row:
            current = row.followers if row.followers is not None else 0
            new_val = current + 1
            self.session.execute("UPDATE users SET followers = %s WHERE user_id = %s", (new_val, user_id))
            return True
        return False 

    def op4_delete_activity(self, activity_id: str, user_id: str) -> bool:
        # Scan de partição 
        rows = self.session.execute("SELECT activity_id, ts FROM activities WHERE user_id = %s", (user_id,))
        target_ts = None
        for r in rows:
            if r.activity_id == activity_id:
                target_ts = r.ts
                break
        
        if target_ts:
            self.session.execute(
                "DELETE FROM activities WHERE user_id = %s AND ts = %s AND activity_id = %s",
                (user_id, target_ts, activity_id)
            )
            return True
        return False

    def op5_create_post_update_stats(self, user_id: str, content: str) -> str:
        new_id = str(uuid.uuid4())
        ts = int(time.time())
        payload = json.dumps({"content": content})
        self.session.execute(
            "INSERT INTO activities (user_id, ts, activity_id, type, payload) VALUES (%s, %s, %s, 'POST', %s)",
            (user_id, ts, new_id, payload)
        )
        row = self.session.execute("SELECT posts_count FROM users WHERE user_id = %s", (user_id,)).one()
        if row:
            current = row.posts_count if row.posts_count is not None else 0
            self.session.execute("UPDATE users SET posts_count = %s WHERE user_id = %s", (current + 1, user_id))
        return new_id

    def op6_get_feed(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        rows = self.session.execute("SELECT * FROM activities WHERE user_id = %s LIMIT %s", (user_id, limit))
        return [r._asdict() for r in rows]

    def op7_get_user_likes(self, user_id: str) -> List[Dict[str, Any]]:
        # Filtro no lado da aplicação 
        try:
            rows = self.session.execute("SELECT * FROM activities WHERE user_id = %s ALLOW FILTERING", (user_id,))
            # Filtramos no Python pois o ALLOW FILTERING com AND pode ser instável dependendo da versão
            return [r._asdict() for r in rows if r.type == 'LIKE'][:50]
        except: return []

    def op8_search_hashtag(self, hashtag: str) -> List[Dict[str, Any]]:
        # SCAN GLOBAL LIMITADO, não pode ser utilizado em produção
        # Usei essa técnica somente para conseguir fazer as comparações corretamente, 
        # mas o Cassandra não lida bem com esse tipo de query devido a sua estrutura de partições.
        found = []
        try:
            # ALLOW FILTERING é obrigatório aqui pois não estamos dando a Partition Key (user_id)
            rows = self.session.execute("SELECT * FROM activities LIMIT 1000 ALLOW FILTERING")
            for r in rows:
                if r.payload and hashtag in r.payload:
                    found.append(r._asdict())
                    if len(found) >= 20: break
        except Exception as e:
            print(f"Cassandra search error: {e}")
        return found

    def op9_aggregate_type_count(self, user_id: str) -> Dict[str, int]:
        rows = self.session.execute("SELECT type FROM activities WHERE user_id = %s", (user_id,))
        counts = {}
        for r in rows:
            counts[r.type] = counts.get(r.type, 0) + 1
        return counts

    def op10_schema_evolution(self) -> int:
        # 1. Alter Table 
        try:
            self.session.execute("ALTER TABLE users ADD verified BOOLEAN")
        except: 
            pass
        
        # 2. Update com Scan + Update individual (bem lento)
        count = 0
        # Seleciona todos os usuarios. Em bases gigantes precisaria de paginação manual com tokens.
        # Aqui assumi que cabe na memória para simplificar.
        rows = self.session.execute("SELECT user_id, followers FROM users")
        
        for r in rows:
            if r.followers and r.followers > 10000:
                self.session.execute("UPDATE users SET verified = true WHERE user_id = %s", (r.user_id,))
                count += 1
        return count


class RedisDb(AbstractSocialDb):
    def connect(self):
        self.conn = redis.Redis(host='localhost', port=6379, db=0)
        print("Redis conectado")

    def close(self): self.conn.close()

    def op1_create_user(self, data: SocialUserData) -> str:
        key = f"user:{data.user_id}"
        mapping = {
            "handle": data.handle, "title": data.title, "bio": data.bio,
            "followers": 0, "posts": 0
        }
        self.conn.hset(key, mapping=mapping)
        return data.user_id

    def op2_read_user(self, user_id: str) -> Dict[str, Any]:
        return _decode_redis(self.conn.hgetall(f"user:{user_id}"))

    def op3_update_user_stats(self, user_id: str) -> bool:
        return self.conn.hincrby(f"user:{user_id}", "followers", 1) > 0

    def op4_delete_activity(self, activity_id: str, user_id: str) -> bool:
        p = self.conn.pipeline()
        p.delete(f"activity:{activity_id}")
        p.lrem(f"timeline:{user_id}", 0, activity_id)
        res = p.execute()
        return res[0] > 0

    def op5_create_post_update_stats(self, user_id: str, content: str) -> str:
        new_id = str(uuid.uuid4())
        ts = int(time.time())
        payload = json.dumps({"content": content})
        
        pipe = self.conn.pipeline()
        pipe.hset(f"activity:{new_id}", mapping={"user_id": user_id, "type": "POST", "ts": ts, "payload": payload})
        pipe.lpush(f"timeline:{user_id}", new_id)
        pipe.hincrby(f"user:{user_id}", "posts", 1)
        pipe.execute()
        return new_id

    def op6_get_feed(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        ids = self.conn.lrange(f"timeline:{user_id}", 0, limit-1)
        if not ids: return []
        
        pipe = self.conn.pipeline()
        for i in ids:
            pipe.hgetall(f"activity:{i.decode('utf-8')}")
        res = pipe.execute()
        return [_decode_redis(r) for r in res if r]

    def op7_get_user_likes(self, user_id: str) -> List[Dict[str, Any]]:
        likes = []
        ids = self.conn.lrange(f"timeline:{user_id}", 0, 100)
        for i in ids:
            data = _decode_redis(self.conn.hgetall(f"activity:{i.decode('utf-8')}"))
            if data.get('type') == 'LIKE':
                likes.append(data)
        return likes

    def op8_search_hashtag(self, hashtag: str) -> List[Dict[str, Any]]:
        # IMPLEMENTAÇÃO FORÇADA: SCAN em todas as activities
        found = []
        for key in self.conn.scan_iter(match="activity:*"):
            val = self.conn.hmget(key, ["payload", "type"])
            payload_raw, type_raw = val[0], val[1]
            
            if payload_raw and type_raw:
                try:
                    p_str = payload_raw.decode('utf-8')
                    t_str = type_raw.decode('utf-8')
                    if t_str in ['POST', 'COMMENT'] and hashtag in p_str:
                        full_obj = _decode_redis(self.conn.hgetall(key))
                        found.append(full_obj)
                        if len(found) >= 20: break
                except: continue
        return found

    def op9_aggregate_type_count(self, user_id: str) -> Dict[str, int]:
        ids = self.conn.lrange(f"timeline:{user_id}", 0, -1)
        counts = {}
        for i in ids:
            typ = self.conn.hget(f"activity:{i.decode('utf-8')}", "type")
            if typ:
                t_str = typ.decode('utf-8')
                counts[t_str] = counts.get(t_str, 0) + 1
        return counts

    def op10_schema_evolution(self) -> int:
        count = 0
        # Varre todas as chaves que começam com "user:"
        for key in self.conn.scan_iter(match="user:*"):
            try:
                # Tenta ler o campo 'followers' assumindo que a chave é um HASH
                followers = self.conn.hget(key, "followers")
                
                if followers:
                    if int(followers) > 10000:
                        self.conn.hset(key, "verified", "true")
                        count += 1
                        
            except redis.exceptions.ResponseError:
                continue
            except ValueError:
                pass
                
        return count

if __name__ == "__main__":
    pg = PostgresDb()
    mongo = MongoDb()
    cass = CassandraDb()
    red = RedisDb()

    # garante pasta de resultados
    if not os.path.exists("./results"):
        os.makedirs("./results")

    # limpa arquivo de saída agregada
    with open("./results/OUT.txt", "w") as f:
        f.write("")

    test_user = SocialUserData("user_teste_123", "tester", "Testers", "Bio Teste")
    TARGET_USER_ID = "905f0b0a-1e3e-4fd3-823d-2f3fe5eaeefe"
    HASHTAG_TERM = "#Brasil"

    # lista de Dbs a serem testados
    for db in [pg, mongo, cass, red]:
        db.connect()
        name = db.__class__.__name__

        print(f"\n--- Testando {name} ---")

        try:
            result = db.run_all_queries(
                test_user=test_user,
                target_user_id=TARGET_USER_ID,
                hashtag_term=HASHTAG_TERM
            )

            timings = result.get("timings", {})
            total_time = result.get("total_time", sum(timings.values()))

            # Monta um dicionário só com tempos + total_time
            timings_only = dict(timings)
            timings_only["total_time"] = total_time

            # Salva apenas os tempos por banco em JSON
            with open(f"./results/timings_{name}.json", "w") as f:
                json.dump(timings_only, f, indent=4, default=str)

            # Cálculo simples de throughput: 10 operações (op1..op10)
            num_operations = 10
            throughput = num_operations / total_time if total_time > 0 else 0.0

            # Escreve linha resumida no OUT.txt
            with open("./results/OUT.txt", "a") as f:
                f.write(f"{name}: {total_time:.4f}s ({throughput:.2f} ops/sec)\n")

            print(f"fim. Tempo total: {total_time:.4f}s. Throughput: {throughput:.2f} ops/sec")

        except Exception as e:
            print(f"erro --> {e}")
            traceback.print_exc()
        finally:
            db.close()

            