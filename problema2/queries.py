import psycopg2
import psycopg2.extras
from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis
import json
import os
from datetime import datetime
from typing import List, Dict, Any
from collections import Counter
import traceback

from abstract_queries import AbstractFoodDb, FoodProductData

def _decode_redis_hash(s: Dict[bytes, bytes]) -> Dict[str, Any]:
    return {k.decode('utf-8'): v.decode('utf-8') for k, v in s.items()}

class PostgresDb(AbstractFoodDb):
    """Implementação do PostgreSQL. Usa SQL, JOINs e GROUP BY."""

    def connect(self):
        try:
            self.conn = psycopg2.connect(host="localhost", port="5432", database="trabalho_bd", user="admin", password="admin", cursor_factory=psycopg2.extras.RealDictCursor)
            print("postgres conectado")
        except Exception as e:
            print(f"erro ao conectar ao Postgres: {e}")

    def close(self):
        if self.conn: 
            self.conn.close()
            print("postgres desconectado")

    def read_produto(self, produto_id: str) -> Dict[str, Any]:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM produto WHERE id = %s", (produto_id,))
            self.conn.commit()
            return cursor.fetchone()

    def create_produto(self, data: FoodProductData) -> str:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO produto (id, nome, marca, categoria, energia, data_atualizacao) 
                   VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING""",
                (data.id, data.nome, data.marca, data.categoria, data.energia, datetime.now())
            )
            self.conn.commit()
        return data.id

    def add_new_nutrient_vitamin_c(self, produto_id: str, vitamin_c_value: float) -> bool:
        # limitação do postgres nesse caso visto que não tem como alterar o schema dinamicamente
        # já que os dados são estruturados
        return False 

    def delete_produto(self, produto_id: str) -> bool:
        with self.conn.cursor() as cursor:
            cursor.execute("DELETE FROM produto WHERE id = %s", (produto_id,))
            self.conn.commit()
            return cursor.rowcount > 0

    def get_batch_products(self, ids: List[str]) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM produto WHERE id IN %s", (tuple(ids),))
            return cursor.fetchall()

    def find_by_marca(self, marca: str) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM produto WHERE marca = %s LIMIT 100", (marca,))
            return cursor.fetchall()

    def find_by_energia_range(self, min_val: float, max_val: float) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM produto WHERE energia BETWEEN %s AND %s LIMIT 100", (min_val, max_val))
            return cursor.fetchall()

    def find_products_with_calcium(self) -> List[Dict[str, Any]]:
        # essa consulta é só para lidar com dados invalidos. 
        # não existe a coluna calcium na tabela, logo não tem como fazer a consulta
        return []
    
    def search_by_name(self, partial_name: str) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM produto WHERE nome ILIKE %s LIMIT 100", (f"%{partial_name}%",))
            return cursor.fetchall()

    def aggregate_avg_carbs_by_category(self) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT categoria, AVG(carboidratos) as media 
                FROM produto WHERE carboidratos IS NOT NULL 
                GROUP BY categoria ORDER BY media DESC LIMIT 5
            """)
            return cursor.fetchall()


class MongoDb(AbstractFoodDb):
    """ Implementação do MongoDB. Usa documentos flexíveis, agregações nativas."""

    def connect(self):
        try:
            self.client = MongoClient("mongodb://admin:admin@localhost:27017/")
            self.db = self.client["trabalho_bd"]
            print("mongo conectado")
        except Exception as e:
            print(f"erro ao conectar ao MongoDB: {e}")

    def close(self):
        if self.client:
            self.client.close()
            print("mongo desconectado")

    def read_produto(self, produto_id: str) -> Dict[str, Any]:
        return self.db.produtos.find_one({"_id": produto_id})

    def create_produto(self, data: FoodProductData) -> str:
        doc = {"_id": data.id, "nome": data.nome, "marca": data.marca, "categoria": data.categoria, "nutrientes": {"energia": data.energia}}
        try: self.db.produtos.insert_one(doc)
        except: pass
        return data.id

    def add_new_nutrient_vitamin_c(self, produto_id: str, vitamin_c_value: float) -> bool:
        # diferente do postgres, mongo aceita qualquer campo novo facilmente
        res = self.db.produtos.update_one(
            {"_id": produto_id}, 
            {"$set": {"nutrientes.vitamina_c": vitamin_c_value}}
        )
        return res.acknowledged

    def delete_produto(self, produto_id: str) -> bool:
        res = self.db.produtos.delete_one({"_id": produto_id})
        return res.deleted_count > 0

    def get_batch_products(self, ids: List[str]) -> List[Dict[str, Any]]:
        return list(self.db.produtos.find({"_id": {"$in": ids}}))

    def find_by_marca(self, marca: str) -> List[Dict[str, Any]]:
        return list(self.db.produtos.find({"marca": marca}).limit(100))

    def find_by_energia_range(self, min_val: float, max_val: float) -> List[Dict[str, Any]]:
        return list(self.db.produtos.find({"nutrientes.energia": {"$gte": min_val, "$lte": max_val}}).limit(100))

    def find_products_with_calcium(self) -> List[Dict[str, Any]]:
        # novamente, diferente do postgres, mongo aceita campos dinâmicos
        return list(self.db.produtos.find({"nutrientes.calcio": {"$exists": True}}).limit(100))

    def search_by_name(self, partial_name: str) -> List[Dict[str, Any]]:
        return list(self.db.produtos.find({"nome": {"$regex": partial_name, "$options": "i"}}).limit(100))

    def aggregate_avg_carbs_by_category(self) -> List[Dict[str, Any]]:
        pipeline = [
            {"$match": {"nutrientes.carboidratos": {"$ne": None}}},
            {"$group": {"_id": "$categoria", "media": {"$avg": "$nutrientes.carboidratos"}}},
            {"$sort": {"media": -1}}, {"$limit": 5}
        ]
        return list(self.db.produtos.aggregate(pipeline))


class CassandraDb(AbstractFoodDb):
    """ Implementação do Cassandra. Usa tabelas wide-column, MAPs e buscas client-side."""
    
    def connect(self):
        try:
            self.conn = Cluster(['localhost'], port=9042)
            self.session = self.conn.connect('trabalho_bd')
            print("cassandra conectado")
        except Exception as e:
            print(f"erro ao conectar ao Cassandra: {e}")

    def close(self):
        if self.conn:
            self.conn.shutdown()
            print("cassandra desconectado")

    def read_produto(self, produto_id: str) -> Dict[str, Any]:
        row = self.session.execute("SELECT * FROM produtos WHERE produto_id = %s", (produto_id,)).one()
        return {
            "produto_id": row.produto_id,
            "categoria": row.categoria,
            "data_atualizacao": row.data_atualizacao,
            "marca": row.marca,
            "nome": row.nome,
            "nutrientes": row.nutrientes,
        }

    def create_produto(self, data: FoodProductData) -> str:
        self.session.execute(
            "INSERT INTO produtos (produto_id, nome, marca, categoria, nutrientes, data_atualizacao) VALUES (%s, %s, %s, %s, %s, %s)",
            (data.id, data.nome, data.marca, data.categoria, {'energia': data.energia}, datetime.now())
        )
        return data.id

    def add_new_nutrient_vitamin_c(self, produto_id: str, vitamin_c_value: float) -> bool:
        # novamente, possível adicionar chaves dinamicas facilmente
        self.session.execute("UPDATE produtos SET nutrientes['vitamina_c'] = %s WHERE produto_id = %s", (vitamin_c_value, produto_id))
        return True

    def delete_produto(self, produto_id: str) -> bool:
        self.session.execute("DELETE FROM produtos WHERE produto_id = %s", (produto_id,))
        return True

    def get_batch_products(self, ids: List[str]) -> List[Dict[str, Any]]:
        if not ids: return []
        query = f"SELECT * FROM produtos WHERE produto_id IN ({', '.join(['%s'] * len(ids))})"
        rows = self.session.execute(query, ids)
        return [row._asdict() for row in rows]

    def find_by_marca(self, marca: str) -> List[Dict[str, Any]]:
        # limitaçaõ do cassandra. não tem como fazer filtro direto sem criar um index secundário
        # então trago os dados e resolvo no python
        rows = self.session.execute("SELECT * FROM produtos")
        res = []
        for r in rows:
            if r.marca == marca:
                res.append(r._asdict())
                if len(res) >= 100: break
        return res

    def find_by_energia_range(self, min_val: float, max_val: float) -> List[Dict[str, Any]]:
        # mesma coisa acima
        rows = self.session.execute("SELECT * FROM produtos")
        res = []
        for r in rows:
            if r.nutrientes and 'energia' in r.nutrientes:
                if min_val <= r.nutrientes['energia'] <= max_val:
                    res.append((r._asdict()))
                    if len(res) >= 100: break
        return res

    def find_products_with_calcium(self) -> List[Dict[str, Any]]:
        # mesma coisa acima
        rows = self.session.execute("SELECT * FROM produtos")
        res = []
        for r in rows:
            if r.nutrientes and 'calcio' in r.nutrientes:
                res.append((r._asdict()))
                if len(res) >= 100: break
        return res

    def search_by_name(self, partial_name: str) -> List[Dict[str, Any]]:
        # mesma coisa acima
        rows = self.session.execute("SELECT * FROM produtos")
        res = []
        for r in rows:
            if r.nome and partial_name in r.nome:
                res.append((r._asdict()))
                if len(res) >= 100: break
        return res

    def aggregate_avg_carbs_by_category(self) -> List[Dict[str, Any]]:
        # mesma coisa acima
        rows = self.session.execute("SELECT categoria, nutrientes FROM produtos")
        sums = {}
        counts = {}
        for r in rows:
            cat = r.categoria
            if r.nutrientes and 'carboidratos' in r.nutrientes:
                val = r.nutrientes['carboidratos']
                sums[cat] = sums.get(cat, 0) + val
                counts[cat] = counts.get(cat, 0) + 1
        
        return sorted([{"cat": k, "avg": v/counts[k]} for k, v in sums.items()], key=lambda x: x['avg'], reverse=True)[:5]


class RedisDb(AbstractFoodDb):
    """ Implementação do Redis. Usa estrutura de dados chave-valor, Hashes, Sets e Sorted Sets."""
    
    def connect(self):
        try:
            self.conn = redis.Redis(host='localhost', port=6379, db=0)
            print("redis conectado")
        except Exception as e:
            print(f"erro ao conectar ao redis: {e}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            print("redis desconectado")

    def read_produto(self, produto_id: str) -> Dict[str, Any]:
        return _decode_redis_hash(self.conn.hgetall(f"item:{produto_id}"))

    def create_produto(self, data: FoodProductData) -> str:
        # operaçao um pouco complicada no redis
        pipe = self.conn.pipeline()
        key = f"item:{data.id}"
        pipe.hset(key, mapping={"nome": data.nome, "marca": data.marca, "categoria": data.categoria, "energia": str(data.energia)})
        pipe.sadd(f"idx:marca:{data.marca.lower()}", data.id)
        pipe.sadd(f"idx:categoria:{data.categoria.lower()}", data.id)
        pipe.zadd("idx:energia", {data.id: data.energia})
        pipe.execute()
        return data.id

    def add_new_nutrient_vitamin_c(self, produto_id: str, vitamin_c_value: float) -> bool:
        # novamente ao contrario do postgres, redis aceita campos dinamicos facilmente
        return self.conn.hset(f"item:{produto_id}", "vitamina_c", str(vitamin_c_value)) > 0

    def delete_produto(self, produto_id: str) -> bool:
        return self.conn.delete(f"item:{produto_id}") > 0

    def get_batch_products(self, ids: List[str]) -> List[Dict[str, Any]]:
        # ponto forte do redis
        if not ids: return []
        pipe = self.conn.pipeline()
        for i in ids:
            pipe.hgetall(f"item:{i}")
        results = pipe.execute()
        return [_decode_redis_hash(d) for d in results if d]

    def find_by_marca(self, marca: str) -> List[Dict[str, Any]]:
        # operação complicada pro redis, precisei trazer pro python
        ids = self.conn.smembers(f"idx:marca:{marca.lower()}")
        res = []
        for i in list(ids)[:100]:
            data = _decode_redis_hash(self.conn.hgetall(f"item:{i.decode('utf-8')}"))
            res.append(data)
        return res

    def find_by_energia_range(self, min_val: float, max_val: float) -> List[Dict[str, Any]]:
        ids = self.conn.zrangebyscore("idx:energia", min_val, max_val, start=0, num=100)
        pipe = self.conn.pipeline()
        for i in ids: pipe.hgetall(f"item:{i.decode('utf-8')}")
        return [_decode_redis_hash(d) for d in pipe.execute()]

    def find_products_with_calcium(self) -> List[Dict[str, Any]]:
        # operação complicada pro redis, precisei trazer pro python
        res = []
        for key in self.conn.scan_iter("item:*"):
            if self.conn.hexists(key, "calcio"): # Usando sodio
                res.append(_decode_redis_hash(self.conn.hgetall(key)))
                if len(res) >= 100: break
        return res

    def search_by_name(self, partial_name: str) -> List[Dict[str, Any]]:
        # mesmo caso acima
        res = []
        for key in self.conn.scan_iter("item:*"):
            nome = self.conn.hget(key, "nome")
            if nome and partial_name.encode() in nome:
                res.append(_decode_redis_hash(self.conn.hgetall(key)))
                if len(res) >= 100: break
        return res

    def aggregate_avg_carbs_by_category(self) -> List[Dict[str, Any]]:
        # mesma coisa acima
        sums = {}
        counts = {}
        for key in self.conn.scan_iter("item:*"):
            data = self.conn.hmget(key, ["categoria", "carboidratos"])
            cat = data[0]
            carb = data[1]
            if cat and carb:
                c_str = cat.decode('utf-8')
                try:
                    val = float(carb)
                    sums[c_str] = sums.get(c_str, 0) + val
                    counts[c_str] = counts.get(c_str, 0) + 1
                except: pass
        
        return sorted([{"cat": k, "avg": v/counts[k]} for k, v in sums.items()], key=lambda x: x['avg'], reverse=True)[:5]


if __name__ == "__main__":
    
    pg = PostgresDb()
    mongo = MongoDb()
    cass = CassandraDb()
    red = RedisDb()

    if not os.path.exists("./results"): os.makedirs("./results")

    for db in [pg, mongo, cass, red]:
        
        db.connect()
        name = db.__class__.__name__
        
        print(f"\n--- Testando {name} ---")
        
        try:
            results = db.run_all_queries(
                read_id="3017620422003",
                new_product=FoodProductData("9999999991", "Produto Benchmark 2", "MarcaX", "Snacks", 500.0),
                batch_ids=["9999999991", "0000000018883", "0011110017598"],
                filter_marca="Ferrero",
                filter_score="e",
                range_min=0,
                range_max=200,
                search_term="Choco"
            )
            
            with open(f"./results/results_{name}.json", "w") as f:
                json.dump(results, f, indent=4, default=str)
            print(f"fim. Tempo total: {results['total_time']:.4f}s")
        except Exception as e:
            print(f"erro --> {e}")
            traceback.print_exc()
        finally:
            db.close()