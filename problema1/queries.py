import psycopg2
import psycopg2.extras
from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import Counter
import traceback

from abstract_queries import AbstractDb, ProductData

def _decode_redis_hash(s: Dict[bytes, bytes]) -> Dict[str, Any]:
    """Converte um hash do Redis (bytes) para um dict (str)."""
    return {k.decode('utf-8'): v.decode('utf-8') for k, v in s.items()}

def _mongo_fix_id(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Muda a chave '_id' do MongoDB para 'id'."""
    if doc and '_id' in doc:
        doc['id'] = doc.pop('_id')
    return doc

class PostgresDb(AbstractDb):
    """Implementação do PostgreSQL. Usa SQL, JOINs e GROUP BY."""
    
    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host="localhost",
                port="5432",
                database="trabalho_bd",
                user="admin",
                password="admin",
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            print("postgres conectado")
        except Exception as e:
            print(f"erro ao conectar ao postgresql: {e}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            print("postgres desconectado")

    def read_cliente(self, cliente_id: str) -> Optional[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM cliente WHERE id = %s", (cliente_id,))
            return cursor.fetchone()

    def create_produto(self, product_data: ProductData) -> str:
        with self.conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO item (id, nome, valor) VALUES (%s, %s, %s) RETURNING id",
                (product_data.id, product_data.nome, product_data.valor)
            )
            self.conn.commit()
            return cursor.fetchone()['id']

    def update_produto_preco(self, product_id: str, novo_preco: float) -> bool:
        with self.conn.cursor() as cursor:
            cursor.execute(
                "UPDATE item SET valor = %s WHERE id = %s",
                (novo_preco, product_id)
            )
            self.conn.commit()
            return cursor.rowcount > 0

    def delete_pedido(self, order_id: str) -> bool:
        with self.conn.cursor() as cursor:
            cursor.execute("DELETE FROM pedido WHERE id = %s", (order_id,))
            self.conn.commit()
            return cursor.rowcount > 0

    def find_pedidos_por_status(self, status: str) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM pedido WHERE status = %s", (status,))
            return cursor.fetchall()

    def find_pedidos_por_data(self, data_inicio: datetime, data_fim: datetime) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM pedido WHERE data BETWEEN %s AND %s", (data_inicio, data_fim))
            return cursor.fetchall()

    def find_pedidos_por_cliente(self, cliente_id: str) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM pedido WHERE cliente_id = %s", (cliente_id,))
            return cursor.fetchall()

    def find_itens_por_pedido(self, order_id: str) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM pedido_item WHERE pedido_id = %s", (order_id,))
            return cursor.fetchall()

    def find_cliente_por_pedido(self, order_id: str) -> Optional[Dict[str, Any]]:
        query = """
        SELECT c.id, c.nome, c.email
        FROM cliente c
        JOIN pedido p ON c.id = p.cliente_id
        WHERE p.id = %s
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, (order_id,))
            return cursor.fetchone()

    def get_top_10_clientes_por_pedidos(self) -> List[Dict[str, Any]]:
        query = """
        SELECT cliente_id, COUNT(id) AS total_pedidos
        FROM pedido
        GROUP BY cliente_id
        ORDER BY total_pedidos DESC
        LIMIT 10
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()
    

class MongoDb(AbstractDb):
    """Implementação do MongoDB. Usa busca de documentos, $lookup e $group."""
    
    def connect(self):
        try:
            self.conn = MongoClient("mongodb://admin:admin@localhost:27017/")
            self.conn.admin.command('ping') # Testa a conexão
            self.db = self.conn["trabalho_bd"]
            print("mongodb conectado")
        except Exception as e:
            print(f"erro ao conectar ao mongodb: {e}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            print("mongodb desconectado")

    def read_cliente(self, cliente_id: str) -> Dict[str, Any]:
        doc = self.db.clientes.find_one({"_id": cliente_id})
        return _mongo_fix_id(doc)

    def create_produto(self, product_data: ProductData) -> str:
        data_with_id = {
            "_id": product_data.id, 
            "nome": product_data.nome,
            "valor": product_data.valor
        }
        result = self.db.itens.insert_one(data_with_id)
        return result.inserted_id

    def update_produto_preco(self, product_id: str, novo_preco: float) -> bool:
        result = self.db.itens.update_one(
            {"_id": product_id},
            {"$set": {"valor": novo_preco}}
        )
        return result.modified_count > 0

    def delete_pedido(self, order_id: str) -> bool:
        # O banco ja remove os itens junto com o pedido pois o modelo foi feito junto
        result = self.db.pedidos.delete_one({"_id": order_id})
        return result.deleted_count > 0

    def find_pedidos_por_status(self, status: str) -> List[Dict[str, Any]]:
        cursor = self.db.pedidos.find({"status": status})
        return [_mongo_fix_id(doc) for doc in cursor]

    def find_pedidos_por_data(self, data_inicio: datetime, data_fim: datetime) -> List[Dict[str, Any]]:
        cursor = self.db.pedidos.find({
            "data_pedido": {"$gte": data_inicio, "$lt": data_fim}
        })
        return [_mongo_fix_id(doc) for doc in cursor]

    def find_pedidos_por_cliente(self, cliente_id: str) -> List[Dict[str, Any]]:
        cursor = self.db.pedidos.find({"cliente_id": cliente_id})
        return [_mongo_fix_id(doc) for doc in cursor]

    def find_itens_por_pedido(self, order_id: str) -> List[Dict[str, Any]]:
        # os dados foram modelados com pedidos e itens juntos, ai só pego o array de itens
        doc = self.db.pedidos.find_one(
            {"_id": order_id},
            {"itens": 1, "_id": 0}
        )
        return doc.get('itens', []) if doc else []

    def find_cliente_por_pedido(self, order_id: str) -> Dict[str, Any]:
        # implementação de "JOIN" 
        pipeline = [
            {"$match": {"_id": order_id}},
            {"$lookup": {
                "from": "clientes",
                "localField": "cliente_id",
                "foreignField": "_id",
                "as": "cliente_info"
            }},
            {"$unwind": "$cliente_info"}, # converte array em objeto
            {"$replaceRoot": {"newRoot": "$cliente_info"}} # retorna só o cliente
        ]
        result = list(self.db.pedidos.aggregate(pipeline))
        return _mongo_fix_id(result[0])

    def get_top_10_clientes_por_pedidos(self) -> List[Dict[str, Any]]:
        # implementando "GROUP BY"
        pipeline = [
            {"$group": {
                "_id": "$cliente_id",
                "total_pedidos": {"$sum": 1}
            }},
            {"$sort": {"total_pedidos": -1}}, # -1 = DESC
            {"$limit": 10},
            {"$project": { 
                "_id": 0,
                "cliente_id": "$_id",
                "total_pedidos": "$total_pedidos"
            }}
        ]
        return list(self.db.pedidos.aggregate(pipeline))
    

class CassandraDb(AbstractDb):
    """Implementação do Cassandra. Otimizado para buscas diretas por chaves, mas dificil lidar com queries maiores"""

    def connect(self):
        try:
            self.conn = Cluster(['localhost'], port=9042)
            self.session = self.conn.connect('trabalho_bd')
            print("cassandra conectado.")
        except Exception as e:
            print(f"erro ao conectar ao cassandra: {e}")
            raise
    
    def close(self):
        if self.conn:
            self.conn.shutdown()
            print("cassandra desconectado.")

    def read_cliente(self, cliente_id: str) -> Dict[str, Any]:
        row = self.session.execute(
            "SELECT * FROM clientes WHERE cliente_id = %s", (cliente_id,)
        ).one()
        return {
            "id": row.cliente_id, 
            "nome": row.nome,
            "email": row.email,
            "data_cadastro": row.data_cadastro
        }

    def create_produto(self, product_data: ProductData) -> str:
        # limitação do cassandra. A ideia não é ter os dados separados, mas sim ter uma tabela
        # que responde diretamente as queries necessárias. Como a modelagem não foi feita com "produtos"
        # separados, não da pra implementar essa função direito.
        return ""

    def update_produto_preco(self, product_id: str, novo_preco: float) -> bool:
        # mesma coisa acima
        return False

    def delete_pedido(self, order_id: str) -> bool:
        # não é muito comum fazer essa operação com a tabela atual,
        # deletar um pedido é ruim pois não temos uma tabela separada, mas da pra fazer
        # acabo precisando trazer os dados para serem processados no python
        try:
            self.session.execute(
                "DELETE FROM itens_por_pedido WHERE pedido_id = %s", (order_id,)
            )
            
            rows = self.session.execute("SELECT cliente_id, pedido_id FROM pedidos_por_cliente")
            cliente_id_para_deletar = None
            for row in rows:
                if row.pedido_id == order_id:
                    cliente_id_para_deletar = row.cliente_id
                    break
            
            if cliente_id_para_deletar:
                self.session.execute(
                    "DELETE FROM pedidos_por_cliente WHERE cliente_id = %s AND pedido_id = %s",
                    (cliente_id_para_deletar, order_id)
                )
            return True
        except Exception as e:
            print(f"Erro ao deletar pedido no Cassandra: {e}")
            return False

    def find_pedidos_por_status(self, status: str) -> List[Dict[str, Any]]:
        # novamente preciso lidar com os dados no python
        rows = self.session.execute("SELECT * FROM pedidos_por_cliente")
        pedidos_filtrados = []
        for row in rows:
            if row.status == status:
                pedidos_filtrados.append({
                    "cliente_id": row.cliente_id,
                    "pedido_id": row.pedido_id,
                    "data_pedido": row.data_pedido,
                    "status": row.status
                })
        return pedidos_filtrados

    def find_pedidos_por_data(self, data_inicio: datetime, data_fim: datetime) -> List[Dict[str, Any]]:
        # mesmo caso da query acima
        rows = self.session.execute("SELECT * FROM pedidos_por_cliente")
        pedidos_filtrados = []
        for row in rows:
            if data_inicio <= row.data_pedido < data_fim:
                pedidos_filtrados.append({
                    "cliente_id": row.cliente_id,
                    "pedido_id": row.pedido_id,
                    "data_pedido": row.data_pedido,
                    "status": row.status
                })
        print(f"Cassandra: Scan concluído, {len(pedidos_filtrados)} encontrados.")
        return pedidos_filtrados

    def find_pedidos_por_cliente(self, cliente_id: str) -> List[Dict[str, Any]]:
        # ja nessa consulta foi possível executar perfeitamente
        # o modelo foi construido para evitar "JOINs" com a informação salva junta
        rows = self.session.execute(
            "SELECT * FROM pedidos_por_cliente WHERE cliente_id = %s", (cliente_id,)
        )
        return [{
            "cliente_id": row.cliente_id,
            "pedido_id": row.pedido_id,
            "data_pedido": row.data_pedido,
            "status": row.status
        } for row in rows]

    def find_itens_por_pedido(self, order_id: str) -> List[Dict[str, Any]]:
        # mesmo caso acima
        rows = self.session.execute(
            "SELECT * FROM itens_por_pedido WHERE pedido_id = %s", (order_id,)
        )
        return [{
            "pedido_id": row.pedido_id,
            "item_id": row.item_id,
            "quantidade": row.quantidade,
            "preco_unitario": row.preco_unitario
        } for row in rows]

    def find_cliente_por_pedido(self, order_id: str) -> Dict[str, Any]:
        # precisei de novo processar no python
        rows = self.session.execute("SELECT cliente_id, pedido_id FROM pedidos_por_cliente")
        cliente_id_encontrado = ""
        for row in rows:
            if row.pedido_id == order_id:
                cliente_id_encontrado = row.cliente_id
                break
        
        return self.read_cliente(cliente_id_encontrado)

    def get_top_10_clientes_por_pedidos(self) -> List[Dict[str, Any]]:
        # mesmo caso acima
        rows = self.session.execute("SELECT cliente_id FROM pedidos_por_cliente")
        
        contagem = Counter()
        for row in rows:
            contagem[row.cliente_id] += 1
        
        return [
            {"cliente_id": cid, "total_pedidos": total}
            for cid, total in contagem.most_common(10)
        ]

class RedisDb(AbstractDb):
    """Implementação do Redis. Rápido para chaves, lento para scans."""

    def connect(self):
        try:
            self.conn = redis.Redis(host='localhost', port=6379, db=0)
            self.conn.ping()
            print("redis conectado.")
        except Exception as e:
            print(f"erro ao conectar ao redis: {e}")
            raise
    
    def close(self):
        if self.conn:
            self.conn.close()
            print("redis desconectado.")

    def read_cliente(self, cliente_id: str) -> Dict[str, Any]:
        data = self.conn.hgetall(f"cliente:{cliente_id}")
        return _decode_redis_hash(data)

    def create_produto(self, product_data: ProductData) -> str:
        key = f"item:{product_data.id}"
        self.conn.hmset(key, {
            "nome": product_data.nome,
            "valor": product_data.valor
        })
        return product_data.id

    def update_produto_preco(self, product_id: str, novo_preco: float) -> bool:
        key = f"item:{product_id}"
        return self.conn.hset(key, "valor", novo_preco) > 0

    def delete_pedido(self, order_id: str) -> bool:
        # uso pipeline pois executo as duas operaçoes juntas
        pipe = self.conn.pipeline()
        pipe.delete(f"pedido:{order_id}")
        pipe.delete(f"pedido_item:{order_id}")
        results = pipe.execute()
        return sum(results) > 0 # retorna true se pelo menos 1 chave foi deletada

    def find_pedidos_por_status(self, status: str) -> List[Dict[str, Any]]:
        # aqui ja fica complicado de fazer com o redis
        # no fim das contas eu trago os dados e lido com eles manualmente no Python
        # não vi como fazer isso no redis ...
        pedidos = []
        for key in self.conn.scan_iter("pedido:*"):
            data = self.conn.hgetall(key)
            if data.get(b'status') == status.encode('utf-8'):
                pedidos.append(_decode_redis_hash(data))
        return pedidos

    def find_pedidos_por_data(self, data_inicio: datetime, data_fim: datetime) -> List[Dict[str, Any]]:
        # mesmo caso que ocorreu com a query acima
        pedidos = []
        for key in self.conn.scan_iter("pedido:*"):
            data = self.conn.hgetall(key)
            data_pedido_str = data.get(b'data_pedido')
            if data_pedido_str:
                data_pedido = datetime.fromisoformat(data_pedido_str.decode('utf-8'))
                if data_inicio <= data_pedido < data_fim:
                    pedidos.append(_decode_redis_hash(data))
        return pedidos

    def find_pedidos_por_cliente(self, cliente_id: str) -> List[Dict[str, Any]]:
        # mesmo caso acima, tive que processar no python
        pedidos = []
        for key in self.conn.scan_iter("pedido:*"):
            data = self.conn.hgetall(key)
            if data.get(b'cliente_id') == cliente_id.encode('utf-8'):
                pedidos.append(_decode_redis_hash(data))
        return pedidos

    def find_itens_por_pedido(self, order_id: str) -> List[Dict[str, Any]]:
        key = f"pedido_item:{order_id}"
        data = self.conn.hgetall(key)
        itens = []
        for item_id, json_str in data.items():
            item_data = json.loads(json_str.decode('utf-8'))
            itens.append({
                "item_id": item_id.decode('utf-8'),
                "quantidade": item_data['quantidade'],
                "preco_unit": item_data['preco_unit']
            })
        return itens

    def find_cliente_por_pedido(self, order_id: str) -> Dict[str, Any]:
        pedido_key = f"pedido:{order_id}"
        cliente_id_b = self.conn.hget(pedido_key, "cliente_id")
        
        cliente_id = cliente_id_b.decode('utf-8')
        return self.read_cliente(cliente_id)

    def get_top_10_clientes_por_pedidos(self) -> List[Dict[str, Any]]:
        # tive que novamente processar no python
        contagem = Counter()
        for key in self.conn.scan_iter("pedido:*"):
            cliente_id_b = self.conn.hget(key, "cliente_id")
            if cliente_id_b:
                contagem[cliente_id_b.decode('utf-8')] += 1
        
        return [
            {"cliente_id": cid, "total_pedidos": total}
            for cid, total in contagem.most_common(10)
        ]
    
if __name__ == "__main__":

    postgress = PostgresDb()
    mongo = MongoDb()
    cassandra = CassandraDb()
    redis_db = RedisDb()

    for db in [postgress, mongo, cassandra, redis_db]:
        print(f"\n--- Testando {db.__class__.__name__} ---")

        db.connect()

        try:
            results = db.run_all_queries(
                cliente_id="9613",
                product_data=ProductData(id="9999999.0", nome="Produto Teste", valor=99.99),
                order_id="10143.0",
                status="Pago",
                data_inicio=datetime(2023, 1, 1),
                data_fim=datetime(2023, 12, 31)
            )

            with open(f"./results/results_{db.__class__.__name__}.json", "w") as f:
                json.dump(results, f, default=str, indent=4)
            print(f"fim. Tempo total: {results['total_time']:.4f}s")
        except Exception as e:
            print(f"erro --> {e}")
            traceback.print_exc()
        finally:
            db.close()
