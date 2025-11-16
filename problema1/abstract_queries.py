from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any
import time

class ProductData:
    def __init__(self, id: str, nome: str, valor: float):
        self.id = id
        self.nome = nome
        self.valor = valor
        
class AbstractDb(ABC):
    """
    Classe base abstrata que define 10 consultas pra cada um dos SGBD (Postgres, Mongo, Cassandra, Redis)

    Ideia semelhante ao que está no artigo: A performance comparison of SQL and NoSQL databases
    """
    
    def __init__(self):
        self.conn = None

    @abstractmethod
    def connect(self):
        pass
        
    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def read_cliente(self, cliente_id: str) -> Dict[str, Any]:
        """
        Buscar um cliente específico pelo seu ID.
        Retorna um dicionário com os dados do cliente ou None se não encontrado.
        """
        pass

    @abstractmethod
    def create_produto(self, product_data: ProductData) -> str:
        """
        Adicionar um novo produto (item) ao catálogo.
        Recebe um objeto ProductData com dados (id, nome, valor)
        Retorna o ID do produto criado.
        """
        pass

    @abstractmethod
    def update_produto_preco(self, product_id: str, novo_preco: float) -> bool:
        """
        Atualizar o preço de um produto específico (pelo seu ID).
        Retorna True em sucesso, False em falha.
        """
        pass

    @abstractmethod
    def delete_pedido(self, order_id: str) -> bool:
        """
        Deletar um pedido específico pelo seu ID. Retorna True em sucesso, False em falha.
        """
        pass

    @abstractmethod
    def find_pedidos_por_status(self, status: str) -> List[Dict[str, Any]]:
        """
        Listar todos os pedidos que tenham um status específico.
        """
        pass

    @abstractmethod
    def find_pedidos_por_data(self, data_inicio: datetime, data_fim: datetime) -> List[Dict[str, Any]]:
        """
        Listar todos os pedidos feitos em um determinado intervalo de datas.
        """
        pass

    @abstractmethod
    def find_pedidos_por_cliente(self, cliente_id: str) -> List[Dict[str, Any]]:
        """
        Dado um ID de cliente, encontrar todos os pedidos que ele já fez.
        """
        pass
        
    @abstractmethod
    def find_itens_por_pedido(self, order_id: str) -> List[Dict[str, Any]]:
        """
        Dado um ID de pedido, buscar todos os itens, quantidades e preços unitários associados a ele.
        """
        pass

    @abstractmethod
    def find_cliente_por_pedido(self, order_id: str) -> Dict[str, Any]:
        """
        Dado um ID de pedido, buscar o nome e o email do cliente que fez aquele pedido.
        """
        pass

    @abstractmethod
    def get_top_10_clientes_por_pedidos(self) -> List[Dict[str, Any]]:
        """
        Contar pedidos por cliente e listar os 10 mais.
        """
        pass

    def run_all_queries(self, cliente_id: str, product_data: ProductData, order_id: str, status: str, data_inicio: datetime, data_fim: datetime):
        """
        Método pra rodar todas as queries, coletar resultado e medir tempo.
        """
        timings = {}
        results = {}
        
        start = time.perf_counter()
        results["read_cliente"] = self.read_cliente(cliente_id)
        timings["read_cliente"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["create_produto"] = self.create_produto(product_data)
        timings["create_produto"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["update_produto_preco"] = self.update_produto_preco(product_data.id, product_data.valor + 10.0)
        timings["update_produto_preco"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["find_pedidos_por_status"] = self.find_pedidos_por_status(status)
        timings["find_pedidos_por_status"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["find_pedidos_por_data"] = self.find_pedidos_por_data(data_inicio, data_fim)
        timings["find_pedidos_por_data"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["find_pedidos_por_cliente"] = self.find_pedidos_por_cliente(cliente_id)
        timings["find_pedidos_por_cliente"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["find_itens_por_pedido"] = self.find_itens_por_pedido(order_id)
        timings["find_itens_por_pedido"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["find_cliente_por_pedido"] = self.find_cliente_por_pedido(order_id)
        timings["find_cliente_por_pedido"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["delete_pedido"] = self.delete_pedido(order_id)
        timings["delete_pedido"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["get_top_10_clientes_por_pedidos"] = self.get_top_10_clientes_por_pedidos()
        timings["get_top_10_clientes_por_pedidos"] = time.perf_counter() - start

        return {
            "results": results,
            "timings": timings,
            "total_time": sum(timings.values())
        }