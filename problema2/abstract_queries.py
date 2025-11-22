from abc import ABC, abstractmethod
from typing import List, Dict, Any
import time

class FoodProductData:
    def __init__(self, id: str, nome: str, marca: str, categoria: str, energia: float):
        self.id = id
        self.nome = nome
        self.marca = marca
        self.categoria = categoria
        self.energia = energia

class AbstractFoodDb(ABC):
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
    def read_produto(self, produto_id: str) -> Dict[str, Any]:
        """
        Buscar um produto específico pelo seu código de barras (ID).
        Retorna um dicionário com os dados do produto ou None se não encontrado.
        """
        pass

    @abstractmethod
    def create_produto(self, data: FoodProductData) -> str:
        """
        Adicionar um novo produto com dados básicos e nutricionais.
        Retorna o ID do produto criado.
        """
        pass

    @abstractmethod
    def add_new_nutrient_vitamin_c(self, produto_id: str, vitamin_c_value: float) -> bool:
        """
        Adicionar um campo NOVO ('vitamina_c') a um produto existente. 
        Retorna True em sucesso, False em falha.
        """
        pass

    @abstractmethod
    def delete_produto(self, produto_id: str) -> bool:
        """
        Deletar um produto específico pelo seu ID.
        Retorna True em sucesso, False em falha.
        """
        pass

    # --- Buscas e Filtros ---

    @abstractmethod
    def get_batch_products(self, ids: List[str]) -> List[Dict[str, Any]]:
        """
        Buscar uma lista de produtos pelos seus IDs de uma vez.
        Retorna uma lista de dicionários com os dados dos produtos encontrados.
        """
        pass

    @abstractmethod
    def find_by_marca(self, marca: str) -> List[Dict[str, Any]]:
        """
        Listar produtos de uma marca específica.
        Retorna uma lista de dicionários com os dados dos produtos encontrados.
        """
        pass

    @abstractmethod
    def find_by_energia_range(self, min_val: float, max_val: float) -> List[Dict[str, Any]]:
        """
        Listar produtos com valor de energia entre um intervalo específico.
        Retorna uma lista de dicionários com os dados dos produtos encontrados.
        """
        pass

    @abstractmethod
    def find_products_with_calcium(self) -> List[Dict[str, Any]]:
        """
        Listar produtos que POSSUEM a informação  de 'Cálcio' preenchida.
        Retorna uma lista de dicionários com os dados dos produtos encontrados.
        """
        pass

    # --- Consultas Complexas ---

    @abstractmethod
    def search_by_name(self, partial_name: str) -> List[Dict[str, Any]]:
        """
        Buscar produtos que contenham uma palavra específica no nome .
        Retorna uma lista de dicionários com os dados dos produtos encontrados.
        """
        pass

    @abstractmethod
    def aggregate_avg_carbs_by_category(self) -> List[Dict[str, Any]]:
        """
        Calcular a média de carboidratos para cada categoria e listar as 5 com maior média.
        Retorna uma lista de dicionários com os dados agregados.
        """
        pass

    def run_all_queries(self, read_id: str, new_product: FoodProductData, batch_ids: list[str], filter_marca: str, filter_score: str, range_min: float, range_max: float, search_term: str):
        """
        Método pra rodar todas as queries, coletar resultado e medir tempo.
        """
        timings = {}
        results = {}
        
        start = time.perf_counter()
        results["read_produto"] = self.read_produto(read_id)
        timings["read_produto"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["create_produto"] = self.create_produto(new_product)
        timings["create_produto"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["add_new_nutrient_vitamin_c"] = self.add_new_nutrient_vitamin_c(new_product.id, 15.0)
        timings["add_new_nutrient_vitamin_c"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["get_batch_products"] = self.get_batch_products(batch_ids)
        timings["get_batch_products"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["find_by_marca"] = self.find_by_marca(filter_marca)
        timings["find_by_marca"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["find_by_energia_range"] = self.find_by_energia_range(range_min, range_max)
        timings["find_by_energia_range"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["find_products_with_calcium"] = self.find_products_with_calcium()
        timings["find_products_with_calcium"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["search_by_name"] = self.search_by_name(search_term)
        timings["search_by_name"] = time.perf_counter() - start
        
        start = time.perf_counter()
        results["aggregate_avg_carbs_by_category"] = self.aggregate_avg_carbs_by_category()
        timings["aggregate_avg_carbs_by_category"] = time.perf_counter() - start

        start = time.perf_counter()
        results["delete_produto"] = self.delete_produto(new_product.id)
        timings["delete_produto"] = time.perf_counter() - start

        return {
            "results": results,
            "timings": timings,
            "total_time": sum(timings.values())
        }