from abc import ABC, abstractmethod
from typing import List, Dict, Any
import time
import uuid
import datetime

class SocialUserData:
    def __init__(self, user_id: str, handle: str, title: str, bio: str):
        self.user_id = user_id
        self.handle = handle
        self.title = title
        self.bio = bio
        self.created_at = int(time.time())

class AbstractSocialDb(ABC):
    """
    Interface abstrata para o Benchmark de Rede Social.
    Define as 10 operações que devem ser implementadas por todos os SGBDs.
    """
    
    def __init__(self):
        self.conn = None

    @abstractmethod
    def connect(self): pass
        
    @abstractmethod
    def close(self): pass

    # --- Operações Simples ---

    @abstractmethod
    def op1_create_user(self, data: SocialUserData) -> str:
        """1. Criar um novo usuário."""
        pass

    @abstractmethod
    def op2_read_user(self, user_id: str) -> Dict[str, Any]:
        """2. Buscar um perfil pelo ID."""
        pass

    @abstractmethod
    def op3_update_user_stats(self, user_id: str) -> bool:
        """3. Atualizar as estatísticas do perfil (Incrementar followers em +1 atomicamente)."""
        pass

    @abstractmethod
    def op4_delete_activity(self, activity_id: str, user_id: str) -> bool:
        """4. Excluir uma atividade (Post/Comentário)."""
        pass

    @abstractmethod
    def op5_create_post_update_stats(self, user_id: str, content: str) -> str:
        """5. Criar um novo post E atualizar o contador de posts do usuário (Consistência/Atomicidade)."""
        pass

    # --- Buscas e Filtros ---

    @abstractmethod
    def op6_get_feed(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """6. Buscar o feed do usuário: trazer atividades ordenadas pelo tempo (desc)."""
        pass

    @abstractmethod
    def op7_get_user_likes(self, user_id: str) -> List[Dict[str, Any]]:
        """7. Filtrar todos os likes de um usuário específico."""
        pass

    @abstractmethod
    def op8_search_hashtag(self, hashtag: str) -> List[Dict[str, Any]]:
        """8. Buscar posts e comentários que contenham determinada hashtag no payload."""
        pass

    # --- Operações Complexas ---

    @abstractmethod
    def op9_aggregate_type_count(self, user_id: str) -> Dict[str, int]:
        """9. Calcular o total de interações agrupadas por tipo para um usuário."""
        pass

    @abstractmethod
    def op10_schema_evolution(self) -> int:
        """10. Adicionar campo 'verified: true' para usuários com > 10.000 seguidores."""
        pass

    def run_all_queries(self, test_user: SocialUserData, target_user_id: str, hashtag_term: str):
        """
        Executa a bateria de testes e mede o tempo.
        """
        timings = {}
        results = {}
        
        # 1. Create User
        start = time.perf_counter()
        self.op1_create_user(test_user)
        timings["op1_create_user"] = time.perf_counter() - start

        # 2. Read User
        start = time.perf_counter()
        results["op2_read_user"] = self.op2_read_user(test_user.user_id)
        timings["op2_read_user"] = time.perf_counter() - start

        # 3. Update Stats
        start = time.perf_counter()
        self.op3_update_user_stats(test_user.user_id)
        timings["op3_update_user_stats"] = time.perf_counter() - start

        # 5. Create Post (Transaction) 
        start = time.perf_counter()
        new_post_id = self.op5_create_post_update_stats(test_user.user_id, f"Post de teste com {hashtag_term}")
        timings["op5_create_post_update_stats"] = time.perf_counter() - start

        # 4. Delete Activity
        start = time.perf_counter()
        self.op4_delete_activity(new_post_id, test_user.user_id)
        timings["op4_delete_activity"] = time.perf_counter() - start

        # 6. Get Feed (Usando um usuário alvo que já tenha dados carregados)
        start = time.perf_counter()
        results["op6_get_feed"] = self.op6_get_feed(target_user_id)
        timings["op6_get_feed"] = time.perf_counter() - start

        # 7. Get Likes
        start = time.perf_counter()
        results["op7_get_user_likes"] = self.op7_get_user_likes(target_user_id)
        timings["op7_get_user_likes"] = time.perf_counter() - start

        # 8. Search Hashtag
        start = time.perf_counter()
        results["op8_search_hashtag"] = self.op8_search_hashtag(hashtag_term)
        timings["op8_search_hashtag"] = time.perf_counter() - start

        # 9. Aggregate
        start = time.perf_counter()
        results["op9_aggregate_type_count"] = self.op9_aggregate_type_count(target_user_id)
        timings["op9_aggregate_type_count"] = time.perf_counter() - start

        # 10. Schema Evolution
        start = time.perf_counter()
        results["op10_modified_docs"] = self.op10_schema_evolution()
        timings["op10_schema_evolution"] = time.perf_counter() - start

        return {
            "results": results,
            "timings": timings,
            "total_time": sum(timings.values())
        }