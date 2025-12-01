# SQL_x_NoSQL

Trabalho final desenvolvido para a disciplina de Banco de Dados Avançado - UFMG

O trabalho foi motivado pelos artigos: [A performance comparison of SQL and NoSQL databases](https://www.researchgate.net/publication/261079289_A_performance_comparison_of_SQL_and_NoSQL_databases?enrichId=rgreq-c3560ee5a91d852b2ed9781e45f0bf55-XXX&enrichSource=Y292ZXJQYWdlOzI2MTA3OTI4OTtBUzoyOTgwMjU3OTc4NjU0NzNAMTQ0ODA2NjI5NTU5MQ%3D%3D&el=1_x_3&_esc=publicationCoverPdf) e [Comparative case study: An evaluation of performance computacion between SQL and NoSQL databases](https://www.researchgate.net/publication/369173525_Comparative_Case_Study_An_Evaluation_of_Performance_Computation_Between_SQL_And_NoSQL_Database)

## Integrantes

- Francisco Aragão
- Gabriel Pains
- Iasmin Correa

## Descrição

A ideia do trabalho é comparar o desempenho de bancos de dados SQL e NoSQL em diferentes cenários de uso. Para isso, foram escolhidos diferentes SGBDs:

- SQL: PostgreSQL
- NoSQL: MongoDB, Cassandra e Redis

#### Objetivo

A ideia é avaliar o desempenho dos bancos de dados em diferentes tipos de problemas, evidenciando as vantagens e desvantagens de cada abordagem e qual cenário de uso é mais favorável para cada ferramenta. Esperamos encontrar aplicações especializadas em atividades que justifiquem suas escolhas de arquitetura, e não alguma solução que sirva para todos os cenários possíveis.

## Instalação das ferramentas

- PostgreSQL

Instalação pode ser feita pelo [site oficial](https://www.postgresql.org/download/), basta selecionar a plataforma desejada e seguir as instruções para iniciar o serviço.

Outra forma é usando o [Docker](https://hub.docker.com/_/postgres), bastando iniciar o container com o serviço.

- Redis

Instação direta pelo [site](https://redis.io/docs/latest/operate/oss_and_stack/install/) ou no [docker](https://hub.docker.com/_/redis).

- MongoDB

Instalação direta pelo [site](https://www.mongodb.com/docs/manual/installation/) ou no [docker](https://hub.docker.com/_/mongo).

- Cassandra

Instalação direta pelo [site](https://cassandra.apache.org/_/download.html) ou no [docker](https://hub.docker.com/_/cassandra).

Em todo caso, recomendamos o uso do Docker para facilitar a instalação e configuração dos serviços.

## Estrutura dos arquivos

`problemaX` - Contém o arquivo para cada problema X (1, 2, 3...), conectando com o banco, criando tabelas, inserindo dados e fazendo consultas.

`data` - Contém os arquivos CSV dentro da pasta de cada problema. **IMPORTANTE**: É necessário baixar os datasets e coloca-los na pasta `data` para realizar os testes. Os datasets estão referenciados na descrição de cada problema.

`start_databases.sh` - Script para iniciar os containers Docker com os bancos que serão utilizados.

`stop_databases.sh` - Script para parar os containers Docker e apagar os containers.

`requirements.txt` - Dependências Python necessárias para rodar os scripts em cada problema.

`check_db.py` - Script para verificar se os bancos estão funcionando corretamente.

## Como rodar

1. Instale o Docker
2. Execute o script `start_databases.sh` para iniciar os containers com os bancos de dados.
3. Instale as dependências Python com `pip install -r requirements.txt`
4. Execute o script `check_db.py` para verificar se os bancos estão funcionando corretamente.
5. Entre em cada pasta dos problemas e execute os scripts conforme necessário.

Assim, dentro de cada pasta dos problemas, basta fazer:

1. `python3 prepare_tables.py` para criar as tabelas.
2. `python3 populate_tables.py` para popular as tabelas.
3. `python3 queries.py` para executar as consultas.

Exemplo:

```bash
./start_databases.sh
./check_db.py
pip install -r requirements.txt
cd problema1 # poderia ser qualquer problema
# necessário estar com o ambiente virtual ativado e o Docker em execução
python3 prepare_tables.py # criar tabelas
python3 populate_tables.py # popular tabelas
python3 queries.py # executar consultas e salvar resultados na pasta 'results'
```

## Execução dos testes

### Problemas

### 1. Modelagem de sistema de vendas possuindo clientes, produtos e items. Tarefa é organizar os clientes e seus pedidos

[Database](https://www.kaggle.com/code/danttis/an-lise-explorat-ria-de-dados)

PostgreSQL

```sql
CREATE TABLE cliente (
  id            UUID PRIMARY KEY,
  nome          VARCHAR(120) NOT NULL,
  data          TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE item (
  id    UUID PRIMARY KEY,
  nome  VARCHAR(120) NOT NULL,
  valor NUMERIC(12,2) NOT NULL CHECK (valor >= 0)
);

CREATE TABLE pedido (
  id          UUID PRIMARY KEY,
  cliente_id  BIGINT NOT NULL REFERENCES cliente(id) ON DELETE CASCADE,
  data        TIMESTAMP NOT NULL DEFAULT NOW(),
  status      VARCHAR(24) NOT NULL DEFAULT 'pendente'
);

CREATE TABLE pedido_item (
  pedido_id  UUID NOT NULL REFERENCES pedido(id) ON DELETE CASCADE,
  item_id    UUID NOT NULL REFERENCES item(id),
  quantidade INT    NOT NULL CHECK (quantidade > 0),
  preco_unit NUMERIC(12,2) NOT NULL CHECK (preco_unit >= 0), -- preço no momento da compra
  PRIMARY KEY (pedido_id, item_id)
);
```

Cassandra

```cql
CREATE TABLE clientes (
    cliente_id UUID PRIMARY KEY,
    nome TEXT,
    email TEXT,
    data_cadastro TIMESTAMP
);

CREATE TABLE pedidos_por_cliente (
    cliente_id UUID,
    pedido_id UUID,
    data_pedido TIMESTAMP,
    valor_total DECIMAL,
    status TEXT,
    PRIMARY KEY (cliente_id, pedido_id)
) WITH CLUSTERING ORDER BY (pedido_id DESC);

CREATE TABLE itens_por_pedido (
    pedido_id UUID,
    item_id UUID,
    produto_nome TEXT,
    quantidade INT,
    preco_unitario DECIMAL,
    PRIMARY KEY (pedido_id, item_id)
);

```

Redis

```redis
HMSET cliente:<customer_id> nome "<nome>" email "<email>" data_cadastro "<data_cadastro>"

HMSET item:<product_id> nome "<nome>" valor "<valor>"

HMSET pedido:<order_id> cliente_id "<customer_id>" data_pedido "<data_pedido>" status "<status>"

HSET pedido_item:<order_id> <product_id> '{"quantidade": <quantidade>, "preco_unit": <preco_unit>}'
```

MongoDB

```javascript
{
    _id: ObjectId("<cliente_id>"),
    nome: "<nome>",
    data_cadastro: ISODate("<data_cadastro>")
}

{
    _id: ObjectId("<item_id>"), 
    nome: "<nome>",
    valor: <valor>
}

{
    _id: ObjectId("<pedido_id>"), 
    cliente_id: ObjectId("<cliente_id>"), 
    data_pedido: ISODate("<data_pedido>"),
    status: "<status>",
    itens: [ 
        {
            item_id: ObjectId("<item_id>"), 
            quantidade: <quantidade>,
            preco_unit: <preco_unit>
        },
        //...
    ]
}

```

As consultas que foram testadas tentam executar operações básicas em bancos de dados (CRUD) mas também explorar operações um pouco mais complexas. Vale destacar que algumas operações não foram possíveis em determinados bancos de dados, devido às limitações de cada projeto.

#### Operações simples

1. Buscar um cliente específico pelo seu ID.
2. Adicionar um novo produto (item) ao catálogo.
3. Atualizar o preço de um produto específico (pelo seu ID) para um novo valor.
4. Deletar um pedido específico pelo seu ID.

#### Operações de busca e relações

5. Listar todos os pedidos que tenham um status específico (ex: "cancelado").
6. Listar todos os pedidos feitos em um determinado intervalo de datas (ex: no último mês).
7. Dado um ID de cliente, encontrar todos os IDs dos pedidos que ele já fez.
8. Dado um ID de pedido, buscar todos os itens, quantidades e preços unitários associados a ele.

#### Consultas Complexas

9. Dado um ID de pedido específico, buscar o nome e o email do cliente que fez aquele pedido.
10. Contar quantos pedidos cada cliente fez e listar os 10 clientes com mais pedidos.

### Resultados de Performance

| Métrica | PostgreSQL | MongoDB | Cassandra | Redis |
| :--- | :--- | :--- | :--- | :--- |
| **Tempos de Execução (s)** | | | | |
| `read_cliente` | 0.0024 | 0.0005 | 0.0037 | 0.0002 |
| `create_produto` | 0.0040 | 0.0003 | - | 0.0002 |
| `update_produto_preco` | 0.0013 | 0.0005 | - | 0.0001 |
| `find_pedidos_por_status` | 0.0224 | 0.0210 | 0.2021 | 1.9273 |
| `find_pedidos_por_data` | 0.0011 | 0.0053 | 0.1480 | 1.7699 |
| `find_pedidos_por_cliente` | 0.0008 | 0.0056 | 0.0017 | 1.9101 |
| `find_itens_por_pedido` | 0.0003 | 0.0004 | 0.0018 | 0.0001 |
| `find_cliente_por_pedido` | 0.0006 | 0.0005 | 0.0949 | 0.0002 |
| `delete_pedido` | 0.0009 | 0.0003 | 0.0842 | 0.0002 |
| `get_top_10_clientes_por_pedidos` | 0.0042 | 0.0157 | 0.0905 | 1.6825 |
| **Total Time** | **0.0381** | **0.0500** | **0.6268** | **7.2905** |
| **Consumo de Recursos** | | | | |
| CPU (Antes) | 0.02% | 0.95% | 2.47% | 1.16% |
| CPU (Depois) | 0.02% | 1.04% | 2.33% | 1.09% |
| Memória (Antes) | 23.87MiB | 116.8MiB | 2.533GiB | 8.324MiB |
| Memória (Depois) | 49.36MiB | 283.5MiB | 2.71GiB | 35.14MiB |

Obs: A memória do Cassandra foi mais alta logo no ínicio foi isso foi definido durante a criação do container.

---
---
---

### 2. Modelagem de acesso a informações de produtos alimentícios, incluindo especificações nutricionais e ingredientes. Foco é recuperar rapidamente informações sobre produtos

[Database](https://www.kaggle.com/datasets/openfoodfacts/world-food-facts)

PostgreSQL

```sql
CREATE TABLE produto (
    id              VARCHAR(50) PRIMARY KEY,
    nome            VARCHAR(255),
    marca           VARCHAR(255),
    categoria       VARCHAR(255),
    energia         REAL,
    gordura         REAL,
    carboidratos    REAL,
    proteinas       REAL,
    fibras          REAL,
    sodio           REAL, 
    data_atualizacao TIMESTAMP
);
```

Cassandra

```cql
CREATE TABLE produtos (
    produto_id TEXT PRIMARY KEY,
    nome TEXT,
    marca TEXT,
    categoria TEXT,
    nutrientes MAP<TEXT, FLOAT>, -- Ex: {'energia': 500, 'gordura': 10}
    data_atualizacao TIMESTAMP
);
```

Redis

```redis
HMSET item:<id> nome "..." marca "..." categoria "..." energia "..." gordura "..."

# indices pra realizar buscas -> sem isso não da pra fazer no redis
SADD idx:marca:<marca> <id>    
SADD idx:categoria:<categoria> <id> 
ZADD idx:energia <energia> <id>  
```

MongoDB

```javascript
{
    _id: "<code_barras>",
    nome: "...",
    marca: "...",
    categoria: "...",
    nutrientes: {
        energia: 500,
        gordura: 10,
        ...
    },
    data_atualizacao: ISODate("...")
}
```

As consultas que foram testadas tentam executar operações básicas em bancos de dados (CRUD) mas também explorar operações um pouco mais complexas. No Redis foi necessário criar indíces para permitir a execução de algumas consultas (simular filtros). No postgress algumas consultas não foram possíveis devido à falta de suporte a esquemas dinamicos (até existe o JSONB, mas não foi utilizado para focar no modelo relacional tradicional).

#### Operações simples

1. Buscar um produto específico pelo seu código de barras (ID).
2. Adicionar um novo produto com dados básicos e nutricionais.
3. Adicionar um campo novo ("Vitamina C") a um produto existente
4. Deletar um produto específico pelo seu ID.

#### Operações de busca e filtros

5. Listar todos os produtos que pertencem a uma categoria específica
6. Listar produtos de uma marca específica que tenham NutriScore 'A'
7. Listar produtos com valor de energia entre um intervalo específico
8. Listar produtos que possuem a informação de "Cálcio" preenchida (teste de nulidade/existência).

#### Consultas Complexas

9. Buscar produtos que contenham uma palavra específica no nome
10. Calcular a média de carboidratos para cada categoria e listar as 5 categorias com maior média.

### Resultados de Performance

| Métrica | PostgreSQL | MongoDB | Cassandra | Redis |
| :--- | :--- | :--- | :--- | :--- |
| **Tempos de Execução (s)** | | | | |
| `read_produto` | 0.0079 | 0.0082 | 0.0053 | 0.0008 |
| `create_produto` | 0.0019 | 0.0003 | 0.0029 | 0.0002 |
| `add_new_nutrient_vitamin_c` | - | 0.0002 | 0.0026 | 0.0001 |
| `get_batch_products` | 0.0011 | 0.0003 | 0.0104 | 0.0002 |
| `find_by_marca` | 0.0136 | 0.0978 | 3.5024 | 0.0068 |
| `find_by_energia_range` | 0.0016 | 0.0010 | 0.0856 | 0.0022 |
| `find_products_with_calcium` | - | 0.1187 | 6.7507 | 19.2768 |
| `search_by_name` | 0.0029 | 0.0012 | 0.1149 | 0.1202 |
| `aggregate_avg_carbs_by_category` | 0.0255 | 0.1748 | 5.7546 | 21.5645 |
| `delete_produto` | 0.0013 | 0.0004 | 0.0013 | 0.0001 |
| **Total Time** | **0.0558** | **0.4029** | **16.2307** | **40.9718** |
| **Consumo de Recursos** | | | | |
| CPU (Antes) | 1.85% | 2.03% | 1.91% | 0.84% |
| CPU (Depois) | 0.10% | 0.82% | 1.59% | 1.03% |
| Memória (Antes) | 23.74MiB | 114MiB | 2.526GiB | 8.082MiB |
| Memória (Depois) | 82.83MiB | 365.1MiB | 2.806GiB | 175.7MiB |

Obs: Novamente o Cassandra teve essa discrepância de memória logo no início pois isso foi definido durante a criação do container.

---
---
---

### 3. Modelagem de perfis de usuários e feed de atividades em uma rede social

Usuário (perfil) possui:

- id, handle, title, bio, estatísticas, data de criação.

Atividade possui:

- id, id do usuário, tipo, timestamp, conteúdo (payload).

[Database](https://zenodo.org/records/13382873)

PostgreSQL

```sql
CREATE TABLE users (
    user_id       VARCHAR(255) PRIMARY KEY,
    handle        VARCHAR(255) NOT NULL,
    title         VARCHAR(255),
    bio           TEXT,
    created_at    BIGINT,
    followers     INTEGER DEFAULT 0,
    following     INTEGER DEFAULT 0,
    posts_count   INTEGER DEFAULT 0
);

CREATE TABLE activities (
    activity_id   VARCHAR(255) PRIMARY KEY,
    user_id       VARCHAR(255) NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    ts            BIGINT NOT NULL,
    type          VARCHAR(50) NOT NULL,
    payload       JSONB,
    created_at    BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())
);

CREATE UNIQUE INDEX idx_users_handle ON users(handle);
CREATE INDEX idx_activities_user ON activities(user_id);
CREATE INDEX idx_activities_ts ON activities(ts DESC);
CREATE INDEX idx_users_followers ON users(followers);
```

Cassandra

```cql
CREATE TABLE users (
    user_id       TEXT PRIMARY KEY,
    handle        TEXT,
    title         TEXT,
    bio           TEXT,
    created_at    BIGINT,
    followers     INT,
    following     INT,
    posts_count   INT
);

CREATE TABLE activities (
    user_id       TEXT,
    ts            BIGINT,
    activity_id   TEXT,
    type          TEXT,
    payload       TEXT,
    PRIMARY KEY ((user_id), ts, activity_id)
) WITH CLUSTERING ORDER BY (ts DESC);

CREATE TABLE user_by_handle (
  handle TEXT PRIMARY KEY,
  user_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_users_followers ON users (followers);
```

Redis

```redis
HSET user:<user_id> handle <user_handle> title <title> bio <bio> created_at <created_at_int> followers <int> following <int> posts <int>

HSET activity:<activity_id> user_id <user_id> ts <timestamp> type <activity_type> payload "<json_string_content_or_target_id>" created_at <insert_timestamp>

SETNX user:handle:<handle> <user_id>

LPUSH timeline:<user_id> <activity_id>
```

MongoDB

```javascript
{
  "_id": "<id_do_arquivo_koo>",
  "handle": "<user_handle>",
  "title": "<title>",
  "profile": {
    "bio": "<description>"
  },
  "createdAt": <timestamp_int>,
  "stats": {
    "followers": 0,
    "following": 0,
    "posts": 0
  }
}

{
  "_id": "<id_da_atividade>",
  "userId": "<creatorId_ou_liker_id>",
  "ts": <timestamp_int>,
  "type": "<POST|LIKE|COMMENT|SHARE>", 
  "payload": {
     // dinâmico, conforme o tipo
     "content": "<conteudo_do_post>",      // Se for POST/COMMENT
     "targetId": "<id_do_post_original>"   // Se for LIKE/SHARE/COMMENT
  }
}

db.users.createIndex({ handle: 1 }, { unique: true })
db.activities.createIndex({ userId: 1, ts: -1 })
db.users.create_index({stats.followers: 1})
```

Consultas implementadas:

#### Operações simples

1. Criar um novo usuário
2. Buscar um perfil pelo ID
3. Atualizar as estatísticas do perfil
4. Excluir uma atividade do usuário
5. Criar um novo post, atualizando as duas tabelas (atividades e usuários)

#### Operações de busca e filtros

6. Buscar o feed do usuário: trazer todas as atividades ordenadas pelo tempo
7. Filtrar todos os likes de um usuário
8. Buscar todos os posts e comentários que contenham determinada hashtag

#### Operações complexas

9. Calcular o total de interações agrupadas por tipo para um usuário
10. Adicionar um novo campo `verified: true` apenas para usuários que têm mais de 10.000 seguidores

### Resultados de Performance

| Métrica                             | PostgreSQL | MongoDB    | Cassandra   | Redis        |
| :---------------------------------- | :--------- | :--------- | :---------- | :----------- |
| **Tempos de Execução (s)**          |            |            |             |              |
| `op1_create_user`                   | 0.0605     | 0.0105     | 0.0035      | 0.0012       |
| `op2_read_user`                     | 0.0485     | 0.0357     | 0.0373      | 0.0003       |
| `op3_update_user_stats`             | 0.0292     | 0.0009     | 0.0120      | 0.0002       |
| `op5_create_post_update_stats`      | 0.0424     | 0.0010     | 0.0080      | 0.0004       |
| `op4_delete_activity`               | 0.0016     | 0.0005     | 0.0058      | 0.0003       |
| `op6_get_feed`                      | 0.0016     | 0.0011     | 0.0909      | 0.0012       |
| `op7_get_user_likes`                | 0.0005     | 0.0021     | 0.0372      | 0.0226       |
| `op8_search_hashtag`                | 0.0440     | 0.0065     | 0.1433      | 0.7024       |
| `op9_aggregate_type_count`          | 0.0158     | 0.0145     | 0.0193      | 0.2098       |
| `op10_schema_evolution`             | 0.0548     | 0.0010     | 29.6951     | 161.7963     |
| **Total Time**                      | **0.2990** | **0.0738** | **30.0522** | **162.7347** |
| **Operações por Segundo (ops/sec)** | 33.45      | 135.51     | 0.33        | 0.06         |
| **Consumo de Recursos (Antes)**     |            |            |             |              |
| CPU (Antes)                         | 2.01%      | 0.65%      | 3.59%       | 0.79%        |
| Memória (Antes)                     | 24.06MiB   | 110MiB     | 2.293GiB    | 6.301MiB     |
| **Consumo de Recursos (Depois)**    |            |            |             |              |
| CPU (Depois)                        | 0.20%      | 0.78%      | 3.23%       | 0.69%        |
| Memória (Depois)                    | 249.7MiB   | 1.579GiB   | 2.597GiB    | 2.981GiB     |


---
---
---

### 4. Modelagem de sistema de IoT que monitora temperatura e umidade em milhões de sensores

Cada sensor envia muitas leituras por minuto, há a escrita e leitura concorrente massiva de vários dados de sensores.

DadosSensor possui:

- id_sensor, data, horario, temperatura, humidade

PostgreSQL

```sql
CREATE TABLE sensors (
  id           text PRIMARY KEY,
  location     text,
  model        text,
  installed_at timestamptz
);

CREATE TABLE sensor_data (
  sensor_id   text        NOT NULL REFERENCES sensors(id),
  ts          timestamptz NOT NULL,
  temperature real,
  humidity    real,
  PRIMARY KEY (sensor_id, ts)
) PARTITION BY RANGE (ts);

-- exemplo de particionamento
CREATE TABLE sensor_data_2025_10
PARTITION OF sensor_data
FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

CREATE TABLE sensor_data_2025_11
PARTITION OF sensor_data
FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
```

Cassandra

```cql
CREATE TABLE sensor_data (
    sensor_id text,
    date date,
    timestamp timestamp,
    temperature float,
    humidity float,
    PRIMARY KEY ((sensor_id, date), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

Redis

```redis
TS.CREATE ts:{sensor_42}:temperature RETENTION 7776000000 LABELS sensor_id sensor_42 metric temperature
TS.CREATE ts:{sensor_42}:humidity    RETENTION 7776000000 LABELS sensor_id sensor_42 metric humidity

TS.CREATE ts:{sensor_42}:temperature:agg:1h RETENTION 31536000000 LABELS sensor_id sensor_42 metric temperature agg 1h

TS.CREATERULE ts:{sensor_42}:temperature ts:{sensor_42}:temperature:agg:1h AGGREGATION avg 3600000
```

MongoDB

```javascript
{
  "_id": { "$oid": "..." },
  "sensor_id": "sensor_42",
  "ts": ISODate("2025-10-30T10:32:15Z"),
  "temperature": 24.8,
  "humidity": 55.2,
  "meta": { "firmware": "1.4.2" }
}

db.sensorDataClassic.createIndex({ sensor_id: 1, ts: -1 });

db.sensorDataClassic.createIndex({ ts: 1 }, { expireAfterSeconds: 90 * 24 * 3600 });
```
