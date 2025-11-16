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
- NoSQL: MongoDB, Cassandra, Redis e Neo4j

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

## Execução dos testes

### Problemas

#### 1. Modelagem de sistema de vendas possuindo clientes, produtos e items. Tarefa é organizar os clientes e seus pedidos.

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

#### Operações simples (CRUD)

1.  Buscar um cliente específico pelo seu ID.
2.  Adicionar um novo produto (item) ao catálogo.
3.  Atualizar o preço de um produto específico (pelo seu ID) para um novo valor.
4.  Deletar um pedido específico pelo seu ID.

#### Operações de busca e relações

5.  Listar todos os pedidos que tenham um status específico (ex: "cancelado").
6.  Listar todos os pedidos feitos em um determinado intervalo de datas (ex: no último mês).
7.  Dado um ID de cliente, encontrar todos os IDs dos pedidos que ele já fez.
8.  Dado um ID de pedido, buscar todos os itens, quantidades e preços unitários associados a ele.

#### Consultas Complexas

9.  Dado um ID de pedido específico, buscar o nome e o email do cliente que fez aquele pedido.
10. Contar quantos pedidos cada cliente fez e listar os 10 clientes com mais pedidos.

---

#### 2. Modelagem de acesso a itens vendidos em um e-commerce.

Itens do e-commerce possuem:
- id, nome, marca, categoria, especificacoes, data de atualização

PostgreSQL
```sql
CREATE TABLE item (
    id              BIGSERIAL PRIMARY KEY,
    nome            VARCHAR(120) NOT NULL,
    marca           VARCHAR(120) NOT NULL,
    categoria       VARCHAR(120) NOT NULL,
    especificacoes  JSONB,
    data_atualizacao TIMESTAMP NOT NULL DEFAULT NOW()
);
```

Cassandra
```cql
CREATE TABLE itens (
    item_id UUID PRIMARY KEY,
    nome TEXT,
    marca TEXT,
    categoria TEXT,
    especificacoes MAP<TEXT, TEXT>,
    data_atualizacao TIMESTAMP
);
```

Redis
```redis
HMSET item:<item_id> nome "<nome>" marca "<marca>" categoria "<categoria>" especificacoes "<especificacoes>" data_atualizacao "<data_atualizacao>"
``` 

MongoDB
```javascript
{
    _id: ObjectId("<item_id>"),
    nome: "<nome>",
    marca: "<marca>",
    categoria: "<categoria>",
    especificacoes: {
        //...
    },
    data_atualizacao: ISODate("<data_atualizacao>")
}
```
---

#### 3. Modelagem de perfis de usuários e feed de atividades em uma rede social.

Usuário (perfil) possui: 
- id, nome, e-mail, preferências, estatísticas, data de criação.

Atividade possui: 
- id, id do usuário, tipo, origem, conteúdo (payload), métricas, timestamp, chave de deduplicação.

PostgreSQL
```sql
CREATE TABLE users (
    user_id         UUID PRIMARY KEY,
    handle          VARCHAR(50) UNIQUE NOT NULL,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(150) UNIQUE NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    bio             TEXT,
    location        VARCHAR(100),
    avatar_url      TEXT,
    lang            VARCHAR(10),
    marketing       BOOLEAN DEFAULT TRUE,
    privacy_show_email    BOOLEAN DEFAULT FALSE,
    privacy_show_activity BOOLEAN DEFAULT TRUE,
    followers       INTEGER DEFAULT 0,
    following       INTEGER DEFAULT 0,
    posts           INTEGER DEFAULT 0,
    tags            TEXT[],
    schema_version  INTEGER DEFAULT 1
);

CREATE TABLE activities (
    activity_id     BIGSERIAL PRIMARY KEY,
    user_id         UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    ts              TIMESTAMPTZ NOT NULL,
    type            VARCHAR(50) NOT NULL,
    source          VARCHAR(50),
    payload         JSONB,
    likes           INTEGER DEFAULT 0,
    replies         INTEGER DEFAULT 0,
    visibility      VARCHAR(20) DEFAULT 'public',
    dedup_key       VARCHAR(255) UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Cassandra
```cql
CREATE TABLE users (
    user_id          UUID PRIMARY KEY,
    handle           TEXT,
    first_name       TEXT,
    last_name        TEXT,
    email            TEXT,
    created_at       TIMESTAMP,
    bio              TEXT,
    location         TEXT,
    avatar_url       TEXT,
    lang             TEXT,
    marketing        BOOLEAN,
    privacy_show_email    BOOLEAN,
    privacy_show_activity BOOLEAN,
    followers        INT,
    following        INT,
    posts            INT,
    tags             SET<TEXT>,
    schema_version   INT
);

CREATE TABLE activities (
    user_id          UUID,
    ts               TIMESTAMP,
    activity_id      TIMEUUID,
    type             TEXT,
    source           TEXT,
    payload          TEXT,
    likes            INT,
    replies          INT,
    visibility       TEXT,
    dedup_key        TEXT,
    created_at       TIMESTAMP,
    PRIMARY KEY ((user_id), ts, activity_id)
) WITH CLUSTERING ORDER BY (ts DESC);

CREATE TABLE user_by_email (
  email TEXT PRIMARY KEY,
  user_id UUID
);

CREATE TABLE user_by_handle (
  handle TEXT PRIMARY KEY,
  user_id UUID
);

CREATE TABLE activity_dedup (
  user_id  UUID,
  dedup_key TEXT,
  activity_id TIMEUUID,
  PRIMARY KEY ((user_id), dedup_key)
);

```

Redis
```redis
HSET user:<user_id> handle <user_handle> first_name <first_name> last_name <last_name> email <email> created_at <created_at> bio <bio> location <location> avatar_url <avatar_url> lang <language_code> marketing <boolean> privacy_show_email <boolean> privacy_show_activity <boolean> followers <int> following <int> posts <int> tags "<tag_1>,<tag_2>,..." schema_version <int>
HSET activity:<activity_id> user_id <user_id> ts <timestamp> created_at <insert_timestamp> type <activity_type> source <source> payload "<json_string>" likes <int> replies <int> visibility <visibility_level> dedup_key <unique_deduplication_key>
SETNX user:email:<email> <user_id>
SETNX user:handle:<handle> <user_id>
SETNX dedup:<user_id>:<dedup_key> <activity_id>
```

MongoDB
```javascript
{
  "_id": "<user_uuid>",
  "handle": "<user_handle>",
  "name": {
    "first": "<first_name>",
    "last": "<last_name>"
  },
  "email": "<email>",
  "createdAt": ISODate("<created_at>"),
  "profile": {
    "bio": "<bio>",
    "location": "<location>",
    "avatarUrl": "<avatar_url>"
  },
  "prefs": {
    "lang": "<language_code>",
    "marketing": <boolean>,
    "privacy": {
      "showEmail": <boolean>,
      "showActivity": <boolean>
    }
  },
  "stats": {
    "followers": <int>,
    "following": <int>,
    "posts": <int>
  },
  "tags": ["<tag_1>", "<tag_2>", "..."],
  "schemaVersion": <int>
}

{
  "_id": ObjectId("<activity_id>"),
  "userId": "<user_uuid>",
  "ts": ISODate("<timestamp>"),
  "type": "<activity_type>",
  "source": "<source>", 
  "payload": {
    "<key>": "<value>"
  },
  "metrics": {
    "likes": <int>,
    "replies": <int>
  },
  "visibility": "<visibility_level>",
  "dedupKey": "<unique_deduplication_key>"
  "createdAt": ISODate("<insert_timestamp>")
}
db.users.createIndex({ email: 1 }, { unique: true })
db.users.createIndex({ handle: 1 }, { unique: true })
db.activities.createIndex({ userId: 1, dedupKey: 1 }, { unique: true })
```

#### 4. Modelagem de sistema de IoT que monitora temperatura e umidade em milhões de sensores.

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

### Estrutura dos arquivos

`problemaX` - Contém o arquivo para cada problema X (1, 2, 3...), conectando com o banco, criando tabelas, inserindo dados e fazendo consultas.
`data` - Contém os arquivos CSV dentro da pasta de cada problema
`start_databases.sh` - Script para iniciar os containers Docker com os bancos que serão utilizados.
`requirements.txt` - Dependências Python necessárias para rodar os scripts em cada problema.


## Como rodar

1. Instale o Docker
2. Execute o script `start_databases.sh` para iniciar os containers com os bancos de dados.
3. Instale as dependências Python com `pip install -r requirements.txt`
4. Entre em cada projeto e execute os scripts conforme necessário.

### Problema 1
```bash
cd problema1
python3 -m venv venv # criar ambiente 
source venv/bin/activate # ativar ambiente
pip install -r requirements.txt # instalar dependências
python3 prepare_tables.py # criar tabelas
python3 populate_tables.py # popular tabelas
python3 queries.py # executar consultas e salvar resultados na pasta 'results'
```

