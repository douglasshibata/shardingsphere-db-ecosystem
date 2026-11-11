# Apache ShardingSphere 5.x — Ecossistema de Banco de Dados Distribuído e Alta Disponibilidade

Este diretório contém a arquitetura completa, infraestrutura declarativa baseada em containers (`docker-compose.yml`), especificações de governança de dados (`server.yaml` / `global.yaml` e `database-sharding.yaml`), scripts SQL de inicialização física e uma suite de testes automatizada para validar um ecossistema de banco de dados distribuído de alta disponibilidade baseado no **Apache ShardingSphere 5.x**.

---

## 🏛️ 1. Arquitetura do Sistema e Conceitos do ShardingSphere

### Diagrama ASCII da Arquitetura do Ecossistema

```text
                  +----------------------------------------------+
                  |           Camada de Aplicação                |
                  |  (Java/Spring, Python, Node.js, psql/mysql)   |
                  +----------------------------------------------+
                                         |
                                         | (Conexão única transparente via JDBC/TCP)
                                         v
                  +----------------------------------------------+
                  |        Apache ShardingSphere-Proxy 5.x       |
                  |            Porta Única: 5432 / 3307          |
                  +----------------------------------------------+
                         |                               |
        (Leitura/Escrita |                               | (Coordenação e Sincronização
         e Criptografia) |                               |  de Metadados e Topologia)
                         v                               v
         +-------------------------------+     +-------------------------------+
         |       SQL Parse Engine        |     |       Governance Center       |
         |       SQL Router Engine       |     |      (Apache ZooKeeper)       |
         |      SQL Rewrite Engine       |     |          Porta: 2181          |
         |      SQL Execution Engine     |     +-------------------------------+
         |         Merge Engine          |
         +-------------------------------+
                         |
                         +------------------------+------------------------+
                         | (Shard 0)                                       | (Shard 1)
                         v                                                 v
           +---------------------------+                     +---------------------------+
           |       Grupo ds_0          |                     |       Grupo ds_1          |
           | (Read-Write Splitting)    |                     | (Read-Write Splitting)    |
           +---------------------------+                     +---------------------------+
             |                       |                         |                       |
             v (Escritas)            v (Leituras)              v (Escritas)            v (Leituras)
     +---------------+       +---------------+         +---------------+       +---------------+
     | ds_0_primary  |       | ds_0_replica  |         | ds_1_primary  |       | ds_1_replica  |
     | (Primary Node)| ----> | (Replica Node)|         | (Primary Node)| ----> | (Replica Node)|
     |  Porta: 5433  | Sync  |  Porta: 5434  |         |  Porta: 5435  | Sync  |  Porta: 5436  |
     +---------------+       +---------------+         +---------------+       +---------------+
```

---

### ShardingSphere-Proxy vs ShardingSphere-JDBC

O Apache ShardingSphere oferece duas abordagens arquiteturais principais para integração com aplicações:

| Critério | ShardingSphere-Proxy | ShardingSphere-JDBC |
| :--- | :--- | :--- |
| **Modelo de Implantação** | Proxy independente operando como um servidor/processo de banco de dados intermediário. | Biblioteca/Framework embarcado (Embedded) acoplado diretamente dentro da aplicação JVM. |
| **Independência de Linguagem**| **Totalmente independente**. Qualquer stack (Python, Go, Node.js, Java) conecta via driver TCP nativo. | Restrito ao ecossistema Java (Spring Data JPA, MyBatis, Hibernate, Kotlin, etc.). |
| **Desempenho (Overhead)** | Introduz um salto extra na rede (*network hop*) e latência marginal devido ao parsing centralizado. | Latência extremamente baixa, pois a inteligência de roteamento e parse roda na mesma CPU do app. |
| **Perfis e Operação** | Ideal para DBAs, Ops e equipes heterogêneas. Facilita auditoria centralizada e Online DDLs. | Excelente para desenvolvedores que desejam microserviços autocontidos de altíssima performance. |
| **Uso de Recursos** | Requer infraestrutura própria e dedicada para escalar a camada de Proxy horizontalmente. | Consome a memória RAM e CPU do próprio servidor do microserviço. |

---

### Conceitos Fundamentais do ShardingSphere

1. **Logic Table (Tabela Lógica):** A visão lógica e unificada que o desenvolvedor enxerga ao consultar o ShardingSphere (ex: `orders`). Para a aplicação, existe apenas uma única tabela contínua.
2. **Actual Table (Tabela Física / Real):** As partições físicas reais de dados instanciadas dentro dos Data Nodes (ex: `orders_0`, `orders_1` em `ds_0` e `ds_1`).
3. **Data Node (Nó de Dados):** A menor unidade física que compreende o nome do banco de dados/esquema físico configurado e a tabela física correspondente (ex: `ds_0.orders_0`).
4. **Sharding Column (Chave de Fragmentação / Sharding Key):** O atributo na tabela que determina em qual fragmento o dado deve residir (ex: `user_id` para distribuir usuários, ou `order_id` para distribuir transações).
5. **Sharding Algorithm (Algoritmo de Fragmentação):**
   - **Standard Sharding:** Fragmentação baseada em uma única chave utilizando operadores `=`, `IN`, `BETWEEN`. Pode ser configurado via Inline Expressions (Groovy), algoritmos de Módulo (`MOD`), ou Intervalos de Datas (`INTERVAL`).
   - **Auto Sharding:** Gerenciamento automático em que o usuário especifica o número total de fragmentos físicos e o ShardingSphere equilibra a distribuição sem intervenção direta do DBA nas partições físicas.
6. **Binding Tables:** Tabelas com a mesma relação lógica e regras de fragmentação equivalentes que são comumente unidas por `JOIN` (ex: `orders` e `order_items`). Quando configuradas como *Binding Tables*, o ShardingSphere evita o custo de rotas cruzadas em rede (*cross-node joins*) e executa um join local direto e otimizado no mesmo nó de dados físico.

---

## 🛠️ 2. Guia de Configuração e Uso de Regras do ShardingSphere

Abaixo está o detalhamento prático de como as regras de negócio declarativas são unificadas e aplicadas nos arquivos `/conf/global.yaml` e `/conf/database-sharding.yaml`:

### A) Regra de Particionamento (Data Sharding)
Para garantir que grandes volumes sejam divididos uniformemente, utilizamos uma estratégia de dois níveis:
1. **Database Strategy:** O banco de dados físico de destino é selecionado via operador de Módulo com base na `user_id` (`user_id % 2`). Isso distribui os usuários de forma equilibrada entre os Shards `ds_0` e `ds_1`.
2. **Table Strategy:** Dentro de cada banco físico, a tabela física de destino (`orders_0` ou `orders_1`) é selecionada via operador de Módulo com base na `order_id` (`order_id % 2`).
3. **Key Generation:** Para garantir IDs únicos globais sem colisões entre shards independentes, acoplamos nativamente o algoritmo **Snowflake ID Generator**, que gera identificadores numéricos ordenados com base no tempo de máquina.

### B) Regra de Separação de Leitura e Escrita (Read-Write Splitting)
Para aliviar a carga nos nós primários, o ShardingSphere intercepta os comandos SQL:
- Comandos que alteram estados (`INSERT`, `UPDATE`, `DELETE`, `DDLs`) são direcionados obrigatoriamente para as fontes primárias (`ds_0_primary`, `ds_1_primary`).
- Consultas analíticas ou puras de leitura (`SELECT`) são balanceadas e encaminhadas para os nós réplicas de leitura (`ds_0_replica`, `ds_1_replica`) usando o balanceador nativo **Round Robin (Rodízio)**.

### C) Criptografia Transparente (Data Encryption / LGPD & GDPR Compliance)
O desenvolvedor interage com os campos normais (`cpf`, `credit_card`), mas no banco de dados físico os dados são persistidos criptografados em colunas específicas (`cpf_cipher`, `credit_card_cipher`) usando o algoritmo **AES**.
O ShardingSphere intercepta o SQL lógico, criptografa os parâmetros em tempo de execução antes de enviar ao disco físico, e descriptografa automaticamente os resultados retornados no `ResultSet` de leitura, tudo de forma 100% transparente para a aplicação consumidora.

---

## 🚀 3. Transações Distribuídas (XA / Narayana / BASE)

Garantir a consistência dos dados em múltiplos nós físicos de dados de forma isolada é um dos maiores desafios em bancos distribuídos. O ShardingSphere integra nativamente gerenciadores de transação de mercado:

1. **XA (Consistência Forte / Narayana ou Atomikos):** Implementa o protocolo de **Two-Phase Commit (2PC)**. Garante conformidade total de transações distribuídas (ACID). Se qualquer nó falhar na fase de preparação (*Prepare*), o Transaction Manager dispara imediatamente o *Rollback* em todos os demais nós. Configurado em `global.yaml` utilizando o provider Narayana.
2. **BASE (Consistência Eventual / Seata):** Ideal para cenários de altíssima escala e throughput em que tempos de bloqueio de locks do XA são proibitivos. Utiliza o padrão *Saga* ou *AT (Automatic Transaction)* para compensar operações e harmonizar os dados de forma assíncrona.

---

## 📈 4. Guia Operacional, Performance e Otimização

### Cuidados Importantes e Evitando "Full Route" (Table Scan)
A maior armadilha de performance em bancos de dados distribuídos é a execução de queries sem incluir a **Sharding Key** (`user_id`) no filtro `WHERE`.
- **Single-Route Query (Ideal):** Quando a query possui `WHERE user_id = 10`, o Proxy calcula o hash e envia o SQL especificamente para uma única tabela em um único banco físico.
- **Full Route (Evitar):** Uma query como `SELECT * FROM orders WHERE total_amount > 100.00` força o ShardingSphere a abrir conexões paralelas com **todos os bancos físicos e varrer todas as tabelas** (`orders_0`, `orders_1` em `ds_0` e `ds_1`), fundindo os resultados em memória dentro do Proxy. Isso destrói o poder de escala horizontal e causa gargalos de CPU e IO.

### Gerenciamento de Memória e Pooling no Proxy
1. **HikariCP Tuning:** Configure `maxPoolSize` e `minPoolSize` de acordo com a capacidade de conexões dos bancos físicos PostgreSQL e o número de threads do Proxy para evitar exaustão de conexões físicas (*connection leaks*).
2. **proxy-backend-query-fetch-size:** Limitamos a 500 para evitar que grandes consultas de agregação tragam gigabytes de dados de uma só vez para a memória JVM do Proxy, o que poderia desencadear em Out Of Memory (OOM).

### Online DDL (Esquema Dinâmico) e Rebalanceamento de Dados (Scaling)
1. **Online DDL:** Alterações de tabelas (`ALTER TABLE`) em sistemas distribuídos devem ser feitas via **DistSQL** (Distributed SQL) fornecido pelo ShardingSphere-Proxy para propagar as mudanças de forma coordenada a todos os fragmentos sem interromper os serviços de leitura.
2. **Elastic Scaling / Rebalanceamento:** Conforme o volume de dados cresce, novos shards físicos podem ser inseridos ao cluster de forma dinâmica. A governança do ShardingSphere via **ZooKeeper** ativa os módulos de migração (*Pipeline / Migration*) que movem dados de forma assíncrona baseados em logs de replicação física e atualizam as regras de roteamento sem gerar downtime para o usuário final.
