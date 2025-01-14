/*
O SGBD escolhido foi o Postgres SQL.

1. Estruturas de dados para armazenamento
    Tabelas Relacionais:
        -Tabelas comuns: Estruturas relacionais padrão, armazenadas em páginas de 8 KB por padrão.
        -Tabelas particionadas: Suportam particionamento horizontal baseado em listas, intervalos ou hash, permitindo organizar grandes conjuntos de dados em partições menores.
        -Tabelas temporárias: Exclusivas para a sessão em que são criadas, usadas para armazenamento temporário.
        -Tabelas unlogged: Não registram operações no log WAL (Write-Ahead Log), oferecendo maior desempenho, mas sem recuperação em caso de falha.

    Clusters
        - O comando `CLUSTER` instrui o PostgreSQL a agrupar uma tabela específica com base um índice. O índice deve ter sido definido previamente na tabela.
    Schemas
        - Conjuntos lógicos de tabelas e outros objetos de banco de dados.

2. Estruturas para indexação

    Índices únicos
    - Garantem que os valores em uma ou mais colunas sejam únicos.

    Índices não únicos
    - Permitem duplicatas, usados para melhorar o desempenho de consultas que não precisam de unicidade.

    Tipos de índices no PostgreSQL
    1. B-Tree:  
    - O tipo de índice padrão. Otimizado para consultas de igualdade e intervalos.
    - Exemplo: `CREATE INDEX idx_nome ON tabela(nome);`

    2. Hash:  
    - Para buscas rápidas de igualdade.  
    - Exemplo: `CREATE INDEX idx_nome_hash ON tabela USING HASH (nome);`

    3. GiST (Generalized Search Tree):  
    - Suporta buscas aproximadas e consultas em tipos geométricos.  
    - Exemplo: usado com extensões como PostGIS.

    4. GIN (Generalized Inverted Index):  
    - Otimizado para tipos de dados complexos, como arrays e JSON.  
    - Exemplo: `CREATE INDEX gin_idx ON tabela USING GIN (coluna_json);`

    5. BRIN (Block Range INdex):  
    - Ideal para grandes tabelas com dados organizados de forma sequencial.  
    - Exemplo: `CREATE INDEX brin_idx ON tabela USING BRIN (data);`


3. Paralelismo

    O PostgreSQL oferece paralelismo em:

    1. consultas:
    - Operações como varredura sequencial (`Parallel Sequential Scan`), agregações e joins podem ser executadas paralelamente.
    - Exemplo: Ativado automaticamente em consultas que excedem o custo de execução paralela (`parallel_setup_cost`).

    2. índices:
    - Índices B-Tree e GiST suportam varreduras paralelas.

    3. planejamento de consultas:
    - Configurado via parâmetros como `max_parallel_workers_per_gather` e `parallel_workers`.

    4. Particionamento


    1. Particionamento de tabelas:
    - Por lista: Baseado em valores discretos de uma coluna.  
    - Por intervalo: Baseado em intervalos de valores.  
    - Por hash: Divide os dados em partições usando um hash.  

    2. Particionamento de índices:
    - Índices podem ser criados em tabelas particionadas, e o PostgreSQL automaticamente cria índices para cada partição.

4. Os principais componentes e processos envolvidos no otimizador do PostgreSQL incluem:
    - Geração de Planos Possíveis: O otimizador cria múltiplos planos de execução para uma consulta, avaliando diferentes métodos de acesso aos dados, como varreduras sequenciais, índices e combinações de junções;
    - Avaliação de Custos: Cada plano potencial recebe uma estimativa de custo baseada em fatores como leituras de disco, uso de CPU e memória. O otimizador utiliza essas estimativas para prever o desempenho de cada plano. 
    - Seleção do Plano Ótimo: Após avaliar os custos, o otimizador escolhe o plano com o menor custo estimado para executar a consulta. Em casos de consultas complexas com muitas junções, o PostgreSQL pode empregar um Otimizador Genético para encontrar um plano razoável em tempo hábil. 
    - Consideração de Estatísticas: O otimizador utiliza estatísticas sobre a distribuição dos dados nas tabelas para estimar a seletividade de condições e o número de linhas afetadas, influenciando a escolha do plano. 
    - Paralelização: Em versões mais recentes, o otimizador suporta a execução paralela de consultas, permitindo que operações como junções hash sejam distribuídas entre múltiplos núcleos de CPU para melhorar o desempenho. 
    
    São utilizadas as modalidades de otimização heurística, baseada em custo e física;
        - A otimização Heurística se basea em regras como reordenar filtros para aplicar os mais seletivos primeiro e eliminar subconsultas desnecessárias ou redundantes.
        - A Otimização Baseada em Custo calcula o custo estimado de diferentes planos de execução, considerando I/O de disco, CPU, tamanho dos dados, cardinalidade e distribuição das tabelas.
        - A Otimização Física escolhe métodos específicos de acesso e junção.

5. Base de dados escolhida: Dados de listagem de imóveis e avaliações do AirBnb;
    Tabelas:
        - Listings: propriedades e suas informações;
        - Reviews: avaliações atribuídas a um determinado imóvel;
    Uma review possui uma chave estrangeira com a id de uma listing;

*/

-- 6. As consultas de criação e importação do banco estão no arquivo criacap_airbnb_data.pgsql
-- Consultas apresentadas:
-- Observação: o tempo de CPU para o container executando o Postgres foi limitado para 0.1m para reduzir
--              propositalmente o desempenho das consultas e tornar melhorias mais evidentes;

---------------------------------------------------------------------------------------------------------------
-- Consulta 1: Número de reviews por bairro com total de listings, preço médio e total de reviews
SET enable_seqscan = on;
EXPLAIN
SELECT 
    l.neighbourhood,
    COUNT(l.listing_id) AS total_listings,
    AVG(l.price) AS avg_price,
    COUNT(r.review_id) AS total_reviews
FROM 
    airbnb_data.listings l
JOIN
    airbnb_data.reviews r
ON
    l.listing_id = r.listing_id
GROUP BY 
    l.neighbourhood
ORDER BY 
    avg_price DESC;
---------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------
--  Consulta 2: Propriedades sem Reviews e sem hospedeiro identificado
SET enable_seqscan = off;
EXPLAIN
SELECT 
    l.listing_id,
    l.name,
    l.host_id,
    l.neighbourhood,
    l.price
FROM 
    airbnb_data.listings l
LEFT JOIN 
    airbnb_data.reviews r
ON 
    l.listing_id = r.listing_id
WHERE 
    r.review_id IS NULL and l.host_identity_verified = false;
---------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------
--  Consulta 3: Propriedades que tiveram avaliações no ano mais recente da tabela e a data da review mais recente
SELECT 
    l.listing_id,
    l.name,
    l.host_id,
    l.neighbourhood,
    l.price,
    MAX(r.date) as latest_review_date
FROM 
    airbnb_data.listings l
INNER JOIN 
    airbnb_data.reviews r
ON 
    l.listing_id = r.listing_id
WHERE 
    date_part('year', r.date) = (SELECT date_part('year', MAX(date)) FROM airbnb_data.reviews)    
GROUP BY l.listing_id;
---------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------
--  Consulta 4: Propriedades que foram avaliadas mais de uma vez pela mesma pessoa
SELECT 
    l.listing_id,
    l.name,
    l.host_id,
    l.neighbourhood,
    l.price,
    COUNT(r.reviewer_id) AS total_reviews
FROM 
    airbnb_data.listings l
INNER JOIN 
    airbnb_data.reviews r
ON 
    l.listing_id = r.listing_id
GROUP BY 
    l.listing_id
HAVING 
    COUNT(r.reviewer_id) > 1;

---------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------
--  Consulta 5: Superhosts Instantaneamente Reserváveis
SELECT 
    l.listing_id,
    l.name,
    l.host_id,
    l.neighbourhood,
    COUNT(r.review_id) AS total_reviews
FROM 
    airbnb_data.listings l
INNER JOIN 
    airbnb_data.reviews r
ON 
    l.listing_id = r.listing_id
WHERE 
    l.host_is_superhost = TRUE
    AND l.instant_bookable = TRUE
GROUP BY 
    l.listing_id, l.name, l.host_id, l.neighbourhood
ORDER BY 
    total_reviews DESC;
---------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------
-- Otimização:
---------------------------------------------------------------------------------------------------------------
-- Comandos executados para a melhoria da Consulta 1 e 2:
CREATE INDEX idx_listings_listing_id ON airbnb_data.listings(listing_id);
CREATE INDEX idx_reviews_listing_id_hash ON airbnb_data.reviews USING HASH (listing_id);
ANALYZE airbnb_data.reviews;
ANALYZE airbnb_data.listings;
SET enable_seqscan = off;
---------------------------------------------------------------------------------------------------------------
/*
 Consulta 1:
    Tentativa de melhoria:
    Criação de índice sobre a chave primária listing_id da tabela listing
    Criação de índice hash sobre a chave estrangeira listing_id da tabela reviews
    Rodar ANALYZE em ambas as tabelas para atualização das estatísticas

    Antes da melhoria:
        Tempo de execução médio: 6.7s
        Plano de execução:
            Sort  (cost=69788.83..69788.93 rows=42 width=60)
                Sort Key: (avg(l.price)) DESC
                ->  Finalize GroupAggregate  (cost=69776.32..69787.69 rows=42 width=60)
                        Group Key: l.neighbourhood
                        ->  Gather Merge  (cost=69776.32..69786.12 rows=84 width=60)
                            Workers Planned: 2
                            ->  Sort  (cost=68776.29..68776.40 rows=42 width=60)
                                    Sort Key: l.neighbourhood
                                    ->  Partial HashAggregate  (cost=68774.64..68775.16 rows=42 width=60)
                                        Group Key: l.neighbourhood
                                        ->  Hash Join  (cost=812.26..64729.27 rows=404537 width=25)
                                                Hash Cond: (r.listing_id = l.listing_id)
                                                ->  Parallel Seq Scan on reviews r  (cost=0.00..51476.10 rows=2238810 width=8)
                                                ->  Hash  (cost=685.45..685.45 rows=10145 width=21)
                                                    ->  Seq Scan on listings l  (cost=0.00..685.45 rows=10145 width=21)

    Após a melhoria:
        Tempo de execução médio: 1.9s
        Plano de execução:
            Sort  (cost=188414.04..188414.15 rows=42 width=60)
                Sort Key: (avg(l.price)) DESC
                ->  HashAggregate  (cost=188412.39..188412.91 rows=42 width=60)
                    Group Key: l.neighbourhood
                    ->  Nested Loop  (cost=0.29..178891.07 rows=952132 width=25)
                            ->  Index Scan using idx_listings_listing_id on listings l  (cost=0.29..2594.24 rows=10145 width=21)
                            ->  Index Scan using idx_reviews_listing_id_hash on reviews r  (cost=0.00..16.44 rows=94 width=8)
                                Index Cond: (listing_id = l.listing_id)
            
    - Após a criação dos índices, apesar da execução dos comandos ANALYZE, o otimnizador não os estava usando durante a execução;
    - Portanto, desabilitamos a variável do POSTGRES enable_seqscan para que o otimizador forçosamente usasse o index.
    - Assim, obteve-se os resultados apresentados;
    - A principal diferença entre os planos de execução gerados aparece na parte debaixo, que é na verdade a primeira a ser executada:
        - Na primeira, é feita uma busca sequencial em listings (Seq Scan) para realização da junção
            Como a tabela reviews tem milhões de linhas, é usado um hash join, no qual é criado uma tabela hash com os registros de 
                reviews pela coluna listing_id;
            Como não havia índices anteriores, o custo da criação da tabela hash está incluído na consulta.
            A junção é feita por meio de uma busca paralela (parallel scan) pelos valores do hash  
        - Após a otimização, ao invés de existir uma busca sequencial em diretamente em listings, ambos os índices criados são utilizados
            para a combinação da junção. 
        - Como consequência, a ligação entre os elementos das duas tabelas é feita de forma muito mais rápida, sem que se tenha que criar uma nova
            hash table
        - A melhoria de desempenho foi de cerca de 3 vezes


*/
---------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------
/*
 Consulta 2
    Tentativa de melhoria:
        Mesmas otimizações utilizadas na consulta 1, pois a consulta 2 também realiza junção entre as duas tabelas;
            Criação de índice sobre a chave primária listing_id da tabela listing
            Criação de índice hash sobre a chave estrangeira listing_id da tabela reviews
            Rodar ANALYZE em ambas as tabelas para atualização das estatísticas

    Antes da melhoria:
        Tempo de execução médio: 4.1s
        Plano de execução:
            Hash Right Join  (cost=718.90..106199.11 rows=1 width=63)
                Hash Cond: (r.listing_id = l.listing_id)
                Filter: (r.review_id IS NULL)
                ->  Seq Scan on reviews r  (cost=0.00..82819.43 rows=5373143 width=8)
                ->  Hash  (cost=685.45..685.45 rows=2676 width=63)
                        ->  Seq Scan on listings l  (cost=0.00..685.45 rows=2676 width=63)
                            Filter: (NOT host_identity_verified)

    Depois da melhoria:
        Tempo de execução médio: 0.7s
        Plano de execução:
            Nested Loop Left Join  (cost=0.29..136023.70 rows=1 width=63)
                Filter: (r.review_id IS NULL)
                ->  Index Scan using idx_listings_listing_id on listings l  (cost=0.29..2594.24 rows=2676 width=63)
                        Filter: (NOT host_identity_verified)
                ->  Index Scan using idx_reviews_listing_id_hash on reviews r  (cost=0.00..48.92 rows=94 width=8)
                        Index Cond: (listing_id = l.listing_id)


    - Dessa vez, antes da otimização, são aplicados os filtros por uma busca linear e em seguida é feito a junção com os resultados;
    - Na versão otimizada, é feito um Nested loop join com o uso do index e o filtro é aplicado no meio dessa operação; 
    - Novamente, a ligação entre as duas tabelas é feita de forma mais eficiente, com uma melhoria de cerca de 6x
    - De novo, precisou-se de desabilitar a variável enable_seqscan para que o otimizador forçosamente usassem os indexes.
*/
---------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------
/*
 Consulta 3
    Tentativa de melhoria:
    Particionamento de review sobre a coluna neighbourhood
    
    Antes da melhoria:
        Tempo de execução médio: 11.7s
*/
---------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------
/*
 Consulta 4
    Tentativa de melhoria:
    Criação de índice não único pela coluna bedrooms de listing
    
    Antes da melhoria:
        Tempo de execução médio: 7.0s

*/
---------------------------------------------------------------------------------------------------------------

/*
 Consulta 5
    Tentativa de melhoria:
    Uso de estatísticas em ambas as tabelas
    
    Antes da melhoria:
        Tempo de execução médio: 4.2s
*/
---------------------------------------------------------------------------------------------------------------

SELECT
*
FROM
    pg_indexes
WHERE
     schemaname = 'airbnb_data';

DROP INDEX IF EXISTS airbnb_data.listings_neighbourhood_hash_idx;
ALTER TABLE airbnb_data.listings DROP CONSTRAINT IF EXISTS listings_pkey;
DROP INDEX IF EXISTS airbnb_data.reviews_review_id_hash_idx;
DROP INDEX IF EXISTS airbnb_data.idx_reviews_listing_id_hash;
