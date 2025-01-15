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
--  Consulta 3: Propriedades com o maior número de reviews por ano
EXPLAIN
WITH review_count_by_year_and_listing AS (
    SELECT 
        l.listing_id,
        l.name,
        date_part('year', r.date) AS _year,
        COUNT(*) AS year_count
    FROM 
        airbnb_data.listings l
    INNER JOIN 
        airbnb_data.reviews r
    ON 
        l.listing_id = r.listing_id 
    GROUP BY 
        date_part('year', r.date),
        l.listing_id,
        l.name
),
max_reviews_by_year AS (
    SELECT 
        _year,
        MAX(year_count) AS max_count
    FROM 
        review_count_by_year_and_listing
    GROUP BY 
        _year
)
SELECT 
    rcy._year,
    rcy.listing_id,
    rcy.name,
    rcy.year_count AS num_reviews
FROM 
    review_count_by_year_and_listing rcy
INNER JOIN 
    max_reviews_by_year mry
ON 
    rcy._year = mry._year AND rcy.year_count = mry.max_count
ORDER BY 
    rcy._year;
---------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------
--  Consulta 4: Número de reviews por reviewer
-- EXPLAIN
SELECT 
    r.reviewer_id,
    COUNT(*)
FROM
    airbnb_data.reviews r
GROUP BY 
    r.reviewer_id;
---------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------
--  Consulta 5: Superhosts Instantaneamente Reserváveis
SET enable_seqscan=off;
EXPLAIN
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
-- Comandos executados para a melhoria da Consulta 1, 2 e 5:
CREATE INDEX idx_listings_listing_id ON airbnb_data.listings(listing_id);
CREATE INDEX idx_reviews_listing_id_hash ON airbnb_data.reviews USING HASH (listing_id);
ANALYZE airbnb_data.reviews;
ANALYZE airbnb_data.listings;
SET enable_seqscan = off;
---------------------------------------------------------------------------------------------------------------
-- Comandos executados para a melhoria da Consulta 3:
-- Comandos realizados para criação da tabela particionada e execução da consulta
-- Cria a tabela reviews particionada pelo ano da coluna date
CREATE TABLE airbnb_data.reviews_partitioned (
    listing_id INT,
    review_id INT,
    date DATE NOT NULL,
    reviewer_id INT
) PARTITION BY RANGE (EXTRACT(YEAR FROM date));

-- Criação das partições para cada ano de 2008 a 2021
DO $$
BEGIN
    FOR yr IN 2008..2021 LOOP
        EXECUTE format(
            'CREATE TABLE airbnb_data.reviews_%s PARTITION OF airbnb_data.reviews_partitioned
             FOR VALUES FROM (%s) TO (%s);',
            yr, yr, yr + 1
        );
    END LOOP;
END $$;

-- População da tabela particionada com todas as linhas da tabela review
INSERT INTO airbnb_data.reviews_partitioned (listing_id, review_id, date, reviewer_id)
SELECT listing_id, review_id, date, reviewer_id
FROM airbnb_data.reviews;

-- Mudança do texto da consulta para utilização da tabela particionada
EXPLAIN
WITH review_count_by_year_and_listing AS (
    SELECT 
        l.listing_id,
        l.name,
        date_part('year', r.date) AS _year,
        COUNT(*) AS year_count
    FROM 
        airbnb_data.listings l
    INNER JOIN 
        airbnb_data.reviews_partitioned r
    ON 
        l.listing_id = r.listing_id 
    GROUP BY 
        date_part('year', r.date),
        l.listing_id,
        l.name
),
max_reviews_by_year AS (
    SELECT 
        _year,
        MAX(year_count) AS max_count
    FROM 
        review_count_by_year_and_listing
    GROUP BY 
        _year
)
SELECT 
    rcy._year,
    rcy.listing_id,
    rcy.name,
    rcy.year_count AS num_reviews
FROM 
    review_count_by_year_and_listing rcy
INNER JOIN 
    max_reviews_by_year mry
ON 
    rcy._year = mry._year AND rcy.year_count = mry.max_count
ORDER BY 
    rcy._year;
---------------------------------------------------------------------------------------------------------------
-- Comandos executados para a melhoria da Consulta 4:
CREATE INDEX idx_reviewer_id ON airbnb_data.reviews(reviewer_id);
---------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------
-- RESULTADOS
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
        Particionamento de review sobre o ano da coluna date
        Rodar ANALYZE em ambas as tabelas para atualização das estatísticas


    Antes da melhoria:
        Tempo de execução médio: 11.5s
        Plano de execução:
            Sort  (cost=293076.18..293088.20 rows=4805 width=536)
                Sort Key: rcy._year
                CTE review_count_by_year_and_listing
                    ->  Finalize GroupAggregate  (cost=118631.99..244491.28 rows=960911 width=58)
                        Group Key: (date_part('year'::text, (r.date)::timestamp without time zone)), l.listing_id, l.name
                        ->  Gather Merge  (cost=118631.99..222070.01 rows=800760 width=58)
                                Workers Planned: 2
                                ->  Partial GroupAggregate  (cost=117631.97..128642.42 rows=400380 width=58)
                                    Group Key: (date_part('year'::text, (r.date)::timestamp without time zone)), l.listing_id, l.name
                                    ->  Sort  (cost=117631.97..118632.92 rows=400380 width=50)
                                            Sort Key: (date_part('year'::text, (r.date)::timestamp without time zone)), l.listing_id, l.name
                                            ->  Hash Join  (cost=812.26..66689.59 rows=400380 width=50)
                                                Hash Cond: (r.listing_id = l.listing_id)
                                                ->  Parallel Seq Scan on reviews r  (cost=0.00..51476.10 rows=2238810 width=8)
                                                ->  Hash  (cost=685.45..685.45 rows=10145 width=42)
                                                        ->  Seq Scan on listings l  (cost=0.00..685.45 rows=10145 width=42)
                ->  Hash Join  (cost=24027.78..48291.07 rows=4805 width=536)
                        Hash Cond: ((rcy._year = review_count_by_year_and_listing._year) AND (rcy.year_count = (max(review_count_by_year_and_listing.year_count))))
                        ->  CTE Scan on review_count_by_year_and_listing rcy  (cost=0.00..19218.22 rows=960911 width=536)
                        ->  Hash  (cost=24024.78..24024.78 rows=200 width=16)
                            ->  GroupAggregate  (cost=0.00..24024.78 rows=200 width=16)
                                    Group Key: review_count_by_year_and_listing._year
                                    ->  CTE Scan on review_count_by_year_and_listing  (cost=0.00..19218.22 rows=960911 width=16)

    Após a melhoria:
        Tempo de execução médio: 12.6
        Plano de execução:
            Sort  (cost=1032931.35..1032956.71 rows=10145 width=536)
                Sort Key: rcy._year
                CTE review_count_by_year_and_listing
                    ->  HashAggregate  (cost=792449.11..927864.40 rows=2029000 width=58)
                        Group Key: date_part('year'::text, (r.date)::timestamp without time zone), l.listing_id, l.name
                        Planned Partitions: 64
                        ->  Hash Join  (cost=812.26..211278.21 rows=5374991 width=50)
                                Hash Cond: (r.listing_id = l.listing_id)
                                ->  Append  (cost=0.00..109684.86 rows=5374991 width=8)
                                    ->  Seq Scan on reviews_2008 r_1  (cost=0.00..28.50 rows=1850 width=8)
                                    ->  Seq Scan on reviews_2009 r_2  (cost=0.00..2.15 rows=115 width=8)
                                    ->  Seq Scan on reviews_2010 r_3  (cost=0.00..19.36 rows=1236 width=8)
                                    ->  Seq Scan on reviews_2011 r_4  (cost=0.00..96.53 rows=6253 width=8)
                                    ->  Seq Scan on reviews_2012 r_5  (cost=0.00..307.22 rows=19922 width=8)
                                    ->  Seq Scan on reviews_2013 r_6  (cost=0.00..779.22 rows=50522 width=8)
                                    ->  Seq Scan on reviews_2014 r_7  (cost=0.00..1882.32 rows=122132 width=8)
                                    ->  Seq Scan on reviews_2015 r_8  (cost=0.00..4319.32 rows=280332 width=8)
                                    ->  Seq Scan on reviews_2016 r_9  (cost=0.00..7730.54 rows=501754 width=8)
                                    ->  Seq Scan on reviews_2017 r_10  (cost=0.00..11974.00 rows=777200 width=8)
                                    ->  Seq Scan on reviews_2018 r_11  (cost=0.00..17600.96 rows=1142496 width=8)
                                    ->  Seq Scan on reviews_2019 r_12  (cost=0.00..25165.46 rows=1633546 width=8)
                                    ->  Seq Scan on reviews_2020 r_13  (cost=0.00..11636.24 rows=755324 width=8)
                                    ->  Seq Scan on reviews_2021 r_14  (cost=0.00..1268.09 rows=82309 width=8)
                                ->  Hash  (cost=685.45..685.45 rows=10145 width=42)
                                    ->  Seq Scan on listings l  (cost=0.00..685.45 rows=10145 width=42)
                ->  Hash Join  (cost=50730.00..101962.87 rows=10145 width=536)
                        Hash Cond: ((rcy._year = review_count_by_year_and_listing._year) AND (rcy.year_count = (max(review_count_by_year_and_listing.year_count))))
                        ->  CTE Scan on review_count_by_year_and_listing rcy  (cost=0.00..40580.00 rows=2029000 width=536)
                        ->  Hash  (cost=50727.00..50727.00 rows=200 width=16)
                            ->  HashAggregate  (cost=50725.00..50727.00 rows=200 width=16)
                                    Group Key: review_count_by_year_and_listing._year
                                    ->  CTE Scan on review_count_by_year_and_listing  (cost=0.00..40580.00 rows=2029000 width=16)

    - Antes e depois da melhoria, após rodar as subconsultas, o otimizador escolheu um hash join para agregar os resultados das duas subconsultas sob as quais é feito um inner join
        pelas colunas ano e quantidade de reviews;
    - A principal diferença entre os dois planos é o estágio inicial:
        - O primeiro utiliza parallel seq scan nas duas tabelas após criar uma hash table e assim fazer o join presente nas subconsultas;
        - O segundo faz uma busca sequencial em cada partição paara agrupar por ano e em seguida também realiza um Hash join criando uma tabela hash;
        - Porém, o uso do particionamento não trouxe ganho efetivo, sendo que na média o tempo de execução piorou cerca de 1s após a sua aplicação.
*/
---------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------
/*
 Consulta 4
    Tentativa de melhoria:
        Criação de índice não único pela coluna reviewer_id de reviews
    
    Antes da melhoria:
        Tempo de execução médio: 7.0s
        Plano de execução:
            HashAggregate  (cost=385058.72..457814.48 rows=3077808 width=12)
                Group Key: reviewer_id
                Planned Partitions: 64
                ->  Seq Scan on reviews r  (cost=0.00..82819.43 rows=5373143 width=4)

    Após a melhoria:
        Tempo de execução médio: 2.0s
        Plano de execução:
            GroupAggregate  (cost=0.43..193657.06 rows=3077808 width=12)
                Group Key: reviewer_id
                ->  Index Only Scan using idx_reviewer_id on reviews r  (cost=0.43..136013.27 rows=5373143 width=4)

    Dessa vez, o otimizador utilizou uma busca sequencial antes da otimização;
    Após a criação do indice, ele usou uma busca pelo índice, já que a coluna na cláusula WHERE era a do indice;
    Dessa forma, houve uma melhoria
*/
---------------------------------------------------------------------------------------------------------------

/*
 Consulta 5
    Tentativa de melhoria:
        Criação de índice sobre a chave primária listing_id da tabela listing
        Criação de índice hash sobre a chave estrangeira listing_id da tabela reviews
        Uso de estatísticas em ambas as tabelas
    
    Antes da melhoria:
        Tempo de execução médio: 4.2s
        Plano de execução:
            Sort  (cost=61856.87..61857.07 rows=83 width=66)
                Sort Key: (count(r.review_id)) DESC
                    ->  Finalize GroupAggregate  (cost=61782.19..61854.22 rows=83 width=66)
                            Group Key: l.listing_id, l.name, l.host_id, l.neighbourhood
                            ->  Gather Merge  (cost=61782.19..61851.32 rows=166 width=66)
                                Workers Planned: 2
                                ->  Partial GroupAggregate  (cost=60782.16..60832.13 rows=83 width=66)
                                        Group Key: l.listing_id, l.name, l.host_id, l.neighbourhood
                                        ->  Sort  (cost=60782.16..60790.35 rows=3276 width=62)
                                            Sort Key: l.listing_id, l.name, l.host_id, l.neighbourhood
                                            ->  Hash Join  (cost=686.49..60590.88 rows=3276 width=62)
                                                    Hash Cond: (r.listing_id = l.listing_id)
                                                    ->  Parallel Seq Scan on reviews r  (cost=0.00..51476.10 rows=2238810 width=8)
                                                    ->  Hash  (cost=685.45..685.45 rows=83 width=58)
                                                        ->  Seq Scan on listings l  (cost=0.00..685.45 rows=83 width=58)
    
    Tempo de execução médio: 1.5s
    Plano de execução:
        Sort  (cost=29715.34..29715.55 rows=83 width=66)
        Sort Key: (count(r.review_id)) DESC
        ->  HashAggregate  (cost=29711.86..29712.69 rows=83 width=66)
                Group Key: l.listing_id, l.name, l.host_id, l.neighbourhood
                ->  Nested Loop  (cost=5.01..29614.28 rows=7807 width=62)
                    ->  Index Scan using idx_listings_listing_id on listings l  (cost=0.29..2594.24 rows=83 width=58)
                            Filter: (host_is_superhost AND instant_bookable)
                    ->  Bitmap Heap Scan on reviews r  (cost=4.73..324.60 rows=94 width=8)
                            Recheck Cond: (l.listing_id = listing_id)
                            ->  Bitmap Index Scan on idx_reviews_listing_id_hash  (cost=0.00..4.71 rows=94 width=0)
                                Index Cond: (listing_id = l.listing_id)                                                            Filter: (host_is_superhost AND instant_bookable)

    - Assim como nas consultas 1 e 2, após a criação dos índices, o uso de busca sequencial seguido por criação de
        tabelas hashes foi substituído por uma busca por índice.
    - Como consequência, o tempo de execução melhorou bastante.
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
