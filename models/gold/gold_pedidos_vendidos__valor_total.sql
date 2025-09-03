{{
    config(
        tags=['vendas', 'fatos']
    )
}}

select
    data_pedido
    , metodo
    , status
    , count(*) as total_pedidos
    , sum(valor) as valor_total
    , avg(valor) as valor_medio
from {{ ref('silver_pedidos_vendidos') }}
group by
    data_pedido
    , metodo
    , status