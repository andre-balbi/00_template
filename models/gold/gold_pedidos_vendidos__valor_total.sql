{{
    config(
        tags=['vendas', 'fatos']
    )
}}

select
    data_pedido 
    , metodo
    , status
    , count(*) as total_pedidos_vendidos
    , sum(valor) as valor_total_vendido
    , avg(valor) as valor_medio_pedido
    , min(valor) as valor_minimo_pedido
    , max(valor) as valor_maximo_pedido
from {{ ref('silver_pedidos_vendidos') }}
group by
    data_pedido
    , metodo
    , status
order by
    data_pedido desc