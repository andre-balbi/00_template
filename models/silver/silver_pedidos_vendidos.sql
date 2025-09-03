{{
    config(
        tags=['vendas']
    )
}}

with pedidos as (
    select
        *
    from {{ ref('bronze_pedidos') }}
)
, pagamentos as (
    select
        *
    from {{ ref('bronze_pagamentos') }}
)

, joined as (
    select
        pedidos.data_pedido
        , pagamentos.valor
        , pagamentos.metodo
        , pagamentos.status
        , pagamentos.data_pagamento
    from pedidos
    left join pagamentos on pedidos.id = pagamentos.pedido_id
)

select
    *
from joined