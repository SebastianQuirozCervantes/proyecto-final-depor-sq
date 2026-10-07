-- Ventas de entradas: la extracción es incremental por updated_at, así que una
-- venta anulada después llega dos veces; se conserva su último estado.
with fuente as (
    select * from {{ source('raw', 'taquilla_ventas') }}
),

tipado as (
    select
        try_to_number(venta_id)                                   as venta_id,
        try_to_date(fecha_partido)                                as fecha_partido,
        codigo_competicion,
        upper(tribuna)                                            as tribuna,
        upper(canal)                                              as canal,
        try_to_number(cantidad)                                   as cantidad,
        try_to_decimal(precio_unitario, 8, 2)                     as precio_unitario,
        upper(estado)                                             as estado,
        try_to_timestamp_ntz(created_at, 'YYYY-MM-DD HH24:MI:SS') as creado_en,
        try_to_timestamp_ntz(updated_at, 'YYYY-MM-DD HH24:MI:SS') as actualizado_en
    from fuente
)

select
    *,
    cantidad * precio_unitario as importe,
    estado = 'PAGADA'          as es_venta_valida
from tipado
qualify row_number() over (partition by venta_id order by actualizado_en desc) = 1
