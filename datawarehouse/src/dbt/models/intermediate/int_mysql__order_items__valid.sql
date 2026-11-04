-- Redshift no soporta SELECT * EXCLUDE: se listan las columnas con dbt_utils.star.
select {{ dbt_utils.star(from=ref('int_mysql__order_items__qa'), except=['rejection_reason']) }}
from {{ ref('int_mysql__order_items__qa') }}
where rejection_reason is null
