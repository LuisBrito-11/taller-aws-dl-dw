-- Redshift no soporta SELECT * EXCLUDE: se listan las columnas con dbt_utils.star.
select {{ dbt_utils.star(from=ref('int_mongodb__navigation_events__qa'), except=['rejection_reason']) }}
from {{ ref('int_mongodb__navigation_events__qa') }}
where rejection_reason is null
