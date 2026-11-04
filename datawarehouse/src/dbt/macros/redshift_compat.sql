{# Redshift no soporta IS DISTINCT FROM: equivalente null-safe. #}
{% macro is_distinct(a, b) -%}
(({{ a }} <> {{ b }}) or ({{ a }} is null and {{ b }} is not null) or ({{ a }} is not null and {{ b }} is null))
{%- endmacro %}

{# DuckDB timezone('America/Bogota', ts) -> Redshift: timestamptz a timestamp UTC y luego convert_timezone de 3 argumentos
   (Redshift no acepta convert_timezone(destino, timestamptz)). #}
{% macro to_business_time(ts) -%}
convert_timezone('UTC', '{{ var("business_timezone") }}', cast({{ ts }} as timestamp))
{%- endmacro %}

{# Ultima version de cada llave entre lotes de raw (raw es append-only por _batch_id). #}
{% macro latest_per_key(relation, key) -%}
select *
from (
    select
        *,
        row_number() over (partition by {{ key }} order by _extracted_at desc, _loaded_at desc) as _rn
    from {{ relation }}
) as ranked
where _rn = 1
{%- endmacro %}

{# DuckDB extract(isodow) -> Redshift date_part(dow) (0 = domingo) llevado a ISO (1 = lunes ... 7 = domingo). #}
{% macro iso_dow(d) -%}
(case when date_part(dow, {{ d }}) = 0 then 7 else date_part(dow, {{ d }}) end)
{%- endmacro %}
