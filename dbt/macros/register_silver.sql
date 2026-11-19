{% macro register_silver() %}
  {#- Registers the path-based Silver Delta tables as Unity Catalog external tables. -#}
  {% set catalog = target.catalog %}
  {% set schema = var('silver_schema') %}
  {% set base = 'abfss://silver@' ~ var('storage_account') ~ '.dfs.core.windows.net' %}
  {% do run_query("CREATE SCHEMA IF NOT EXISTS `" ~ catalog ~ "`.`" ~ schema ~ "`") %}
  {% for t in ['trips', 'payments', 'dim_driver', 'dim_rider'] %}
    {% set ddl %}
      CREATE TABLE IF NOT EXISTS `{{ catalog }}`.`{{ schema }}`.`{{ t }}`
      USING DELTA LOCATION '{{ base }}/{{ t }}'
    {% endset %}
    {% do run_query(ddl) %}
    {{ log("registered silver." ~ t, info=True) }}
  {% endfor %}
{% endmacro %}
