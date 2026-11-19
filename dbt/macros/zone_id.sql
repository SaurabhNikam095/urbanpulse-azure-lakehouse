{% macro zone_id(lat, lon) -%}
  case
    when {{ lat }} is null or {{ lon }} is null then null
    else concat('Z_', cast(round({{ lat }}, 2) as string), '_', cast(round({{ lon }}, 2) as string))
  end
{%- endmacro %}
