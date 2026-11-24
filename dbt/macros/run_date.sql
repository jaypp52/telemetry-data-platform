{#- The run date as a SQL date literal; the default far-future date means "no cut-off". -#}
{% macro run_date() -%}
    cast('{{ var("run_date") }}' as date)
{%- endmacro %}

{% macro run_date_is_set() -%}
    {{ return(var("run_date") | string != "9999-12-31") }}
{%- endmacro %}
