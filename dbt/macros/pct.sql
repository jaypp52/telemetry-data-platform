{#- Percentage with a safe zero denominator, as an exact DECIMAL so that later sums and
    averages do not depend on the order DuckDB's threads add floating-point numbers in. -#}
{% macro pct(numerator, denominator, digits=2) -%}
    cast(round(100.0 * ({{ numerator }}) / nullif({{ denominator }}, 0), {{ digits }}) as decimal(9, {{ digits }}))
{%- endmacro %}
