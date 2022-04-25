{#
    This macro detects date values stored as string and converts in to DATE 
#}

{% macro convert_date(datestring) -%}

    CASE 
        WHEN {{ datestring }} LIKE "__.%" THEN parse_date("%d.%m.%y", LEFT({{ datestring }}, 10))
        WHEN {{ datestring }} LIKE "__/%" THEN parse_date("%d/%m/%Y", LEFT({{ datestring }}, 10))
        WHEN {{ datestring }} LIKE "__-%" THEN parse_date("%d-%b-%Y", LEFT({{ datestring }}, 11))
        ELSE NULL
    END

{%- endmacro %}