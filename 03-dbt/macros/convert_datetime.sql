{#
    This macro detects datetime values stored as string and converts in to DATE 
#}

{% macro convert_date(datetimestring) -%}

    CASE 
        WHEN datetimestring LIKE "__.%" THEN parse_date("%d.%m.%y %H:%M", datetimestring)
        WHEN datetimestring LIKE "__/%" THEN parse_date("%d/%m/%Y %H:%M", datetimestring)
        WHEN datetimestring LIKE "__-%" THEN parse_date("%d-%b-%Y %H:%M", datetimestring)
        ELSE NULL
    END

{%- endmacro %}