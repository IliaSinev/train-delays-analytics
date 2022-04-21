{#
    This macro returns the type of the PERFORMANCE_EVENT
#}

{% macro get_performance_event(PERFORMANCE_EVENT_CODE) -%}

    case {{ PERFORMANCE_EVENT_CODE }}
        when 'A' then 'Delay'
        when 'M' then 'Delay'
        when 'C' then 'Full cancellation'
        when 'D' then 'diversion'
        when 'F' then 'Failure to stop'
        when 'O' then 'Part cancellation'
        when 'P' then 'Part cancellation'
        when 'S' then 'Scheduled cancellation'
    end

{%- endmacro %}