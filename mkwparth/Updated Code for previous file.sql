-- This macro drop tables and view that are present in warehouse but not currently used by DBT project.
-- Based on table_schema values table and view will be dropped  
-- Use dry_run as true for logging drop command
-- For Executing Drop Command use dry_run as false
-- Command for Executing this macro : dbt run-operation drop_orphan_models --args '{dry_run : true}'

{% macro drop_orphan_models(schema_list=[],dry_run = true) %}
    {% set query %}
        WITH curr AS (
            SELECT 
                table_schema AS schema_name,
                table_name AS ref_name,
                'table' AS ref_type
            FROM 
                information_schema.tables
            WHERE 
                table_type = 'BASE TABLE' AND
                {%if schema_list|length > 0 %}
                table_schema in (
                    {%for s in schema_list%}
                        '{{ s }}'{% if not loop.last %},{% endif %}
                    {%endfor%}
                ) 
                {%else%}
                    1=1
                {% endif %}
            UNION ALL
            SELECT 
                table_schema AS schema_name,
                table_name AS ref_name,
                'view' AS ref_type
            FROM 
                information_schema.views 
            WHERE
                {%if schema_list|length > 0 %}
                    table_schema in (
                        {%for s in schema_list%}
                            '{{ s }}'{% if not loop.last %},{% endif %}
                        {%endfor%}
                    ) 
                {%else%}
                    1=1
                {% endif %}    
        ),

        desired AS (
        -- This for loop runs and selects all models that are built using the DBT project.
            {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list
                            + graph.nodes.values() | selectattr("resource_type", "equalto", "seed")  | list %}
                SELECT
                '{{ node.schema }}' AS schema_name,
                '{{ node.name }}' AS ref_name,
                CASE
                    WHEN '{{node.is_table}}' = TRUE THEN 'table'
                    ELSE 'view'
                END AS ref_type
                {% if not loop.last %} UNION ALL {% endif %}
            {% endfor %}
        ),

        drop_commands_cte as
        (
            SELECT 
                lower(curr.schema_name) as schema_name, curr.ref_name as ref_name, curr.ref_type as ref_type
            FROM
                curr left join desired
            ON 
                lower(curr.schema_name) = lower(desired.schema_name) AND curr.ref_name = upper(desired.ref_name)
            WHERE
                desired.ref_name is null and desired.schema_name is null
        )
        select 
        'DROP ' ||
            CASE 
                WHEN ref_type = 'table' THEN 'TABLE '
                ELSE 'VIEW '
            END
            || schema_name || '.' || ref_name AS drop_statement
        from
            drop_commands_cte
    {% endset %}

    {% set output = run_query(query) %}
    {% set drop_query_to_execute = output.columns[0].values() %}

    {% for query in drop_query_to_execute %}
        {%if not dry_run%}
            {% set output_result = run_query(query) %} -- it will execute the drop statement.
            {% if execute %}
                {{ log(output_result.columns[0].values(), info=true) }}
            {% endif %}
        {% else %}
            {{log(query,info=true)}}
        {% endif %}    
    {% endfor %}

{% endmacro %}
