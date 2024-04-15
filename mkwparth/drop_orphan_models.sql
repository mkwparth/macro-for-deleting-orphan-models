-- This macro drop tables and view that are present in warehouse but not build by DBT project.
{% macro drop_orphan_models(dry_run = false) %}
    {% set query %}
        WITH curr AS (
            SELECT 
                table_schema AS schema_name,
                table_name AS ref_name,
                'table' AS ref_type
            FROM 
                information_schema.tables
            WHERE 
                table_type = 'BASE TABLE'
                AND table_schema = 'DBT_PMAKWANA' -- Select all the tables that are present in warehouse
            UNION ALL
            SELECT 
                table_schema AS schema_name,
                table_name AS ref_name,
                'view' AS ref_type
            FROM 
                information_schema.views
            WHERE
                table_schema = 'DBT_PMAKWANA' -- Select all the views that are present in warehouse
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
            -- select lower(schema_name) as schema_name, ref_name as ref_name, ref_type as ref_type from curr
            -- except
            -- select lower(schema_name), upper(ref_name), ref_type as ref_type from desired

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
        {%if dry_run%}
            {% set output_result = run_query(query) %} -- it will execute the drop statement.
            {% if execute %}
                {{ log(output_result.columns[0].values(), info=true) }}
            {% endif %}
        {% else %}
            {{log(query,info=true)}}
        {% endif %}    
    {% endfor %}

{% endmacro %}
