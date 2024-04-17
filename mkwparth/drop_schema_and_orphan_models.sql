/*
Run Command Template:
    dbt run-operation drop_orphan_models --args "{include_schema_list: [schema_name1, schema_name2], dry_run: true}"

Description:
    This macro identifies and drops orphaned schemas and models that exist in the database but are not part of the current DBT project.
    It compares the schemas and models present in the database with those defined in the DBT project and drops any that are not found in the project.

Arguments:
    - include_schema_list: List of schema names to include in the operation. If provided, only schemas listed here will be considered.
    - dry_run: If set to true, the macro will only log the SQL queries it would execute without actually executing them. If set to false,
           the macro will execute the drop operations.

Usage:
    Call this macro passing any required parameters to identify and drop orphaned schemas and models. It can be helpful in keeping 
    the database aligned with the DBT project structure.   
*/


{% macro drop_orphan_models(include_schema_list=[],dry_run = true) %}

    {% set isSchemaInDBT %}
        -- Query to find all schemas that are exists in database and in include_schema_list
        with database_all_schema as (
            select distinct table_schema as table_schema from information_schema.tables where 
            {%if include_schema_list|length > 0 %}
                    table_schema in (
                        {%for s in include_schema_list%}
                            '{{ s }}'{% if not loop.last %},{% endif %}
                        {%endfor%}
                    ) 
                {%else%}
                    1=1
            {% endif %}
            UNION       
            select distinct table_schema as table_schema from information_schema.views where
            {%if include_schema_list|length > 0 %}
                    table_schema in (
                        {%for s in include_schema_list%}
                            '{{ s }}'{% if not loop.last %},{% endif %}
                        {%endfor%}
                    ) 
                {%else%}
                    1=1
            {% endif %}
        ),
        -- Query to find all schemas that are currently used by this dbt project
        dbt_schema as (
            {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list
                                + graph.nodes.values() | selectattr("resource_type", "equalto", "seed")  | list %}
                    SELECT
                    '{{ node.schema }}' AS table_schema
                    {% if not loop.last %} UNION ALL {% endif %}
            {% endfor %}
        ),
        -- Query to find Orphaned Schemas
        orphaned_schemas as(
            select database_all_schema.table_schema as table_schema
            from database_all_schema left join dbt_schema
            on lower(database_all_schema.table_schema) = lower(dbt_schema.table_schema)
            where dbt_schema.table_schema is null
        )
        select 
            'DROP SCHEMA ' || table_schema as drop_schema_query
        from
            orphaned_schemas
    {% endset %}

    {% set output_1 = run_query(isSchemaInDBT) %}
    {% set drop_schema_to_execute = output_1.columns[0].values() %}

    {% for query in drop_schema_to_execute %}
        {%if not dry_run%}
            {% set output_result = run_query(query) %} -- it will execute the drop schema query.
            {% if execute %}
                {{ log(output_result.columns[0].values()[0], info=true) }}
            {% endif %}
        {% else %}
            {{log(query,info=true)}}
        {% endif %}
    {% else %}
        {{ log("No Orphaned Schema Found", info=true) }}        
    {% endfor %}


    {% set query %}
        -- Query to find models that are exists in database and in include_schema_list
        WITH database_all_models AS (
            SELECT 
                table_schema AS schema_name,
                table_name AS ref_name,
                'table' AS ref_type
            FROM 
                information_schema.tables
            WHERE 
                table_type = 'BASE TABLE' AND
                {%if include_schema_list|length > 0 %}
                table_schema in (
                    {%for s in include_schema_list%}
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
                {%if include_schema_list|length > 0 %}
                    table_schema in (
                        {%for s in include_schema_list%}
                            '{{ s }}'{% if not loop.last %},{% endif %}
                        {%endfor%}
                    ) 
                {%else%}
                    1=1
                {% endif %}    
        ),
        -- Query to find models that are currently used by this dbt project
        dbt_models AS (
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

        orphaned_models as
        (
            SELECT 
                lower(database_all_models.schema_name) as schema_name, database_all_models.ref_name as ref_name, database_all_models.ref_type as ref_type
            FROM
                database_all_models left join dbt_models
            ON 
                lower(database_all_models.schema_name) = lower(dbt_models.schema_name) AND database_all_models.ref_name = upper(dbt_models.ref_name)
            WHERE
                dbt_models.ref_name is null and dbt_models.schema_name is null
        )
        select 
        'DROP ' ||
            CASE 
                WHEN ref_type = 'table' THEN 'TABLE '
                ELSE 'VIEW '
            END
            || schema_name || '.' || ref_name AS drop_statement
        from
            orphaned_models
    {% endset %}

    {% set output = run_query(query) %}
    {% set drop_query_to_execute = output.columns[0].values() %}
    {% for query in drop_query_to_execute %}
        {%if not dry_run%}
            {% set output_result = run_query(query) %} -- it will execute the drop statement.
            {% if execute %}
                {{ log(output_result.columns[0].values()[0], info=true) }}
            {% endif %}
        {% else %}
            {{log(query,info=true)}}
        {% endif %} 
    {% else %}
        {{ log("No Orphaned Models Found", info=true) }}       
    {% endfor %}
{% endmacro %}
