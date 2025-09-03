{%- macro drop_pr_staging_schemas(project_id, PR_number) %}

    {% do log('Starting PR schema cleanup for PR #' ~ PR_number ~ ' in project ' ~ project_id, info=True) %}

    {% set pr_cleanup_query %}
        select schema_name
        from `{{ project_id }}`.INFORMATION_SCHEMA.SCHEMATA
        where schema_name like 'pr_{{ PR_number }}__%'
    {% endset %}

    {% do log('Executing query: ' ~ pr_cleanup_query, info=True) %}

    {% if execute %}
        {% set results = run_query(pr_cleanup_query) %}
        
        {% if results and results.rows %}
            {% do log('Found ' ~ results.rows|length ~ ' schemas to drop', info=True) %}
            
            {% for row in results.rows %}
                {% set schema_name = row[0] %}
                {% set drop_query = 'DROP SCHEMA IF EXISTS `' ~ project_id ~ '.' ~ schema_name ~ '` CASCADE' %}
                
                {% do log('Dropping schema: ' ~ schema_name, info=True) %}
                {% do log('Drop command: ' ~ drop_query, info=True) %}
                
                {% set drop_result = run_query(drop_query) %}
                {% do log('Successfully dropped schema: ' ~ schema_name, info=True) %}
            {% endfor %}
        {% else %}
            {% do log('No PR schemas found to drop for PR #' ~ PR_number, info=True) %}
        {% endif %}
    {% else %}
        {% do log('Skipping schema cleanup - not in execute mode', info=True) %}
    {% endif %}

{%- endmacro -%}
