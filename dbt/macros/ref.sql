{% macro ref_data(schema_name, model_name) %}
    {% if schema_name is not none %} 
        {% set rel = builtins.ref(model_name) %}
        {% set newrel = rel.replace_path(schema=schema_name) %}
        {% do return(newrel) %}
    {% endif %}

{% endmacro %}
