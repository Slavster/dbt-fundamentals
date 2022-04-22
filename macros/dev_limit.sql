{% macro dev_limit(column_name, days = 3) %}

{% if target.name == 'default' %}

where {{ column_name }} >= date_add(current_date(), interval -{{ days }} day)

{% endif %}
{% endmacro %}