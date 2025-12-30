{% macro check_select_arg() %}

    {% if
        not invocation_args_dict.select
        and target.name  in ['dev']
        and invocation_args_dict.which in ['build', 'run', 'test', 'source', 'snapshot','compile']
    %}

        {{ exceptions.raise_compiler_error("Error: You must provide at least one select argument") }}

    {% endif %}

{% endmacro %}
