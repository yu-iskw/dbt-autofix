
{{ config(
    severity='error' if var('strict', false) else 'warn', 
    meta={'custom_config': 'test'}
) }}
select * from {{ ref("sample_model") }} where email is null
