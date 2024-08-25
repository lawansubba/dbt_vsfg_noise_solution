with tbl as (
    select 
        impact_created_date,
        account_unique_id
    from
        {{ ref('stg_noise_solution_survey') }}
}

select * from tbl