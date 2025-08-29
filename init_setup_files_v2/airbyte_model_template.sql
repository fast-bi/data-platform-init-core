with
    source as (
        select * from {{ source('source_name', 'source_table_name')}}
        ),
    final as (
        select fields
    from source
)
select * from final