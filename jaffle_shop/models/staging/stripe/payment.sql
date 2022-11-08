with payment as (
    select *
    from {{source('stripe','payment')}}
    )

select * from payment