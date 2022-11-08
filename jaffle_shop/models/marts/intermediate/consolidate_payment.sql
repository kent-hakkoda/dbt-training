with consolidate_payment as (

        --mart consolidated_order info
        select 
            ORDERID as order_id, 
            max(CREATED) as payment_finalized_date, 
            sum(AMOUNT) / 100.0 as total_amount_paid
        from 
            {{ref('payment')}}
        where STATUS <> 'fail'
            group by 1)


select * from consolidate_payment