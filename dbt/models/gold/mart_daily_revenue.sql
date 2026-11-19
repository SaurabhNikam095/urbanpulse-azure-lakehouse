-- Grain: one row per day. Cancellation rate = cancelled / (completed + cancelled).
-- Revenue = amount of SUCCESS payments on the day they were paid.
with t as (
    select
        cast(first_event_at as date) as report_date,
        count(*) as trips,
        sum(case when status = 'completed' then 1 else 0 end) as completed_trips,
        sum(case when status = 'cancelled' then 1 else 0 end) as cancelled_trips
    from {{ ref('fact_trips') }}
    group by 1
),

p as (
    select
        cast(paid_at as date) as report_date,
        sum(case when status = 'SUCCESS' then amount else 0 end) as revenue,
        sum(case when status = 'SUCCESS' then 1 else 0 end) as successful_payments,
        sum(case when status = 'FAILED' then 1 else 0 end) as failed_payments
    from {{ ref('fact_payments') }}
    group by 1
)

select
    coalesce(t.report_date, p.report_date) as report_date,
    coalesce(t.trips, 0) as trips,
    coalesce(t.completed_trips, 0) as completed_trips,
    coalesce(t.cancelled_trips, 0) as cancelled_trips,
    round(
        coalesce(t.cancelled_trips, 0)
        / nullif(coalesce(t.completed_trips, 0) + coalesce(t.cancelled_trips, 0), 0),
        4
    ) as cancellation_rate,
    coalesce(p.revenue, 0) as revenue,
    coalesce(p.successful_payments, 0) as successful_payments,
    coalesce(p.failed_payments, 0) as failed_payments
from t
full outer join p on t.report_date = p.report_date
