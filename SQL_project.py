with first_payments as ( --1 первая транзакция для каждого студента
select 
      user_id ,
    min(transaction_datetime::date) as first_payment_date
from skyeng_db.payments
    where id_transaction is not null and status_name = 'success'
group by 1
),
all_dates as ( -- 2 таблицу с датами за каждый календарный день
select 
    date_trunc('day', class_status_datetime) as dt
from skyeng_db.classes
    where class_status_datetime<='2016.12.31'      --and class_status = 'success'
group by dt
),
all_dates_by_user as ( -- 3 за какие даты имеет смысл собирать баланс для каждого студента
select 
    f.user_id, a.dt
from first_payments f
    join all_dates a on a.dt>= f.first_payment_date
group by 1,2
),
payments_by_dates as( -- 4 Найдем все изменения балансов, связанные с успешными транзакциями
select 
     user_id,
     date_trunc('day', transaction_datetime) as payment_date,
     sum(classes) as transaction_balance_change
from skyeng_db.payments
    where id_transaction is not null and status_name = 'success'  
group by 1, payment_date
),
payments_by_dates_cumsum as( --5 Найдем баланс студентов, который сформирован только транзакциями
select 
    d.user_id, d.dt,transaction_balance_change,
    sum(coalesce(transaction_balance_change,0)) over (partition by d.user_id order by d.dt rows between unbounded preceding and current row) as transaction_balance_change_cs
from all_dates_by_user d
   left join payments_by_dates p on d.user_id=p.user_id and d.dt=p.payment_date
),
classes_by_dates as( --6 Найдем изменения балансов из-за прохождения уроков.
select 
    user_id,
    date_trunc('day', class_status_datetime) as class_date,
    count(id_class)*-1 as classes
from skyeng_db.classes
    where class_status in ('success', 'failed_by_student') and class_type!='trial'
group by 1, class_date
),
classes_by_dates_dates_cumsum as( -- 7 CTE для хранения кумулятивной суммы количества пройденных уроков.
select 
    aa.user_id, aa.dt, cc.classes,
    sum(coalesce(cc.classes,0)) over (partition by aa.user_id order by aa.dt rows between unbounded preceding and current row) as classes_cs
from all_dates_by_user aa
    left join classes_by_dates cc on aa.user_id=cc.user_id and aa.dt=cc.class_date
),
balances as (   -- 8 с вычисленными балансами каждого студента.
select
    pdc.user_id, pdc.dt,transaction_balance_change, transaction_balance_change_cs, cdc.classes, cdc.classes_cs,
    (cdc.classes_cs+transaction_balance_change_cs) as balance
from payments_by_dates_cumsum pdc 
    join classes_by_dates_dates_cumsum cdc on pdc.user_id=cdc.user_id and pdc.dt=cdc.dt
order by user_id,dt
)
select dt,         -- 9 Посмотрим, как менялось общее количество уроков на балансах студентов
    sum(transaction_balance_change) as sum_transaction_balance_change,
    sum(transaction_balance_change_cs) as sum_transaction_balance_change_cs,
    sum(classes) as sum_classes,
    sum(classes_cs) as sum_classes_cs,
    sum(balance) as sum_balance
from balances
group by dt
order by dt