select user_id, income, additional_id_1, additional_id_2, operation_date::date, posting_date:: date,
from dm.operations
where operation_date >= date '2025-12-26' and operation_date < date '2025-12-27'
GROUP BY user_id, income, additional_id_1, additional_id_2, operation_date::date, posting_date:: date
HAVING COUNT(*) > 1


-- Проверка
select id, user_id 
from dm.operations
where user_id = '40f84546-c037-4eb6-b775-dfb64e18cf62'
and operation_date >= date '2025-12-26' and operation_date < date '2025-12-27'
