# SQL

#### Основное задание курсовой

Задача — смоделировать изменение балансов студентов. Баланс — это количество уроков, которое есть у каждого студента. 
Чтобы проверить, всё ли в порядке с нашими данными, составить список гипотез и вопросов, нам важно понимать: 

- сколько всего уроков было на балансе всех учеников за каждый календарный день;
- как это количество менялось под влиянием транзакций (оплат, начислений, корректирующих списаний) и уроков (списаний с баланса по мере прохождения уроков).

Также мы хотим создать таблицу, где будут балансы каждого студента за каждый день.

#### Решение:

1. Узнала, когда была первая транзакция для каждого студента. Начиная с этой даты, буду собирать его баланс уроков. 
Создала CTE first_payments с двумя полями: user_id и first_payment_date (дата первой успешной транзакции). 
2. Собрала таблицу с датами за каждый календарный день 2016 года. Выбрала все даты из таблицы classes, создала CTE all_dates с полем dt,
   где будут храниться уникальные даты (без времени) уроков. 
3. Узнала, за какие даты имеет смысл собирать баланс для каждого студента. Для этого объединила таблицы и создала CTE all_dates_by_user,
   где будут храниться все даты жизни студента после того, как произошла его первая транзакция. 
4. Нашла все изменения балансов, связанные с успешными транзакциями. Выбрала все транзакции из таблицы payments, сгруппировала их по user_id и
   дате транзакции (без времени) и нашла сумму по полю classes.
5. Нашла баланс студентов, который сформирован только транзакциями. Для этого объединила all_dates_by_user и payments_by_dates так, чтобы совпадали даты
   и user_id. Использовала оконные выражения (функцию sum), чтобы найти кумулятивную сумму по полю transaction_balance_change для всех строк до текущей
   включительно с разбивкой по user_id и сортировкой по dt. 
6. Нашла изменения балансов из-за прохождения уроков. Создала CTE classes_by_dates, посчитала в таблице classes количество уроков за каждый день
   для каждого ученика. Исключила вводные уроки и уроки со статусом, отличным от success и failed_by_student.
7. По аналогии с уже проделанным шагом для оплат создала CTE для хранения кумулятивной суммы количества пройденных уроков. 
   Для этого объединила таблицы all_dates_by_user и classes_by_dates так, чтобы совпадали даты и user_id. Использовала оконные выражения (функцию sum),
   чтобы найти кумулятивную сумму по полю classes для всех строк до текущей включительно с разбивкой по user_id и сортировкой по dt. 
8. Создала CTE balances с вычисленными балансами каждого студента. Для этого объединила таблицы payments_by_dates_cumsum и classes_by_dates_dates_cumsum
   так, чтобы совпадали даты и user_id.
9. Что бы посмотреть, как менялось общее количество уроков на балансах студентов, просуммировала поля `transaction_balance_change`,
   `transaction_balance_change_cs`, `classes`, `classes_cs`, `balance` из CTE `balances` с группировкой и сортировкой по `dt`.

#### Результат курсовой

В результате получился запрос, который собирает данные о балансах студентов за каждый прожитый ими день.
