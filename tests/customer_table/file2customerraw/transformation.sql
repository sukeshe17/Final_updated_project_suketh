select customer_id, name, email,phone, batchid, created_date, updated_date,rnk from (
SELECT a.*, dense_rank() over (  ORDER	BY batchid DESC) rnk
from dbo.customers_raw a ) test where rnk=1