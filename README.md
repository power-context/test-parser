# Hello!

## For using DB send your IP to me)

- Hostname: mysql.essense.myjino.ru
- Port: 3306
- User: 045332084_test
- Password: testUser

## Second task as SQL query:
=============

with ProfitCTE as 
(
	select A.actor_name as actor,
		   sum(M.tickets_sold) over (partition by A.actor_name) as profit
	from actors A
	left join moovies M
	on A.moovie_name = M.moovie_name
)

select distinct * from ProfitCTE P
order by P.profit desc
limit 3;

