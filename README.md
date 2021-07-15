Hello!

For using DB send your IP to me)

Second task as SQL query:
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

