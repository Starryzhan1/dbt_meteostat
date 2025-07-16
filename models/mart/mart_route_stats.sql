WITH route_table AS 
(SELECT origin, dest, count(DISTINCT tail_number) AS unique_tail_number, count(DISTINCT airline) AS unique_airline, count(flight_number) AS total_flight,
       round(avg(actual_elapsed_time),2) AS avg_actual_elapsed_time, round(avg(arr_delay),2) AS avg_arr_delay,Max(dep_delay+arr_delay) AS Max_delay, Min(dep_delay+arr_delay) AS Min_delay, sum(cancelled) AS total_cancelled, sum(diverted) AS total_diverted
      FROM {{ref('prep_flights')}} as pf
      GROUP BY origin, dest
),
route_stats AS (
	SELECT pa.city AS origin_city,
	       pa.country AS origin_country,
	       pa.name AS origin_name,
	       pa2.city AS dest_city,
	       pa2.country AS dest_country,
	       pa2.name AS dest_name,
	       rt.* 
	FROM route_table rt
	JOIN {{ref('prep_airports')}} as pa
	ON  rt.origin = pa.faa 
	JOIN {{ref('prep_airports')}} as pa2
	ON rt.dest = pa2.faa
)
SELECT * FROM route_stats