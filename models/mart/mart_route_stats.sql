WITH route_table AS 
(SELECT origin, 
        dest, 
        COUNT(DISTINCT tail_number) AS unique_tail_number, 
        COUNT(DISTINCT airline) AS unique_airline, 
        COUNT(flight_number) AS total_flight,
        ROUND(AVG(actual_elapsed_time),2) AS avg_actual_elapsed_time, 
        ROUND(AVG(arr_delay),2) AS avg_arr_delay,
        MAX(arr_delay) AS Max_delay, 
        MIN(arr_delay) AS Min_delay, 
        SUM(cancelled) AS total_cancelled, 
        SUM(diverted) AS total_diverted
FROM {{ref('prep_flights')}} AS pf
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
	JOIN {{ref('prep_airports')}} AS pa
	ON  rt.origin = pa.faa 
	JOIN {{ref('prep_airports')}} AS pa2
	ON rt.dest = pa2.faa
)
SELECT * FROM route_stats