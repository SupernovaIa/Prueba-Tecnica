# Géneros por puntuación

query_1 = """
SELECT final_table.genres AS Genre, avg(final_table.score) AS mean_score
FROM (
	select * 
	FROM artists a 
	INNER JOIN movies m ON m.id=a.movie_id 
	where nullif(m.score, 'NaN') is not null) AS final_table
GROUP BY final_table.genres
ORDER BY mean_score DESC ;"""

# Géneros por oscars
query_2 = """
SELECT final_table.genres AS Genre, sum(final_table.wins) AS oscars
FROM (
	select * 
	FROM artists a 
	INNER JOIN movies m ON m.id=a.movie_id 
	where nullif(m.score, 'NaN') is not null) AS final_table
GROUP BY final_table.genres
ORDER BY oscars DESC ;
"""

# Géneros por nominaciones
query_3 = """
SELECT final_table.genres AS Genre, sum(final_table.nominations) AS nominations
FROM (
	select * 
	FROM artists a 
	INNER JOIN movies m ON m.id=a.movie_id 
	where nullif(m.score, 'NaN') is not null) AS final_table
GROUP BY final_table.genres
ORDER BY nominations DESC ;
"""

# Actores por premios
query_4 = """
select a.artist , sum(wins) AS awards
FROM artists a 
GROUP BY a.artist
ORDER BY awards DESC ;
"""