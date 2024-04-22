-- Qual foi o dia com a maior quantidade de asteroides passando perto da Terra?
SELECT CAST(close_approach_date AS DATE) date,COUNT(1) asteroid_count
FROM space_objects 
GROUP BY CAST(close_approach_date AS DATE)
ORDER BY asteroid_count DESC

-- Qual foi o asteroide que passou mais próximo da Terra?
SELECT name, miss_distance_kilometers, miss_distance_lunar, close_approach_date
FROM space_objects so
INNER JOIN miss_distance md
ON so.id = md.id
ORDER BY miss_distance_kilometers 

-- Qual foi o asteroide que passou próximo da Terra com a maior velocidade relativa?
SELECT name, relative_velocity_km_per_sec, relative_velocity_km_per_hour
FROM space_objects so
INNER JOIN relative_velocity rv
ON so.id = rv.id
ORDER BY relative_velocity_km_per_hour DESC

-- Qual é o asteroide com o maior tamanho estimado?
SELECT so.name, estimated_diameter_km
FROM space_objects so
INNER JOIN estimated_size es
ON so.id = es.id
INNER JOIN estimated_diameter_range edr
ON so.id = edr.id
ORDER BY estimated_diameter_max_km DESC

-- Qual é o asteroide que liberaria a maior quantidade de energia em megatons de TNT caso acertasse a Terra?
SELECT name, CASE WHEN is_potentially_hazardous_asteroid = 0 THEN 'False'
ELSE 'True'
END is_dangerous, energy, ROUND(energy/57, 2) tsar_bombs	
FROM space_objects so
INNER JOIN sentry_object sto
ON so.id = sto.id