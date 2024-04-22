CREATE DATABASE near_earth_objects
GO
CREATE LOGIN [airflow_login] WITH PASSWORD=N'senha12345_' , DEFAULT_DATABASE=[master], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO
USE [near_earth_objects]
GO
CREATE USER [airflow_login] FOR LOGIN [airflow_login]
GO
USE [near_earth_objects]
GO
ALTER ROLE [db_datareader] ADD MEMBER [airflow_login]
GO
USE [near_earth_objects]
GO
ALTER ROLE [db_datawriter] ADD MEMBER [airflow_login]
GO
USE near_earth_objects
GO
CREATE TABLE space_objects(
	id int,
	name varchar(50),
	absolute_magnitude float,
	is_potentially_hazardous_asteroid bit,
	is_sentry_object bit,
	close_approach_date datetime,
	orbiting_body varchar(30)
)
GO
CREATE TABLE estimated_size(
	id int,
	estimated_diameter_min_km float,
	estimated_diameter_max_km float,
	estimated_diameter_meters_min float,
	estimated_diameter_meters_max float,
	estimated_diameter_miles_min float,
	estimated_diameter_miles_max float,
	estimated_diameter_feet_min float,
	estimated_diameter_feet_max float
)
GO
CREATE TABLE relative_velocity(
	id int,
	relative_velocity_km_per_sec float,
	relative_velocity_km_per_hour float,
	relative_velocity_miles_per_hour float
)
GO
CREATE TABLE miss_distance(
	id int,
	miss_distance_astronomical float,
	miss_distance_lunar float,
	miss_distance_kilometers float,
	miss_distance_miles float
)
GO
CREATE TABLE sentry_object(
	id int,
	method varchar(10),
	pdate datetime,
	cdate datetime,
	first_obs date,
	last_obs date,
	darc float,
	mass float,
	diameter float,
	ip float,
	n_imp int,
	v_inf float,
	ndop int,
	energy float, 
	ndel int,
	ps_cum float,
	ps_max float,
	ts_max float,
	v_imp float,
	nsat int
)
GO
CREATE VIEW space_objects_links AS
SELECT id, name, 'https://ssd.jpl.nasa.gov/tools/sbdb_lookup.html#/?sstr=' + CAST(id AS VARCHAR) nasa_url,
'http://api.nasa.gov/neo/rest/v1/neo/' + CAST(id AS VARCHAR) + '?api_key=DEMO_KEY' self_api_link
FROM space_objects
GO
CREATE VIEW estimated_size_range AS
SELECT s.id, name, CAST(ROUND(estimated_diameter_meters_min, 2) AS VARCHAR) + ' m - ' + CAST(ROUND(estimated_diameter_meters_max,2) AS VARCHAR) + ' m' estimated_size_meters
, CAST(ROUND(estimated_diameter_min_km, 2) AS VARCHAR) + ' km - ' + CAST(ROUND(estimated_diameter_max_km,2) AS VARCHAR) + ' km' estimated_size_km
, CAST(ROUND(estimated_diameter_miles_min, 2) AS VARCHAR) + ' mi - ' + CAST(ROUND(estimated_diameter_miles_max,2) AS VARCHAR) + ' mi' estimated_size_miles
, CAST(ROUND(estimated_diameter_feet_min, 2) AS VARCHAR) + ' ft - ' + CAST(ROUND(estimated_diameter_feet_max,2) AS VARCHAR) + ' ft' estimated_size_feet
FROM space_objects s
INNER JOIN estimated_size e
ON s.id = e.id