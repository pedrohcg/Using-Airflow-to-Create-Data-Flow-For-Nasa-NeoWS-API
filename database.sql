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

CREATE TABLE space_objects(
	id int,
	name varchar(20),
	absolute_magnitude float,
	is_potentially_hazardous_asteroid bit,
	is_sentry_object bit,
	close_approach_date datetime,
)

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

CREATE TABLE links(
	type varchar(10),
	link varchar(30)
)

CREATE TABLE orbiting_body(
	id int identity,
	orbiting_body varchar(10)
)

CREATE TABLE relative_velocity(
	id int,
	relative_velocity_km_per_sec float,
	relative_velocity_km_per_hour float,
	relative_velocity_miles_per_hour float
)
