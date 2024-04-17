CREATE DATABASE near_earth_objects
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