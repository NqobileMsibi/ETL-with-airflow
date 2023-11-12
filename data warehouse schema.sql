CREATE TABLE fact_movie_analytics (
customerid INTEGER,
id_dim_devices INTEGER REFERENCES dim_devices(id_dim_devces),
id_dim_location INTEGER REFERENCES dim_location(id_dim_location),
id_dim_os INTEGER REFERENCES dim_os(id_dim_os),
id_dim_date INTEGER REFERENCES dim_date(id_dim_date),
amount_spent DECIMAL(18, 5),
review_score INTEGER,
review_count INTEGER
);

CREATE TABLE dim_date (
id_dim_date INTEGER,
log_date DATE,
day VARCHAR,
month VARCHAR,
year VARCHAR,
season VARCHAR
);

CREATE TABLE dim_devices (
id_dim_devices SERIAL INTEGER,
device VARCHAR
);

CREATE TABLE dim_location (
id_dim_location SERIAL INTEGER,
location VARCHAR
);

CREATE TABLE dim_os (
id_dim_devices SERIAL INTEGER,
os VARCHAR
);

CREATE TABLE dim_browser (
id_dim_devices SERIAL INTEGER,
browser VARCHAR
);
