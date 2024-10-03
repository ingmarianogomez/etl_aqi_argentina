CREATE TABLE IF NOT EXISTS "2024_mariano_gomez_schema".city (
    id INT IDENTITY(1,1) PRIMARY KEY,
    city VARCHAR(100) UNIQUE,
    state VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS "2024_mariano_gomez_schema".aqi_weather_data (
    id INT IDENTITY(1,1) PRIMARY KEY,
    city VARCHAR(100),
    current_pollution_ts TIMESTAMP,
    current_pollution_aqius FLOAT,
    current_pollution_mainus VARCHAR(10),
    current_pollution_aqicn FLOAT,
    current_pollution_maincn VARCHAR(10),
    current_weather_ts TIMESTAMP,
    current_weather_tp FLOAT,
    current_weather_pr FLOAT,
    current_weather_hu FLOAT,
    current_weather_ws FLOAT,
    current_weather_wd FLOAT,
    FOREIGN KEY (city) REFERENCES "2024_mariano_gomez_schema".city(city)
);


CREATE TABLE IF NOT EXISTS "2024_mariano_gomez_schema".aqi_info_diaria (
    id INT IDENTITY(1,1) PRIMARY KEY,
    city VARCHAR(100), 
    current_pollution_ts TIMESTAMP,
    current_pollution_aqius FLOAT,
    current_pollution_aqicn FLOAT,
    current_weather_tp FLOAT,
    current_weather_pr FLOAT,
    current_weather_hu FLOAT,
    FOREIGN KEY (city) REFERENCES "2024_mariano_gomez_schema".city(city)
);

-- El siguiente script no es necesario ya que 
-- CREATE TABLE IF NOT EXISTS "2024_mariano_gomez_schema".aqi_metrica_diaria (
--     id INT IDENTITY(1,1) PRIMARY KEY,
--     city VARCHAR(100),
--     fecha DATE, 
--     max_aqius FLOAT,
--     max_aqicn FLOAT,
--     avg_temp FLOAT,
--     max_temp FLOAT,
--     max_pre FLOAT,
--     max_hum FLOAT,
--     avg_3_aqius FLOAT,
--     avg_3_aqicn FLOAT,
--     FOREIGN KEY (city) REFERENCES "2024_mariano_gomez_schema".city(city)
-- );

CREATE TABLE IF NOT EXISTS "2024_mariano_gomez_schema".aqi_metrica_historica(
    id INT IDENTITY(1,1) PRIMARY KEY,
    city VARCHAR(100),
    fecha DATE, 
    max_aqius FLOAT,
    max_aqicn FLOAT,
    avg_temp FLOAT,
    max_temp FLOAT,
    max_pre FLOAT,
    max_hum FLOAT,
    avg_3_aqius FLOAT,
    avg_3_aqicn FLOAT,
    FOREIGN KEY (city) REFERENCES "2024_mariano_gomez_schema".city(city)
);



