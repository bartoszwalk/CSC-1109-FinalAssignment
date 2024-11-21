-- Load the raw data
raw_accidents = LOAD 'Data/US_Accidents_March23.csv' USING PigStorage(',') AS (
    ID:chararray,
    Source:chararray,
    Severity:int,
    Start_Time:chararray,
    End_Time:chararray,
    Start_Lat:double,
    Start_Lng:double,
    End_Lat:double,
    End_Lng:double,
    Distance:double,
    Description:chararray,
    Street:chararray,
    City:chararray,
    County:chararray,
    State:chararray,
    Zipcode:chararray,
    Country:chararray,
    Timezone:chararray,
    Airport_Code:chararray,
    Weather_Timestamp:chararray,
    Temperature:double,
    Wind_Chill:double,
    Humidity:double,
    Pressure:double,
    Visibility:double,
    Wind_Direction:chararray,
    Wind_Speed:double,
    Precipitation:double,
    Weather_Condition:chararray,
    Amenity:boolean,
    Bump:boolean,
    Crossing:boolean,
    Give_Way:boolean,
    Junction:boolean,
    No_Exit:boolean,
    Railway:boolean,
    Roundabout:boolean,
    Station:boolean,
    Stop:boolean,
    Traffic_Calming:boolean,
    Traffic_Signal:boolean,
    Turning_Loop:boolean,
    Sunrise_Sunset:chararray,
    Civil_Twilight:chararray,
    Nautical_Twilight:chararray,
    Astronomical_Twilight:chararray
);

-- Extract year,month and hour from timestamps using SUBSTRING
accidents_with_time = FOREACH raw_accidents GENERATE
    ID,
    Source,
    Severity,
    Start_Time,
    End_Time,
    SUBSTRING(Start_Time, 0, 4) AS year,       -- Extract year
    SUBSTRING(Start_Time, 5, 7) AS month,     -- Extract month
    SUBSTRING(Start_Time, 11, 13) AS hour,    -- Extract hour
    Start_Lat,
    Start_Lng,
    City,
    State,
    Temperature,
    Humidity,
    Pressure,
    Visibility,
    Wind_Speed,
    Precipitation,
    Weather_Condition,
    Traffic_Signal;

-- Filter for complete years (2017-2022)
filtered_years = FILTER accidents_with_time BY year >= '2017' AND year <= '2022';

-- Handle missing numerical values
cleaned_accidents = FOREACH filtered_years GENERATE
    ID,
    Source,
    Severity,
    Start_Time,
    End_Time,
    year,
    month,
    hour,
    Start_Lat,
    Start_Lng,
    City,
    State,
    (Temperature IS NULL ? 0.0 : Temperature) AS Temperature,
    (Humidity IS NULL ? 0.0 : Humidity) AS Humidity,
    (Pressure IS NULL ? 0.0 : Pressure) AS Pressure,
    (Visibility IS NULL ? 0.0 : Visibility) AS Visibility,
    (Wind_Speed IS NULL ? 0.0 : Wind_Speed) AS Wind_Speed,
    (Precipitation IS NULL ? 0.0 : Precipitation) AS Precipitation,
    (Weather_Condition IS NULL ? 'Unknown' : Weather_Condition) AS Weather_Condition,
    Traffic_Signal;

-- Group weather conditions to standardize categories
normalized_weather = FOREACH cleaned_accidents GENERATE
    ID,
    Source,
    Severity,
    Start_Time,
    End_Time,
    year,
    month,
    hour,
    Start_Lat,
    Start_Lng,
    City,
    State,
    Temperature,
    Humidity,
    Pressure,
    Visibility,
    Wind_Speed,
    Precipitation,
    CASE 
        WHEN LOWER(Weather_Condition) MATCHES '.*rain.*' THEN 'Rain'
        WHEN LOWER(Weather_Condition) MATCHES '.*snow.*' THEN 'Snow'
        WHEN LOWER(Weather_Condition) MATCHES '.*fog.*' THEN 'Fog'
        WHEN LOWER(Weather_Condition) MATCHES '.*cloud.*' THEN 'Cloudy'
        WHEN LOWER(Weather_Condition) MATCHES '.*(clear|fair).*' THEN 'Clear'
        WHEN LOWER(Weather_Condition) MATCHES '.*hail.*' THEN 'Hail'
        WHEN LOWER(Weather_Condition) MATCHES '.*(dust|sand).*' THEN 'Dust/Sand'
        WHEN LOWER(Weather_Condition) MATCHES '.*haze.*' THEN 'Haze'
        WHEN LOWER(Weather_Condition) MATCHES '.*(thunder|t-storm).*' THEN 'Thunderstorm'
        ELSE 'Other'
    END AS Weather_Category,
    Traffic_Signal;

-- Store the cleaned data
STORE normalized_weather INTO 'results/cleaned_accidents' USING PigStorage(',');