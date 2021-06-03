'''
executing some queries on the database implemented in part 2
'''
import getpass

SSO=""
hostname=''
database='dsa_student'

read_password = getpass.getpass("Type Password and hit enter")
connection_string = f"postgres://{SSO}:{read_password}@{hostname}/{database}"
  
%load_ext sql
%sql $connection_string


///////////////////////////////////////////////////////

# how many types of location_description have residence or residential

%%sql
SELECT DISTINCT location_description FROM case_location
WHERE location_description LIKE '%RESIDENCE%' 
    OR location_description LIKE '%RESIDENTIAL%';

///////////////////////////////////////////////////////

# Primary type that happend of february 13th

%%sql
SELECT DISTINCT cr.primary_type
FROM case_reporting cr
JOIN case_information ci ON cr.IUCR = ci.IUCR AND cr.fbi_code = ci.fbi_code
WHERE date::date = '2012-02-013'
GROUP BY cr.primary_type

///////////////////////////////////////////////////////

# month that has largest amount of thefts

%%sql
SELECT to_char(date, 'MM') as month, primary_type
FROM case_information
NATURAL JOIN (
    SELECT primary_type, count(*)
    FROM case_reporting
    WHERE primary_type = 'THEFT'
    GROUP BY primary_type) as p_type
GROUP BY to_char(date, 'MM'), primary_type
ORDER BY count(*) DESC
LIMIT 1;

///////////////////////////////////////////////////////

# crime that happened most on christmas

%%sql
SELECT primary_type, count(*)
FROM case_reporting cr
JOIN case_information ci ON ci.IUCR = cr.IUCR AND ci.fbi_code = cr.fbi_code
NATURAL JOIN (
    SELECT date
    FROM case_information
    WHERE date::date = '2012-12-25'
    ) as date_info
GROUP BY primary_type
ORDER BY count(*) DESC
LIMIT 1;

///////////////////////////////////////////////////////

# number of crimers per month for each community with an average number over 500

%%sql
SELECT community_area, ROUND(AVG(crime_count), 1) as average_crime_count
FROM case_location
NATURAL JOIN (
    SELECT community_area, to_char(date, 'MM') as month, count(primary_type) as crime_count
    FROM case_location cl
    JOIN case_information ci USING (location_id)
    JOIN case_reporting cr ON cr.IUCR = ci.IUCR AND cr.fbi_code = ci.fbi_code
    GROUP BY community_area, to_char(date, 'MM')
    ORDER BY crime_count DESC
) as c_count
GROUP BY community_area
ORDER BY average_crime_count DESC
LIMIT 22;

///////////////////////////////////////////////////////

# arrest rate for domestric battery

%%sql
SELECT arrest, domestic, primary_type, count(*) as crime_count, (591600/22921) as percent_arrest_rate
FROM case_information ci
JOIN case_reporting cr ON cr.IUCR = ci.IUCR AND cr.fbi_code = ci.fbi_code
NATURAL JOIN(
    SELECT primary_type, count(*)
    FROM case_reporting cr
    WHERE primary_type = 'BATTERY'
    GROUP BY primary_type) as p_type
WHERE (arrest = 'FALSE' AND domestic = 'TRUE') OR (arrest = 'TRUE' AND domestic = 'TRUE')
GROUP BY arrest, domestic, primary_type

///////////////////////////////////////////////////////

# Plot two time series for crime count and arrest count per month
import psycopg2
import sqlalchemy
import pandas as pd

db_url = f'postgres://{SSO}:{read_password}@{hostname}/{database}'
conn = sqlalchemy.create_engine(db_url, client_encoding = 'utf8')

SQL = "SELECT EXTRACT(MONTH FROM date) as month, count(arrest) as arrest_count"
SQL += " FROM case_information"
SQL += " WHERE arrest = TRUE"
SQL += " GROUP BY EXTRACT(MONTH FROM date)"
SQL += " ORDER BY EXTRACT(MONTH FROM date)"

print(SQL)
arrest_count = pd.read_sql(SQL, conn)
arrest_count

SQL = """
SELECT EXTRACT(MONTH FROM date) as month, count(crime) as crime_count
FROM case_information
NATURAL JOIN (
    SELECT IUCR, fbi_code, primary_type 
    FROM case_reporting
    GROUP BY IUCR, fbi_code, primary_type
    ) as crime
GROUP BY EXTRACT(MONTH FROM date)
ORDER BY EXTRACT(MONTH FROM date)
"""

print(SQL)
crime_count = pd.read_sql(SQL, conn)
crime_count

import matplotlib.pyplot as plt
%matplotlib inline
ax = arrest_count.plot(x = 'month', y = 'arrest_count', figsize = (12,8))
crime_count.plot (ax = ax, x = 'month', y = 'crime_count', figsize = (12,8))
plt.ylabel('counts')
