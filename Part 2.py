'''
In this part I will be using DDL to create each of the relations in the postgres server
'''

# connection to dsa_student database
import sqlalchemy
import pandas as pd
import getpass
from sqlalchemy import event
from sqlalchemy import Table, Column, Integer, String, Boolean, MetaData, ForeignKey, DateTime, Float, ForeignKeyConstraint

database = input("Type Database name and hit enter:: ")

username = input("Type Username name and hit enter:: ")

password = getpass.getpass("Type Password and hit enter:: ")

connectionstring = 'postgresql://'+username+':'+password+'@pgsql.dsa.lan/'+database

# creating tables
# Going to use sqlalchemy cause I like it

# creating engine
engine = sqlalchemy.create_engine((connectionstring), echo = False)

@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True
        cursor.commit()
 
# using sqlalchemy to create tables
meta = MetaData()

case_information = Table(
    'case_information', meta,
    Column('id', Integer, primary_key = True),
    Column('case_number', String),
    Column('arrest', Boolean),
    Column('domestic', Boolean),
    Column('date', DateTime),
    Column('updated_on', DateTime),
    Column('location_id', Integer, ForeignKey('case_location.location_id')),
    Column('IUCR', String),
    Column('fbi_code', String),
    ForeignKeyConstraint(['IUCR', 'fbi_code'], ['case_reporting.IUCR', 'case_reporting.fbi_code'])
)

case_reporting = Table(
    'crime_code', meta,
    Column('IUCR', String, primary_key = True),
    Column('fbi_code', String, primary_key = True),
    Column('description', String),
    Column('primary_type', String),
)

case_location = Table(
    'case_location', meta,
    Column('location_id', Integer, primary_key = True),
    Column('district', Integer),
    Column('beat', Integer),
    Column('block', String),
    Column('ward', Float),
    Column('community_area', Float),
    Column('location_description', String),
    Column('latitude', Float),
    Column('longitude', Float),
)

meta.create_all(engine)

# loading the data from the csv file to the realtions
pd.options.mode.chained_assignment = None 
datapath = "/dsa/data/DSA-7030/Chicago-Crime-Sample-2012-updated.csv"
df = pd.read_csv(datapath, index_col = False)

# renaming columns to make things easier like getting rid of spaces
df = df.rename(columns = {'ID': 'id', 'Case Number': 'case_number', 'Date': 'date', 'Block': 'block',
              'Primary Type': 'primary_type', 'Description': 'description', 'Location Description': 'location_description',
              'Arrest': 'arrest', 'Domestic': 'domestic', 'Beat': 'beat', 'District': 'district', 'Ward': 'ward',
              'Community Area': 'community_area', 'FBI Code': 'fbi_code', 'X Coordinate': 'x_coordinate',
              'Y Coordinate': 'y_coordinate', 'Year': 'year', 'Updated On': 'updated_on', 'Latitude': 'latitude',
              'Longitude': 'longitude', 'Location': 'location'})

# removing columns I won't need
df = df.drop(['location', 'year', 'x_coordinate', 'y_coordinate'], axis = 1)

##################################################################################################################################################

## case_location relation subsetting and loading
# dataframe subset for case_location relation
case_location_df = df[['district', 'beat', 'block', 'ward', 'community_area', 'location_description', 'latitude', 'longitude']]
case_location_df = case_location_df.drop_duplicates()
case_location_df.insert(0, 'location_id', range(1, 1+len(case_location_df)))

#saving to csv
case_location_df.to_csv('case_location_df.csv')

# load into the database
case_location_df.to_sql('case_location', con = engine, chunksize = 100, index = False, if_exists = 'replace', method = 'multi')

##################################################################################################################################################

## case_information relation subsetting and loading
# dataframe subset for case_information relation
case_information_df = df[['id', 'case_number', 'arrest', 'domestic', 'date', 'updated_on', 'IUCR', 'fbi_code', 'location_id']]
case_information_df = case_information_df.drop_duplicates()

# change 'date' column to datetime
case_information_df['date'] = pd.to_datetime(case_information_df['date'])
case_information_df['updated_on'] = pd.to_datetime(case_information_df['updated_on'])

#saving to csv
case_information_df.to_csv('case_information_df.csv')

# load into the database
case_information_df.to_sql('case_information', con = engine, chunksize = 1000, index = False, if_exists = 'replace', method = 'multi')

##################################################################################################################################################

## case_reporting relation subsetting and loading
case_reporting_check = df.groupby(['IUCR', 'fbi_code', 'primary_type']).size()  

print(f"num of triplets = {case_reporting_check.shape[0]}")

# searching for attributes mapped to multiple primary types
count = 0
for x, y in case_reporting_check.groupby(level=[0,1]):
    if y.shape[0] > 1:
        if count < 5:  # show the first 5 patterns
            print("-" * 20)
            print(x)
            print(y)
        count +=1 
print(f"#iucr, fbi code mapped to multiple primary types = {count}")

case_reporting_check2 = df.groupby(['IUCR', 'fbi_code', 'description']).size()  

print(f"num of triplets = {case_reporting_check2.shape[0]}")

# handling the attributes mapped to multiple primary types

a = {'CRIMINAL SEXUAL ASSAULT': 'CRIM SEXUAL ASSAULT'}
b = {'AGGRAVATED - HANDGUN': 'AGGRAVATED: HANDGUN'}
c = {'AGGRAVATED - OTHER':'AGGRAVATED: OTHER'}
d = {'ARMED - HANDGUN':'ARMED: HANDGUN'}
e = {'AGGRAVATED - HANDGUN':'AGGRAVATED: HANDGUN'}
f = {'FINANCIAL ID THEFT: OVER $300':'FINANCIAL IDENTITY THEFT: OVER $300'}

df['primary_type'] = df['primary_type'].replace(a)
df['description'] = df['description'].replace(b)
df['description'] = df['description'].replace(c)
df['description'] = df['description'].replace(d)
df['description'] = df['description'].replace(e)
df['description'] = df['description'].replace(f)

# dataframe subset for case_reporting relation
case_reporting_df = df[['IUCR', 'fbi_code', 'description', 'primary_type']]
case_reporting_df = case_reporting_df.drop_duplicates()

# saving to csv
case_reporting_df.to_csv('case_reporting_df.csv')

# load into the database
case_reporting_df.to_sql('case_reporting', con = engine, chunksize = 1000, index = False, if_exists = 'replace', method = 'multi')

##################################################################################################################################################

# Showing the number of rows in the table using sql query

# for case_location
with engine.connect() as connection:
    case_location_rows = pd.read_sql_query("SELECT * FROM case_location", connection)
case_location_rows

#for case_information
with engine.connect() as connection:
    case_information_rows = pd.read_sql_query("SELECT * FROM case_information", connection)
case_information_rows

#for case_reporting
with engine.connect() as connection:
    case_reporting_rows = pd.read_sql_query("SELECT * FROM case_reporting", connection)
case_reporting_rows
