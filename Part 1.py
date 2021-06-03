'''
A chicago crime dataset will be explored and implmented into a relational database. This includes finding the features (attributes) in the dataset and
design an entity-relationship model. Refine the model and convert each relation to 3NF. Use DDL to implement the relations in a postgres server, load the given data to the
relations, and execute some queries.
'''

# dataset exploration
import pandas as pd
datapath = "/dsa/data/DSA-7030/Chicago-Crime-Sample-2012-updated.csv"
df = pd.read_csv(datapath, index_col = False)

# renaming columns to make things easier like getting rid of spaces
df = df.rename(columns = {'ID': 'id', 'Case Number': 'case_number', 'Date': 'date', 'Block': 'block', 'IUCR': 'IUCR',
              'Primary Type': 'primary_type', 'Description': 'description', 'Location Description': 'location_description',
              'Arrest': 'arrest', 'Domestic': 'domestic', 'Beat': 'beat', 'District': 'district', 'Ward': 'ward',
              'Community Area': 'community_area', 'FBI Code': 'fbi_code', 'X Coordinate': 'x_coordinate',
              'Y Coordinate': 'y_coordinate', 'Year': 'year', 'Updated On': 'updated_on', 'Latitude': 'latitude',
              'Longitude': 'longitude', 'Location': 'location'})

df.info() # info on the dataset

# Having location and year is useless when you have latitude, longitude, x_coordinate, y_coordinate, and date so
# I am getting rid of those columns. 
# location will prevent a table from being 1NF as it will need to be separated anyways
df = df.drop(['location', 'year'], axis = 1)

# I am going to drop x_coordinate and y_coordinate. These coordinates can be converted from the illinois state plane coordinates
# to latitude and longitude based on the origin. I assume the corresponding lattitude and longitude are correct and will just use
# them instead since both columns list location of incident. I checked to make sure no x_coordinate and y_coordinate columns
# contained normal values while the corresponding latitude and longitude contained NaN
df = df.drop(['x_coordinate', 'y_coordinate'], axis = 1)
