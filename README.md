# Relational Database Loading

## Project
This project will explore a dataset and implement a relational database for storing data.
The goals of this project include:
1. Identify the features of the dataset and design an entity-relationship model
2. Implement relations to a postgres server
3. Load the data to the relations
4. Execute queries on relations

The data used will be a 2012 Chicago crime dataset. 

## Part 1: Identify features of the dataset and design entity-relationship model
To develop an effective entity-relationship model for the Chicago Crime dataset it is important to determine all entities, their associated attributes, and identify primary and foriegn keys. This can be used to develop an initial set of relations. Once an initial set of relations is determined those relations can be converted to 3NF.

3NF is a database schema design approach for relational databases that uses normalization to reduce the duplication of data, avoid data anomalies, ensure referential integrity, and simplify data management.

To convert the relations to 3NF it is important to ensure they meet the criteria of 1NF and 2NF. The criteria for 1NF, 2NF, and 3NF are:   

1NF: Each table cell should contain a single value, and each record needs to be unique.

2NF: Each table must be 1NF and there should be no partial dependencies. A partial dependency is when you have a composite primary key and one or more of the non-key columns is functionally dependent on one, but not all, of the columns in the composite primary key.

3NF: Each table must be 2NF and there should be no transitive dependencies. An example of a transitive dependency is if P -> Q & Q -> R then P -> R.

Once all relations are converted to 3NF an entity relationship diagram can be developed, which will act as a guide to table implementation.

## Entity Relationship Diagram
![Diagram](/image/chicago_crime_erd.png)

## Part 2: Using DDL to create relations in postgres server
Using the entity relationship diagram developed in Part 1 as a guide, tables can now be created to begin loading data into the tables.

For this section [SQLAlchemy](https://www.sqlalchemy.org/) will be utilized.

This portion will consist of:
- Creating the tables into the postgres server
- Load the data from the dataset to the tables
- Ensuring data has been properly loaded into the server

A table schema for the entity relations:
```
CREATE TABLE steinn.case_location (
id INT PRIMARY KEY,
case_number OBJECT FOREIGN KEY,
block OBJECT,
updated_on DATETIME,
beat INT,
district INT,
ward FLOAT,
community_area FLOAT
location_description FLOAT
);

CREATE TABLE steinn.case_coordinates (
block OBJECT PRIMARY KEY,
latitude FLOAT,
longitude FLOAT
);

CREATE TABLE steinn.case_information (
case_number OBJECT PRIMARY KEY,
IUCR OBJECT FOREIGN KEY,
fbi_code OBJECT FOREIGN KEY,
date OBJECT,
arrest BOOL,
domestic BOOL
);

CREATE TABLE steinn.case_reporting(
IUCR OBJECT PRIMARY KEY,
fbi_code OBJECT PRIMARY KEY,
description OBJECT,
primary_type OBJECT
);
```

## Part 3 Execute queries on relations
To both practice SQL queries and ensure queries can be implemented on the database without errors several queries will be executed in this part.
