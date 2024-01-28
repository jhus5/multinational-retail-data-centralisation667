# multinational-retail-data-centralisation667

##Multinational Retail Data Centralisation
##Project Title
Multinational Retail Data Centralisation: A System for Storing and Accessing Sales Data

##Table of Contents
Introduction
Project Description
Installation
Usage
File Structure
License

##1. Introduction
In this project, we develop a system for centralising and accessing sales data from a multinational retail company. Our aim is to make the sales data easily accessible and act as a single source of truth for business metrics.

##2. Project Description
The project involves setting up a database to store the sales data and writing scripts for querying and processing the data. The following libraries are used:

yaml
sqlalchemy
pandas
tabula
requests
json
boto3
re
numpy

##3. Installation
To get started, you'll need to install the required libraries:

pip install sqlalchemy pandas tabula requests boto3 numpy

##4. Usage
First, create a new configuration file config.yaml (such as db_creds.yaml and local_db_credds.yaml) to store your database credentials:

db_uri: postgresql://user:password@localhost/database_name
s3_access_key: YOUR_AWS_ACCESS_KEY
s3_secret_key: YOUR_AWS_SECRET_KEY

Now, you can use the provided scripts to fetch and data from various sources:
AWS RDS
JSON
CSV

##5. File Structure
data_cleaning.py
database_connector.py
database_extraction.py
db_creds.yaml
local_db_creds.yaml
main.py

##6. License
This project is licensed under the MIT License. Feel free to use, modify and distribute the code as needed.
