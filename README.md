PhonePe Pulse Data Visualization with Streamlit and Geomaps in Plotly

PhonePe Pulse Data Visualization with Streamlit and Geomaps in Plotly
This project aims to visualize the data from the PhonePe Pulse GitHub repository
 (https://github.com/PhonePe/pulse) using Streamlit and Plotly in Python. 
 The goal is to extract real data from the PhonePe Pulse GitHub page, process the data, 
 and generate valuable insights through interactive visualizations.

What is PhonePe Pulse?

The PhonePe Pulse website showcases more than 2000+ Crore transactions by consumers on an interactive map of India.
 With over 45% market share, PhonePe's data is representative of the country's digital payment habits. 
 The insights on the website and in the report have been drawn from two key sources - the entirety of PhonePe's transaction 
 data combined with merchant and customer interviews. The report is available as a free download on the PhonePe Pulse website and GitHub.
 
Overall WorkFlow:

Step1: 
Importing the Required Libraries:

import pymysql
import pymysql
import pandas as pd
import sqlalchemy
from sqlalchemy import text
import socket
import os
from os import walk
from pathlib import Path
import pandas as pd
from git.repo.base import Repo

Step 2:

Data extraction and Transaformation:

Cloning the The Phone Pe Pulse Data Github to the Local Directory
Accessing the Json files and Converting them to Pandas DataFrames and then migrate to SQL (MYSQL DATABASE LOCAL). 
And use mysql queries to extract information about the various features and aspects of the Data Extracted.
Conversion of the DataFrames to csv format, 
As per our needs also merge DataFrames to be able to pull the data visualization and Table Data Display 

Step 4:
Database insertion:
To insert the datadrame into SQL first I've created a new database and tables using "mysql-connector-python" library in Python to 
connect to a MySQL database and insert the transformed data using SQL commands
Creating the connection between python and MySQL
Using a connection string , and defining a cursor for the Engine Created for the Mysql Database ,
Cursor to execute Mysql Queries in the Python Environment.


Finally Put all togethers using the Streamlit App:

The Dashboard with all the graphs 
