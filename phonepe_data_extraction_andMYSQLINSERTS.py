import streamlit as st
import os
import json
# from streamlit_option_menu import option_menu
import subprocess
import sqlalchemy
import pymysql
import mysql
import mysql.connector
from mysql.connector import connection
import plotly.express as px
import pandas as pd
import sqlite3
import requests
from PIL import Image
from sqlalchemy import create_engine
from git.repo.base import Repo
from sqlalchemy.dialects import mysql

# Repo.clone_from("https://github.com/PhonePe/maxwell.git", r"C:/Users/91897/Downloads/phonepe_pulse11")

# ----------------------------Aggregate data extraction---------------------------------#
path_to_json1 = r"C:/Users/91897/Downloads/phonepepulse/data/aggregated/transaction/country/india/state/"
aggregate_state_list = os.listdir(path_to_json1)

column1 = {'State': [], 'Year': [], 'Quater': [], 'Transaction_type': [], 'Transaction_count': [],
           'Transaction_amount': []
           }

for i in aggregate_state_list:
    path_i = path_to_json1 + i + '/'
    aggregate_year = os.listdir(path_i)

    for j in aggregate_year:
        path_j = path_i + j + '/'
        aggregate_year_list = os.listdir(path_j)

        for k in aggregate_year_list:
            path_k = path_j + k
            json_data = open(path_k, 'r')
            dict1 = json.load(json_data)

            for m in dict1['data']['transactionData']:
                name = m['name']
                count = m['paymentInstruments'][0]['count']
                amount = m['paymentInstruments'][0]['amount']
                column1['Transaction_type'].append(name)
                column1['Transaction_count'].append(count)
                column1['Transaction_amount'].append(amount)
                column1['State'].append(i)
                column1['Year'].append(j)
                column1['Quater'].append(int(k.strip('.json')))

aggregated_transaction_DF = pd.DataFrame(column1)

# --------------EXTRACTING USER DATA--------------------------------------------------------#

path_to_json2 = r"C:/Users/91897/Downloads/phonepepulse/data/aggregated/user/country/india/state/"
users = os.listdir(path_to_json2)

column2 = {'State': [], 'Year': [], 'Quater': [], 'brands': [], 'Count': [], 'Percentage': []}

my_list = list()
for i in users:
    path_i = path_to_json2 + i + "/"
    aggregate_year = os.listdir(path_i)

    for j in aggregate_year:
        path_j = path_i + j + "/"
        aggregate_year_list = os.listdir(path_j)

        for m in aggregate_year_list:
            path_m = path_j + m
            json_data = open(path_m, 'r')
            dict2 = json.load(json_data)
            try:
                my_list = dict2['data']['usersByDevice'][:]
            except:
                pass

                # print(dict2['data']['usersByDevice'][:])
            try:
                for n in range(0, len(my_list)):
                    brand_name = my_list[n]['brand']
                    count = my_list[n]['count']
                    percentages = my_list[n]['percentage']
                    column2['brands'].append(brand_name)
                    column2['Count'].append(count)
                    column2['Percentage'].append(percentages)
                    column2['State'].append(i)
                    column2['Year'].append(j)
                    column2['Quater'].append(int(m.strip('.json')))
            except:
                pass

aggregated_users_DF = pd.DataFrame(column2)

# ---------------------------------EXTRACTING DATA OF MAP------------------------------------#

path_to_json3 = r"C:/Users/91897/Downloads/phonepepulse/data/map/transaction/hover/country/india/state/"
map_hovering_list = os.listdir(path_to_json3)

column3 = {'State': [], 'Year': [], 'Quater': [], 'District': [], 'count': [], 'amount': []}

for i in map_hovering_list:
    path_i = path_to_json3 + i + "/"
    aggregate_year = os.listdir(path_i)

    for j in aggregate_year:
        path_j = path_i + j + "/"
        aggregate_year_list = os.listdir(path_j)

        for m in aggregate_year_list:
            path_m = path_j + m
            json_data = open(path_m, "r")
            dict3 = json.load(json_data)

            for n in dict3['data']['hoverDataList']:
                district = n['name']
                count = n['metric'][0]['count']
                amount = n['metric'][0]['amount']
                column3['District'].append(district)
                column3['count'].append(count)
                column3['State'].append(i)
                column3['Year'].append(j)
                column3['amount'].append(amount)
                column3['Quater'].append(int(m.strip('.json')))

map_transactions_DF = pd.DataFrame(column3)

# ------------------------EXTRACTING MAP USER INFORMATION------------------------------#

path_to_json4 = r"C:/Users/91897/Downloads/phonepepulse/data/map/user/hover/country/india/state/"
list_maps = os.listdir(path_to_json4)

column4 = {'State': [], 'Year': [], 'Quater': [], 'District': [], 'RegisteredUser': []}

for i in list_maps:
    path_i = path_to_json4 + i + "/"
    aggregate_year = os.listdir(path_i)

    for j in aggregate_year:
        path_j = path_i + j + "/"
        aggregate_year_list = os.listdir(path_j)

        for m in aggregate_year_list:
            path_m = path_j + m
            json_data = open(path_m, 'r')
            dict4 = json.load(json_data)

            for n in dict4['data']['hoverData'].items():
                district = n[0]
                registeredUser = n[1]['registeredUsers']
                column4['District'].append(district)
                column4['RegisteredUser'].append(registeredUser)
                column4['State'].append(i)
                column4['Year'].append(j)
                column4['Quater'].append(int(m.strip('.json')))

users_map_DF = pd.DataFrame(column4)

# ------------------EXTRACTING TOP TRANSACTIONS DATA-----------------#
path_to_json5 = r"C:/Users/91897/Downloads/phonepepulse/data/top/transaction/country/india/state/"
list_of_tops = os.listdir(path_to_json5)

column5 = {'State': [], 'Year': [], 'Quater': [], 'District': [], 'Transaction_count': [],
           'Transaction_amount': []
           }
for i in list_of_tops:
    path_i = path_to_json5 + i + "/"
    aggregate_year = os.listdir(path_i)

    for j in aggregate_year:
        path_j = path_i + j + "/"
        aggregate_year_list = os.listdir(path_j)

        for m in aggregate_year_list:
            path_m = path_j + m
            json_data = open(path_m, 'r')
            dict5 = json.load(json_data)

            for n in dict5['data']['pincodes']:
                name = n['entityName']
                count = n['metric']['count']
                amount = n['metric']['amount']
                column5['District'].append(name)
                column5['Transaction_count'].append(count)
                column5['Transaction_amount'].append(amount)
                column5['State'].append(i)
                column5['Year'].append(j)
                column5['Quater'].append(int(m.strip('.json')))

top_user_transactions_DF = pd.DataFrame(column5)

# ----------------EXTRACTING DATA FOR TOP USERS------------------------#

path_to_json6 = r"C:/Users/91897/Downloads/phonepepulse/data/top/user/country/india/state/"
list_of_users = os.listdir(path_to_json6)

column6 = {'State': [], 'Year': [], 'Quater': [], 'District': [], 'RegisteredUser': []}

for i in list_of_users:
    path_i = path_to_json6 + i + "/"
    aggregate_year = os.listdir(path_i)

    for j in aggregate_year:
        path_j = path_i + j + "/"
        aggregate_year_list = os.listdir(path_j)

        for m in aggregate_year_list:
            path_m = path_j + m
            json_data = open(path_m, 'r')
            dict6 = json.load(json_data)

            for n in dict6['data']['pincodes']:
                name = n['name']
                registeredUser = n['registeredUsers']
                column6['District'].append(district)
                column6['RegisteredUser'].append(registeredUser)
                column6['State'].append(i)
                column6['Year'].append(j)
                column6['Quater'].append(int(m.strip('.json')))

top_users_DF = pd.DataFrame(column6)


# ------------------CONNECTING TO MYSQL DATABASE--------------------------------------------#
import sqlalchemy
from sqlalchemy import create_engine

engine = create_engine('mysql+mysqlconnector://root:mp141534@localhost/phonepe_pulse', echo=False)

import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mp141534",
    database="phonepe_pulse"
)

my_cursor = mydb.cursor()

aggregated_users_DF.to_sql('aggregated_users_tbl', engine, if_exists='replace', index=False,
                               dtype={"State": sqlalchemy.types.VARCHAR(length=1000),
                                      "Year": sqlalchemy.types.VARCHAR(length=100),
                                      "Quater": sqlalchemy.types.INT,
                                      "brands": sqlalchemy.types.VARCHAR(length=1000),
                                      "Count": sqlalchemy.types.BigInteger,
                                      "Percentage": sqlalchemy.types.DECIMAL(65, 30)})



map_transactions_DF.to_sql('map_transactions_tbl', engine, if_exists='replace', index=False,
                               dtype={"State": sqlalchemy.types.VARCHAR(length=1000),
                                      "Year": sqlalchemy.types.VARCHAR(length=100),
                                      "Quater": sqlalchemy.types.INT,
                                      "District": sqlalchemy.types.VARCHAR(length=1000),
                                      "count": sqlalchemy.types.BigInteger,
                                      "amount": sqlalchemy.types.DECIMAL(65, 30)})


users_map_DF.to_sql('users_map_tbl', engine, if_exists='replace', index=False,
                        dtype={"State": sqlalchemy.types.VARCHAR(length=1000),
                               "Year": sqlalchemy.types.VARCHAR(length=100),
                               "Quater": sqlalchemy.types.INT,
                               "District": sqlalchemy.types.VARCHAR(length=1000),
                               "RegisteredUser": sqlalchemy.types.BigInteger})


top_user_transactions_DF.to_sql('top_user_transactions_tbl', engine, if_exists='replace', index=False,
                                    dtype={"State": sqlalchemy.types.VARCHAR(length=1000),
                                           "Year": sqlalchemy.types.VARCHAR(length=100),
                                           "Quater": sqlalchemy.types.INT,
                                           "District": sqlalchemy.types.VARCHAR(length=2000),
                                           "Transaction_count": sqlalchemy.types.DECIMAL(65, 30),
                                           "Transaction_amount": sqlalchemy.types.DECIMAL(65, 30)})

top_users_DF.to_sql('top_users_tbl', engine, if_exists='replace', index=False,
                        dtype={"State": sqlalchemy.types.VARCHAR(length=1000),
                               "Year": sqlalchemy.types.VARCHAR(length=100),
                               "Quater": sqlalchemy.types.INT,
                               "District": sqlalchemy.types.VARCHAR(length=2000),
                               "RegisteredUser": sqlalchemy.types.BigInteger})

aggregated_transaction_DF.to_sql('aggregated_transaction_tbl', engine, if_exists='replace', index=False,
                                     dtype={"State": sqlalchemy.types.VARCHAR(length=1000),
                                            "Year": sqlalchemy.types.VARCHAR(length=100),
                                            "Quater": sqlalchemy.types.INT,
                                            "Transaction_type": sqlalchemy.types.VARCHAR(length=1000),
                                            "Transaction_count": sqlalchemy.types.BigInteger,
                                            "Transaction_amount": sqlalchemy.types.DECIMAL(65, 30)})
