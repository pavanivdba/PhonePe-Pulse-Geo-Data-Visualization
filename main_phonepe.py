import streamlit as st
import os
import json
import seaborn as sns
import matplotlib.pyplot as plt
import subprocess
import sqlalchemy
import pymysql
import mysql
import mysql.connector
from matplotlib import pyplot as plt
from mysql.connector import connection
import plotly.express as px
import pandas as pd
import requests
from PIL import Image
from sqlalchemy import create_engine
import plotly.graph_objects as go
from git.repo.base import Repo
from sqlalchemy.dialects import mysql
import plotly.io as pio
from pathlib import Path
import base64
import streamlit as st

# --------------------INFORMATION ABOUT PHONEPE PULSE-----------------------------------------#

logo = Image.open(r"C:/Users/91897/Downloads/Phonepe11.jpg")
profile = Image.open(r"C:/Users/91897/Downloads/Phonepe11.jpg")
top_image = Image.open(r'C:/Users/91897/Downloads/phonepe4.png')

# ______________________________UI SET UP ------------------------------------#


st.set_page_config(page_title='PhonePe Pulse', page_icon=profile, layout="wide",
                   initial_sidebar_state="auto",
                   menu_items=None)


def add_bg_from_local(image_file):
    with open(image_file, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read())
    st.markdown(
        f"""
    <style>
    .stApp {{
        background-image: url(data:image/{r"C:/Users/91897/Downloads/Phonepe11.jpg"};base64,{encoded_string.decode()});
        background-size: cover
    }}
    </style>
    """,
        unsafe_allow_html=True
    )


st.title(':red[PhonePe Pulse Analysis Dashboard]')

page_element = """
<style>
[data-testid="stAppViewContainer"]{
  background-image: url("https://wallpapersafari.com/image/light-blue-abstract-wallpaper.jpg");
  background-size: cover;
}

[data-testid="stHeader"]{
  background-color: rgba(0,0,0,0);
}
</style>
<script>
[data-testid="stSidebar"]> div:first-child{
background-image: url("https://mcdn.wallpapersafari.com/medium/89/87/X7GDE5.jpg");
background-size: cover;
}
</script>
"""

st.markdown(page_element, unsafe_allow_html=True)

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

consolidated_df = pd.read_csv(r"C:\Users\91897\Downloads\consolidated_data.csv")

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

##########################################visualization################################

table_select_dict = {"Aggregate of Transactions": 'aggregated_transaction_tbl',
                     "Aggregated Info of Registered Users": "aggregated_users_tbl",
                     "Transactions Geography": "map_transactions_tbl",
                     "RegisterUsers Information as of Geography": "users_map_tbl",
                     "Top Transactions": "top_user_transactions_tbl",
                     "Top RegisteredUsers": "top_users_tbl"}

select_items = st.selectbox('***Select***', ["Aggregate of Transactions",
                                             "Aggregated Info of Registered Users",
                                             "Transactions Geography",
                                             "RegisterUsers Information as of Geography",
                                             "Top Transactions", "Top RegisteredUsers"]
                            )

st.write(f"You have selected : {select_items}")
Year = st.selectbox("Select an Year", ('2018', '2019', '2020', '2021', '2022'))

Quarter = st.selectbox("Select The Quarter", ('1', '2', '3', '4'))
st.write(f"You have selected {Quarter} Quarter")

filter_by = st.selectbox("***Select***",
                         ['Transaction_count',
                          'Transaction_amount',
                          'RegisteredUsers',
                          'count',
                          'brands',
                          'Percentage',
                          'amount',
                          'brands',
                          'Count'])

st.write(f"You have selected the filter {filter_by}")

sql = f'select * from {table_select_dict[select_items]} where year={Year} and Quater={Quarter}'

engine = create_engine("mysql+pymysql://root:mp141534@localhost:3306/phonepe_pulse", pool_size=1000,
                       max_overflow=2000)

mysql_df = pd.read_sql_query(sql, engine)

for column in mysql_df:
    if mysql_df[column].dtype == 'float64':
        mysql_df[column] = pd.to_numeric(mysql_df[column], downcast='float')
        if mysql_df[column].dtype == 'int64':
            mysql_df[column] = mysql_df.to_numeric(mysql_df[column], downcast='integer')
st.write(mysql_df)


def scatterplot(mysql_df):
    if filter_by == 'Transaction_type':
        fin_filter_val = 'Transaction_amount'
        if filter_by == "amount":
            fin_filter_val = 'amount'
            data = [dict(
                type='scatter',
                x=mysql_df['State'],
                y=mysql_df[fin_filter_val],
                mode='markers',
                transforms=[dict(
                    type='groupby',
                    groups=mysql_df['State'],
                )]
            )]

            fig_dict = dict(data=data)
            pio.show(fig_dict, validate=False)
            st.plotly_chart(fig_dict)

            fig_1 = px.line(mysql_df, x='State', y=fin_filter_val)
            fig_1.show()

    scatterplot(mysql_df)


col1, col2 = st.columns(2)

import mysql.connector

connection = mysql.connector.connect(user="root", password="mp141534", database="phonepe_pulse")
cursor = connection.cursor()

with col1:
    st.subheader(':blue[Phonepe Transaction Data Visualization]')
    States = aggregated_transaction_DF['State'].unique()
    options = st.selectbox("****select options****", States)
    options_df = aggregated_transaction_DF[aggregated_transaction_DF['State'] == options]
    figure1 = px.bar(options_df, x="Transaction_type", y="Transaction_count", color="Transaction_type",
                     title='Phonepe Transaction data Visualization')
    st.plotly_chart(figure1, use_container_width=True)

with col2:
    st.subheader("Data Analysis Transaction Count as per Year/Quarter")
    data_by_year_quater = consolidated_df.groupby(['Year', 'Quarter'])['Transaction_count'].sum().reset_index()
    figure2 = px.bar(data_by_year_quater, y='Transaction_count', x='Year', color='Quarter',
                     title='Data Analysis Transaction Count as per Year/Quarter')
    st.plotly_chart(figure2, use_container_width=True)

############################# BRAND ANALYSIS ######################################

st.subheader(':red[OVERALL BRAND ANALYSIS]')
for column in aggregated_users_DF:
    if aggregated_users_DF[column].dtype == 'float64':
        aggregated_users_DF[column] = pd.to_numeric(aggregated_users_DF[column], downcast='float')
        if aggregated_users_DF[column].dtype == 'int64':
            aggregated_users_DF[column] = aggregated_users_DF.to_numeric(aggregated_users_DF[column],
                                                                         downcast='integer')

fig8 = px.pie(aggregated_users_DF, values="Percentage", names="brands", title="Percentage Share of All Mobile Brands")
fig8.update_traces(textposition='inside', textinfo='percent+label')
fig8.update_layout({
    'plot_bgcolor': 'rgba(0,0,0,0)',
    'paper_bgcolor': 'rgba(0,0,0,0)',
    'font_color': '#333',
    'hoverlabel': {
        'font': {'color': '#fff'},
        'bgcolor': 'royalblue'
    }
})
st.plotly_chart(fig8, use_container_width=True)
st.info(""" Observation:
            User can observe percentage share of each brand for phonepe transaction""")

india = json.load(open(r"C:\Users\91897\Downloads\india_state_geo.json", "r"))
year = st.selectbox("Please select the year", ('2018', '2019', '2020', '2021', '2022'), key="k9")
agg_trans = aggregated_transaction_DF["State"].drop_duplicates().sort_values()
agg_trans_DF = pd.DataFrame(agg_trans)
group_by_state = aggregated_transaction_DF.groupby("State", sort=True).sum()
merged_df = pd.merge(aggregated_users_DF, group_by_state, on="State")
total_df = pd.merge(group_by_state, merged_df, on="State")
total_df1 = total_df[total_df['Year'] == year]

st.write(":blue[line chart to show overall growth of phonepe in last 5 years]")

a = aggregated_transaction_DF.groupby("Year").sum()
b = aggregated_transaction_DF['Year'].drop_duplicates().sort_values()
years = pd.DataFrame(b)
c = pd.merge(years, a, on="Year")

fig_year = px.line(
    c,
    x="Year",
    y="Transaction_count"
)

fig_year.update_layout(
    title='Total Transactions by Year',
    xaxis_title='Year',
    yaxis_title='Total Transactions',

)

fig_year.update_traces(
    line=dict(color='blue', width=2),
    mode='lines+markers',
    marker=dict(size=5, color='red', symbol='circle'),
    fill='tozeroy',
    fillcolor='rgba(0,176,246,0.2)'
)
st.plotly_chart(fig_year, use_container_width=True)

# --------------------------------STATES TRANSACTION COUNT--------------------------------------#
merged_by_states_Trans_count = merged_df.sort_values(by='Transaction_count')

fig_trans_bar = px.bar(merged_by_states_Trans_count,
                       x="State",
                       y='Transaction_count',
                       color="State",
                       color_discrete_sequence=px.colors.qualitative.Pastel,
                       title='Total Transactions By State')
fig_trans_bar.update_layout(xaxis_title='State', yaxis_title="Total Transactions", font=dict(family="Arial", size=14))
with st.expander("SEE BAR GRAPH FOR THE STATES"):
    st.plotly_chart(fig_trans_bar, use_container_width=True)
    st.info(
        ":blue[the above bar graph shows the transaction done in each states in increasing order. Here you can observe the top states having higher transaction]")

Data_Aggregated_Transaction_df = pd.read_csv(
    r'C:/Users/91897/Downloads/phonepepulse_csv_data/Data_Aggregated_Transaction_Table.csv')
Data_Aggregated_User_Summary_df = pd.read_csv(
    r'C:/Users/91897/Downloads/phonepepulse_csv_data/Data_Aggregated_User_Summary_Table.csv')
Data_Aggregated_User_df = pd.read_csv(r'C:/Users/91897/Downloads/phonepepulse_csv_data/Data_Aggregated_User_Table.csv')
Scatter_Geo_Dataset = pd.read_csv(
    r'C:/Users/91897/Downloads/phonepepulse_csv_data/Data_Map_Districts_Longitude_Latitude.csv')
Choropleth_Dataset = pd.read_csv(r'C:/Users/91897/Downloads/phonepepulse_csv_data/Data_Map_IndiaStates_TU.csv')
Data_Map_Transaction_df = pd.read_csv(r'C:/Users/91897/Downloads/phonepepulse_csv_data/Data_Map_Transaction_Table.csv')
Data_Map_User_Table = pd.read_csv(r'C:/Users/91897/Downloads/phonepepulse_csv_data/Data_Map_User_Table.csv')
Indian_States = pd.read_csv(r'C:/Users/91897/Downloads/phonepepulse_csv_data/Longitude_Latitude_State_Table.csv')
colT1, colT2 = st.columns([2, 8])
with colT2:
    st.title(':red[PhonePe Pulse Data Analysis:signal_strength:]')

# BAR CHART - TOP PAYMENT TYPE
import mysql.connector

connection = mysql.connector.connect(user="root", password="mp141534", database="phonepe_pulse")
mycursor = connection.cursor()

Selected_Year = st.selectbox("Select an Year", options=('2018', '2019', '2020', '2021', '2022'), key='kY')
Selected_Quarter = st.selectbox("Select The Quarter", options=('1', '2', '3', '4'), key='Kq')

mycursor.execute(
    f"select Transaction_type, sum(Transaction_count) as Total_Transactions, "
    f"round(sum(Transaction_amount), 7) as Total_amount "
    f"from aggregated_transaction_tbl "
    f"where Year= {Selected_Year} and Quater = {Selected_Quarter} "
    f"group by Transaction_type order by Transaction_type")
df = pd.DataFrame(mycursor.fetchall(),
                  columns=['Transaction_type', 'Total_Transactions', 'Total_amount'])
fig = px.bar(df,
             title='Transaction Types vs Total_Transactions',
             x="Transaction_type",
             y="Total_Transactions",
             orientation='v',
             color='Total_amount',
             color_continuous_scale=px.colors.sequential.Agsunset)
st.plotly_chart(fig, use_container_width=False)

# ######################################## INDIA MAP ANALYSIS ######################################################## #

c1, c2 = st.columns(2)
with c1:
    Year = st.selectbox(
        'Please select the Year',
        ('2018', '2019', '2020', '2021', '2022'))
with c2:
    Quarter = st.selectbox(
        'Please select the Quarter',
        ('1', '2', '3', '4'))
year = int(Year)
quarter = int(Quarter)
Transaction_scatter_districts = Data_Map_Transaction_df.loc[
    (Data_Map_Transaction_df['Year'] == year) & (Data_Map_Transaction_df['Quarter'] == quarter)].copy()
Transaction_Choropleth_States = Transaction_scatter_districts[Transaction_scatter_districts["State"] == "india"]
Transaction_scatter_districts.drop(
    Transaction_scatter_districts.index[(Transaction_scatter_districts["State"] == "india")],
    axis=0, inplace=True)
# Dynamic Scatter geo Data Generation
Transaction_scatter_districts = Transaction_scatter_districts.sort_values(by=['Place_Name'],
                                                                          ascending=False)
Scatter_Geo_Dataset = Scatter_Geo_Dataset.sort_values(by=['District'], ascending=False)
Total_Amount = []
for i in Transaction_scatter_districts['Total_Amount']:
    Total_Amount.append(i)
Scatter_Geo_Dataset['Total_Amount'] = Total_Amount
Total_Transaction = []
for i in Transaction_scatter_districts['Total_Transactions_count']:
    Total_Transaction.append(i)
Scatter_Geo_Dataset['Total_Transactions'] = Total_Transaction
Scatter_Geo_Dataset['Year_Quarter'] = str(year) + '-Q' + str(quarter)
# Dynamic Choropleth
Choropleth_Dataset = Choropleth_Dataset.sort_values(by=['state'], ascending=False)
Transaction_Choropleth_States = Transaction_Choropleth_States.sort_values(by=['Place_Name'], ascending=False)
Total_Amount = []
for i in Transaction_Choropleth_States['Total_Amount']:
    Total_Amount.append(i)
Choropleth_Dataset['Total_Amount'] = Total_Amount
Total_Transaction = []
for i in Transaction_Choropleth_States['Total_Transactions_count']:
    Total_Transaction.append(i)
Choropleth_Dataset['Total_Transactions'] = Total_Transaction
# ------------------------------------- INDIA MAP VISUALIZATION----------------------------------------------------------------#
# scatter plotting the states codes
Indian_States = Indian_States.sort_values(by=['state'], ascending=False)
Indian_States['Registered_Users'] = Choropleth_Dataset['Registered_Users']
Indian_States['Total_Amount'] = Choropleth_Dataset['Total_Amount']
Indian_States['Total_Transactions'] = Choropleth_Dataset['Total_Transactions']
Indian_States['Year_Quarter'] = str(year) + '-Q' + str(quarter)
fig = px.scatter_geo(Indian_States,
                     lon=Indian_States['Longitude'],
                     lat=Indian_States['Latitude'],
                     text=Indian_States['code'],
                     hover_name="state",
                     hover_data=['Total_Amount', "Total_Transactions", "Year_Quarter"],
                     )
fig.update_traces(marker=dict(color="pink", size=0.3))
fig.update_geos(fitbounds="locations", visible=False, )
# scatter plotting districts
Scatter_Geo_Dataset['col'] = Scatter_Geo_Dataset['Total_Transactions']
fig1 = px.scatter_geo(Scatter_Geo_Dataset,
                      lon=Scatter_Geo_Dataset['Longitude'],
                      lat=Scatter_Geo_Dataset['Latitude'],
                      color=Scatter_Geo_Dataset['col'],
                      size=Scatter_Geo_Dataset['Total_Transactions'],
                      hover_name="District",
                      hover_data=["State", "Total_Amount", "Total_Transactions", "Year_Quarter"],
                      title='District',
                      size_max=22, )
fig1.update_traces(marker=dict(color="red", line_width=1))  # rebeccapurple
# coropleth mapping india
fig_ch = px.choropleth(
    Choropleth_Dataset,
    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
    featureidkey='properties.ST_NM',
    locations='state',
    color="Total_Transactions",
)
fig_ch.update_geos(fitbounds="locations", visible=False, )
# combining districts states and coropleth
fig_ch.add_trace(fig.data[0])
fig_ch.add_trace(fig1.data[0])
st.write("### **:red[PhonePe India Map]**")
colT1, colT2 = st.columns([6, 4])
with colT1:
    st.plotly_chart(fig_ch, use_container_width=True)
with colT2:
    st.info(
        """
    Analysis of Map:
    * Color Depth Of The State: Total Transactions.
    * Size of the Circles : Total Transactions District wise.
    * Larger Circles:  Top Transactions(higher transactions)
    * Hovering over the MAP : Total transactions, Total amount, names of the Districts as
      per the Latitude and Longitude taken.
    """
    )
    st.info(
            """
    Take Away:
    * User can observe Transactions of PhonePe in both statewide and District Wide.
    * Highest Transactions By Year and Quarter
    * District Wise Transactions
        """
    )

