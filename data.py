import pandas as pd
import os
import json
import psycopg2
import streamlit as st
import plotly.express as px

class data_creation:

    def aggregate_transaction():
        path = "pulse/data/aggregated/transaction/country/india/state/"

        # list of files the state folder
        aggregate_transc_state_list = os.listdir(path)

        # Creating a variable to store transactions data
        transc_data = []
        # Looping through each state in state folder
        for state in aggregate_transc_state_list:
            path_2 = path+state+"/"    #Path creation
            aggregate_transc_each_state = os.listdir(path_2)
            # Storing the state name in a standarised form for future map plotting
            state = state.replace('-',' ').title().replace('Islands', '').strip()
            state = state.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')
            # print(aggregate_each_state)

            # Looping through each year in a state 
            for year in aggregate_transc_each_state:
                path_3 = path_2+year+'/'      #Path Creation
                aggregate_transc_each_year = os.listdir(path_3)
                # print(aggregate_transc_each_year)
                
                # Looping through each quarter in a year
                for quarter in aggregate_transc_each_year:
                    # Checking json file is present or not
                    if quarter.endswith(".json"):
                        file_path = path_3+quarter   #Path Creation
                        with open(file_path, "r") as file:
                            data = json.load(file)   #File loading
                            for transc in data['data']['transactionData']:
                                name = transc['name']
                                count = transc['paymentInstruments'][0]['count']
                                amount = transc['paymentInstruments'][0]['amount']
                                transc_data.append({
                                'State': state, 'Year': year,'Quarter': quarter[0], 
                                'Transaction_type': name, 'Transaction_count': count,
                                    'Transaction_amount': amount  
                                })
        return transc_data

    # df_agg_transc = pd.DataFrame(transc_data)

    def aggregate_user():

        path = "pulse/data/aggregated/user/country/india/state/"

        # list of files the state folder
        aggregate_user_state_list = os.listdir(path)

        user_data = []
        # df = pd.read_json('pulse/data/aggregated/user/country/india/state/dadra-&-nagar-haveli-&-daman-&-diu/2020/2.json')
        # df['data']['usersByDevice']

        # Looping through each states in  state list
        for state in aggregate_user_state_list:
            path_2 = path+state+"/"
            aggregate_user_each_state = os.listdir(path_2) 
            # Storing the state name in a standarised form for future map plotting
            state = state.replace('-',' ').title().replace('Islands', '').strip()
            state = state.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')
            # print(aggregate_user_each_state)

            # Looping through each year of each state
            for year in aggregate_user_each_state:
                path_3  = path_2+year+"/"
                aggregate_user_each_year = os.listdir(path_3)
                # print(aggregate_user_each_year)
                
                # Looping through all four quarters of a year 
                for quarter in aggregate_user_each_year:
                    if quarter.endswith(".json"):
                        file_path = path_3+quarter   #Path Creation
                        with open(file_path, "r") as file:
                            data = json.load(file)   #Loading json file
                            try:  # Since some data values are none type
                                for user in  data['data']['usersByDevice']:
                                    brand = user['brand']
                                    count = user['count']
                                    percentage = user['percentage']
                                    user_data.append({
                                    'State': state, 'Year': year,'Quarter': quarter[0], 
                                    'Brand': brand, 'Users_count': count,
                                        'Percentage': percentage  
                                    })
                            except:
                                pass
        return user_data

# df_agg_user = pd.DataFrame(user_data)
    
    def aggregate_insurance():
        path = "pulse/data/aggregated/insurance/country/india/state/"

        aggregate_insur_state_list = os.listdir(path)
        aggregate_insur_state_list

        insur_data = []
        # df = pd.read_json("pulse/data/aggregated/insurance/country/india/state/delhi/2021/3.json")
        # df['data']['transactionData']

        # Looping through each states in state list
        for state in aggregate_insur_state_list:
            path_2 = path+state+"/"
            aggregate_insur_each_state = os.listdir(path_2)
            # Storing the state name in a standarised form for future map plotting
            state = state.replace('-',' ').title().replace('Islands', '').strip()
            state = state.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')
            # print(aggregate_insur_each_state)

            # Looping through each year in for a given state
            for year in aggregate_insur_each_state:
                path_3 = path_2+year+"/"
                aggregate_insur_each_year = os.listdir(path_3)
                # print(aggregate_insur_each_year)

                # Looping through each quarter in a year
                for quarter in  aggregate_insur_each_year:
                    if quarter.endswith(".json"):
                        file_path = path_3+quarter #Path Creation
                        with open(file_path, "r") as file:
                            data = json.load(file) 
                            # print(data['data'])
                            for insur in data['data']['transactionData']:
                                name = insur['name']
                                count = insur['paymentInstruments'][0]['count']
                                amount = insur['paymentInstruments'][0]['amount']
                                insur_data.append({
                                    'State': state, 'Year': year,'Quarter': quarter[0], 
                                'Transaction_type': name, 'Transaction_count': count,
                                    'Insurance_amount': amount 
                                })
        return insur_data
    # df_agg_insur = pd.DataFrame(insur_data)

    def map_transaction():

        path = "pulse/data/map/transaction/hover/country/india/state/"
        map_transc_state_list = os.listdir(path)
        map_transc_state_list
        # df = pd.read_json("pulse/data/map/transaction/hover/country/india/state/chhattisgarh/2018/1.json")
        # print(df['data']['hoverDataList'][0])
        transc_data = []
        # Looping through each states in state list
        for state in map_transc_state_list:
            path_2 = path+state+"/"  #Path Creation
            map_transc_each_state = os.listdir(path_2)
            # Storing the state name in a standarised form for future map plotting
            state = state.replace('-',' ').title().replace('Islands', '').strip()
            state = state.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')
            # print(map_transc_each_state)

            # Looping through each year for a given state
            for year in map_transc_each_state:
                path_3 = path_2+year+"/"
                map_transc_each_year = os.listdir(path_3)
                # print(map_transc_each_year)

                # Looping through each quarter in a year
                for quarter in map_transc_each_year:
                    if quarter.endswith(".json"):
                        file_path = path_3+quarter #Path Creation
                        with open(file_path, "r") as file:
                            data = json.load(file)
                            for transc in data['data']['hoverDataList']:
                                name = transc['name'].replace('district','').strip()
                                count = transc['metric'][0]['count']
                                amount = transc['metric'][0]['amount']
                                transc_data.append({
                                'State': state, 'Year': year, 'Quarter': quarter[0],
                                'District_name': name, 'Transaction_count': count,
                                'Transaction_amount': amount
                                })
        return transc_data

        # df_map_transc = pd.DataFrame(transc_data)
    
    def map_user():

        path = "pulse/data/map/user/hover/country/india/state/"
        # list of files the state folder
        map_user_state_list = os.listdir(path)

        user_data = []
        # df = pd.read_json('pulse/data/map/user/hover/country/india/state/delhi/2023/2.json')
        # for i, j in df['data']['hoverData'].items():
        #     print(i,j )

        # Looping through each states in  state list
        for state in map_user_state_list:
            path_2 = path+state+"/"
            map_user_each_state = os.listdir(path_2) 
            # Storing the state name in a standarised form for future map plotting
            state = state.replace('-',' ').title().replace('Islands', '').strip()
            state = state.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')
            # print(map_user_each_state)

            # Looping through each year of each state
            for year in map_user_each_state:
                path_3  = path_2+year+"/"
                map_user_each_year = os.listdir(path_3)
                # print(map_user_each_year)
                
                # Looping through all four quarters of a year 
                for quarter in map_user_each_year:
                    if quarter.endswith(".json"):
                        file_path = path_3+quarter   #Path Creation
                        with open(file_path, "r") as file:
                            data = json.load(file)   #Loading json file
                            # print(data['data']['hoverData'])
                            # try:  # Since some data values are none type
                            for district, data in  data['data']['hoverData'].items():
                                name = district.replace('district','').strip()
                                registeredUsers = data['registeredUsers']
                                appOpens = data['appOpens']
                                user_data.append({
                                    'State': state, 'Year': year,'Quarter': quarter[0], 
                                    'District_name': name, 'Registered_Users': registeredUsers,
                                    'App_Opens': appOpens  
                                })
                            # except:
                            #     pass
        return user_data
        # df_map_user = pd.DataFrame(user_data)
    
    def map_insurance():

        path = "pulse/data/map/insurance/hover/country/india/state/"
        map_insur_state_list = os.listdir(path)
        map_insur_state_list

        insur_data = []
        df = pd.read_json("pulse/data/map/insurance/hover/country/india/state/delhi/2021/2.json")
        df['data']['hoverDataList']

        # Looping through each states in state list
        for state in map_insur_state_list:
            path_2 = path+state+"/"
            map_insur_each_state = os.listdir(path_2)
            # Storing the state name in a standarised form for future map plotting
            state = state.replace('-',' ').title().replace('Islands', '').strip()
            state = state.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

            # print(map_insur_each_state)

            # Looping through each year in for a given state
            for year in map_insur_each_state:
                path_3 = path_2+year+"/"
                map_insur_each_year = os.listdir(path_3)
                # print(map_insur_each_year)

                # Looping through each quarter in a year
                for quarter in  map_insur_each_year:
                    if quarter.endswith(".json"):
                        file_path = path_3+quarter #Path Creation
                        with open(file_path, "r") as file:
                            data = json.load(file) 
                            # print(data['data']['hoverDataList'])
                            for insur in data['data']['hoverDataList']:
                                name = insur['name'].replace('district','').strip()
                                # print(name)
                                count = insur['metric'][0]['count']
                                amount = insur['metric'][0]['amount']
                                # print(count, amount)
                                insur_data.append({
                                    'State': state, 'Year': year,'Quarter': quarter[0], 
                                'District_name': name, 'Transaction_count': count,
                                    'Insurance_amount': amount 
                                })
        return insur_data
        # df_map_insur = pd.DataFrame(insur_data)
    def top_transaction():
        path = "pulse/data/top/transaction/country/india/state/"

        # list of files the state folder
        top_transc_state_list = os.listdir(path)
        # print(top_transc_state_list)
        # Creating a variable to store transactions data
        transc_data_district, transc_data_pincode = [], []
        # df  = pd.read_json("pulse/data/top/transaction/country/india/state/meghalaya/2021/1.json")
        # for i in df['data']['districts']:
        #     print(i)
        # for j in df['data']['pincodes']:
        #     print(j)

        # Looping through each state in state folder
        for state in top_transc_state_list:
            path_2 = path+state+"/"    #Path creation
            top_transc_each_state = os.listdir(path_2)
            # Storing the state name in a standarised form for future map plotting
            state = state.replace('-',' ').title().replace('Islands', '').strip()
            state = state.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')
            # print(top_transc_each_state)

            # Looping through each year in a state 
            for year in top_transc_each_state:
                path_3 = path_2+year+'/'      #Path Creation
                top_transc_each_year = os.listdir(path_3)
                # print(top_transc_each_year)
                
                # Looping through each quarter in a year
                for quarter in top_transc_each_year:
                    # Checking json file is present or not
                    if quarter.endswith(".json"):
                        file_path = path_3+quarter   #Path Creation
                        with open(file_path, "r") as file:
                            data = json.load(file)   #File loading
                            # print(data['data']['districts'])

                            # looping through each districts
                            for transc in data['data']['districts']:
                                name = transc['entityName']
                                # print(name)
                                count = transc['metric']['count']
                                # print(count)
                                amount = transc['metric']['amount']
                                transc_data_district.append({
                                'State': state, 'Year': year,'Quarter': quarter[0], 
                                'District_name': name, 'Transaction_count': count,
                                    'Transaction_amount': amount  
                                })

                            # looping through each pincodes
                            for transc in data['data']['pincodes']:
                                pincode = transc['entityName']
                                # print(name)
                                count = transc['metric']['count']
                                # print(count)
                                amount = transc['metric']['amount']
                                transc_data_pincode.append({
                                'State': state, 'Year': year,'Quarter': quarter[0], 
                                'Pincode': pincode, 'Transaction_count': count,
                                    'Transaction_amount': amount  
                                })
        return transc_data_district, transc_data_pincode            

        # df_top_transc_district = pd.DataFrame(transc_data_district)
        # df_top_transc_pincode = pd.DataFrame(transc_data_pincode)
    
    def top_user():
        path = "pulse/data/top/user/country/india/state/"

        # list of files the state folder
        top_user_state_list = os.listdir(path)

        user_data_district, user_data_pincode = [], []
        # df = pd.read_json('pulse/data/top/user/country/india/state/dadra-&-nagar-haveli-&-daman-&-diu/2020/2.json')
        # df['data']['pincodes']

        # Looping through each states in  state list
        for state in top_user_state_list:
            path_2 = path+state+"/"
            top_user_each_state = os.listdir(path_2) 
            # Storing the state name in a standarised form for future map plotting
            state = state.replace('-',' ').title().replace('Islands', '').strip()
            state = state.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')
            # print(top_user_each_state)

            # Looping through each year of each state
            for year in top_user_each_state:
                path_3  = path_2+year+"/"
                top_user_each_year = os.listdir(path_3)
        
                
                # Looping through all four quarters of a year 
                for quarter in top_user_each_year:
                    if quarter.endswith(".json"):
                        file_path = path_3+quarter   #Path Creation
                        with open(file_path, "r") as file:
                            data = json.load(file)   #Loading json file
                            # print(data['data']['pincodes'])
                            try:  # Since some data values are none type

                                for user in  data['data']['districts']:
                                    name = user['name']
                                    count = user['registeredUsers']
                                    # percentage = user['percentage']
                                    user_data_district.append({
                                    'State': state, 'Year': year,'Quarter': quarter[0], 
                                    'District Name': name, 'Registered Users count': count 
                                    })

                                for user in  data['data']['pincodes']:
                                    code = user['name']
                                    count = user['registeredUsers']
                                    user_data_pincode.append({
                                    'State': state, 'Year': year,'Quarter': quarter[0], 
                                    'Pincode': code, 'Registered Users count': count 
                                    })

                                
                            except:
                                pass

        return user_data_district, user_data_pincode
        # df_top_user_district = pd.DataFrame(user_data_districts)
        # df_top_user_pincodes = pd.DataFrame(user_data_pincodes)


class data_transform:

    aggregate_transaction = pd.DataFrame(data_creation.aggregate_transaction())
    aggregate_user = pd.DataFrame(data_creation.aggregate_user())
    map_transaction = pd.DataFrame(data_creation.map_transaction())
    map_user = pd.DataFrame(data_creation.map_user())
    top_transaction_district, top_transaction_pincode = data_creation.top_transaction()
    top_transaction_district, top_transaction_pincode = pd.DataFrame(top_transaction_district), pd.DataFrame(top_transaction_pincode)
    top_user_district, top_user_pincode = data_creation.top_user()
    top_user_district, top_user_pincode = pd.DataFrame(top_user_district), pd.DataFrame(top_user_pincode)



class data_load:
    def tables_creation():
        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="Phonepe",
                        port="5432")
        cursor=mydb.cursor()

        drop_query = '''drop table if exists aggregate_transaction, aggregate_user,\
                        map_transaction, map_user, top_transaction_district,\
                        top_transaction_pincode, top_user_district, top_user_pincode;'''
        cursor.execute(drop_query)
        mydb.commit()
        
        # Creating all tables inside postgresql Database Phonepe
        create_query = '''create table if not exists aggregate_transaction(
                                        State varchar(255),
                                        Year	smallint,
                                        Quarter smallint,	
                                        Transaction_type	varchar(255),
                                        Transaction_count	bigint,
                                        Transaction_amount  bigint);
                    create table if not exists aggregate_user(
                                        State varchar(255),
                                        Year	 smallint,
                                        Quarter smallint,	
                                        Brand   varchar(100),
                                        Users_count	bigint,
                                        Percentage decimal);
                    create table if not exists   map_transaction(
                                        State varchar(255),
                                        Year	smallint,
                                        Quarter smallint,
                                        District_name varchar(255),
                                        Transaction_count	bigint,
                                        Transaction_amount  bigint);                 
                    create table if not exists map_user(
                                        State varchar(255),
                                        Year	 smallint,
                                        Quarter smallint,	
                                        District_name   varchar(100),
                                        Registered_Users	bigint,
                                        App_Opens int);
                    create table if not exists top_transaction_district(
                                        State varchar(255),
                                        Year	 smallint,
                                        Quarter smallint,
                                        District_name   varchar(100),
                                        Transaction_count bigint,
                                        Transaction_amount bigint);
                    create table if not exists top_transaction_pincode(
                                        State varchar(255),
                                        Year	 smallint,
                                        Quarter smallint,
                                        Pincode   int,
                                        Transaction_count bigint,
                                        Transaction_amount bigint);
                    create table if not exists top_user_district(
                                        State varchar(255),
                                        Year	 smallint,
                                        Quarter smallint,
                                        District_name   varchar(100),
                                        Registered_Users_count bigint);
                    create table if not exists top_user_pincode(
                                        State varchar(255),
                                        Year	 smallint,
                                        Quarter smallint,
                                        Pincode   int,
                                        Registered_Users_count bigint);'''
        
        try:
            cursor.execute(create_query)
            mydb.commit()
            print("All tables created")
        except:
            pass
        

        cursor.executemany("insert into aggregate_transaction(State, Year, Quarter, Transaction_type,\
                            Transaction_count, Transaction_amount)\
                        values(%s,%s,%s,%s,%s,%s)", data_transform.aggregate_transaction.values.tolist())

        cursor.executemany("insert into aggregate_user(State, Year, Quarter, Brand,\
                                    Users_count, Percentage)\
                                values(%s,%s,%s,%s,%s,%s)", data_transform.aggregate_user.values.tolist())

        cursor.executemany("insert into map_transaction(State, Year, Quarter, District_name,\
                                    Transaction_count, Transaction_amount)\
                                values(%s,%s,%s,%s,%s,%s)", data_transform.map_transaction.values.tolist())

        cursor.executemany("insert into map_user(State, Year, Quarter, District_name,\
                                Registered_Users, App_Opens)\
                        values(%s,%s,%s,%s,%s,%s)", data_transform.map_user.values.tolist())

        cursor.executemany("insert into top_transaction_district(State, Year, Quarter, District_name,\
                                Transaction_count, Transaction_amount)\
                                values(%s,%s,%s,%s,%s,%s)", data_transform.top_transaction_district.values.tolist())
        
        cursor.executemany("insert into top_transaction_pincode(State, Year, Quarter, Pincode,\
                                Transaction_count, Transaction_amount)\
                                values(%s,%s,%s,%s,%s,%s)", data_transform.top_transaction_pincode.values.tolist())
        
        cursor.executemany("insert into top_user_district(State, Year, Quarter, District_name,\
                                Registered_Users_count)\
                                values(%s,%s,%s,%s,%s)", data_transform.top_user_district.values.tolist())
        
        cursor.executemany("insert into top_user_pincode(State, Year, Quarter, Pincode,\
                                Registered_Users_count)\
                                values(%s,%s,%s,%s,%s)", data_transform.top_user_pincode.values.tolist())

        mydb.commit()
        print("All values inserted")
        
class aggregate_transaction:

    def state_wise_total_transction():

        total_aggregate_transaction = data_transform.aggregate_transaction.groupby('State').sum(['Transaction_amount','Transaction_count']).reset_index()
        # if sub_category == ":money_with_wings: ***Transaction***":
        fig = px.choropleth_mapbox(total_aggregate_transaction, locations='State', 
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                color_continuous_scale="Viridis",
                hover_name='State', color = 'Transaction_amount',
                hover_data=['Transaction_amount', "Transaction_count"],
                mapbox_style='carto-positron', zoom = 3,
                center = {'lat' : 24, 'lon' : 78}, opacity=0.65)
        st.plotly_chart(fig)

data_load.tables_creation()
print("SQL Table Creation Done")


