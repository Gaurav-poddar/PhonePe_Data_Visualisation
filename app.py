import pandas as pd
import os
import json
import psycopg2
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

def state_list():
    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="Phonepe",
                        port="5432")
    cursor=mydb.cursor()
    query = '''Select
                    distinct state
                from aggregate_transaction'''
    cursor.execute(query)
    s = cursor.fetchall()
    state = [i[0] for i in s]
    return state

def year_list():
    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="Phonepe",
                        port="5432")
    cursor=mydb.cursor()
    query = '''Select
                    distinct year
                from aggregate_transaction
                order by year'''
    cursor.execute(query)
    s = cursor.fetchall()
    year = [i[0] for i in s]
    return year

def district_list():
    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="Phonepe",
                        port="5432")
    cursor=mydb.cursor()
    query = '''Select
                    distinct district_name
                from map_transaction'''
    cursor.execute(query)
    s = cursor.fetchall()
    district = [i[0] for i in s]
    return district



class state:

    def state_wise_total_transaction_amount():
            mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
            cursor=mydb.cursor()

            query = '''select 
                            state, 
                            sum(transaction_amount) as total_transaction_sum
                    from aggregate_transaction
                    group by state'''
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['state', 'total_transaction_amount'])
            df= df.astype({'state' : str, 'total_transaction_amount': int})

            fig = px.choropleth_mapbox(df, locations='state', 
                    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',
                    color_continuous_scale="Viridis",
                    hover_name='state', color = 'total_transaction_amount',
                    hover_data=['total_transaction_amount'],
                    mapbox_style='carto-positron', zoom = 2.75,
                    center = {'lat' : 24, 'lon' : 78}, opacity=0.65)
            st.plotly_chart(fig)

    def state_transaction_amount(selected_state = 'Select a state'):
    

        def year_wise_transaction_amount(selected_state):
            mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
            cursor=mydb.cursor()
            if selected_state == "Select a state":
                query = '''select year,
                                sum(transaction_amount) year_wise_transaction_amount
                        from aggregate_transaction
                        group by year
                        order by year'''
            else:
                query = f'''select year,
                                sum(transaction_amount) year_wise_transaction_amount
                        from aggregate_transaction
                    where state= '{selected_state}'
                        group by year
                        order by year;'''
                # print(query)
                
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns= ['year', 'transaction_amount'])
            df= df.astype({'year' : int, 'transaction_amount': int})

            fig = px.bar(df, x = 'year', y = 'transaction_amount', text = 'transaction_amount', color ='year',
                        title='Year Wise Transaction Amount')
            fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
        
            # Display the chart
            st.plotly_chart(fig)

        def quarter_wise_total_transaction_amount(selected_state ):
            mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
            cursor=mydb.cursor()
            if selected_state == 'Select a state':
                query = '''select quarter,
                                sum(transaction_amount) quarter_wise_transaction_amount
                        from aggregate_transaction
                        group by quarter
                        order by quarter '''
            else:
                query = f'''select quarter,
                                sum(transaction_amount) quarter_wise_transaction_amount
                            from aggregate_transaction
                            where state = '{selected_state}'
                            group by quarter
                            order by quarter'''
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns = ['quarter', 'quarter_wise_transaction_amount'])
            df = df.astype({'quarter' : int, 'quarter_wise_transaction_amount' : int})
            df['quarter'] = 'Q'+df['quarter'].astype('str')

            fig = px.bar(df, x = 'quarter', y = 'quarter_wise_transaction_amount', text = 'quarter_wise_transaction_amount',
                         color= 'quarter', title='Quarter Wise Transaction Amount' )
            fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
            fig.update_xaxes(title_text='Quarter')
            # Update y-axis to correctly format billions and trillions
            fig.update_yaxes(tickformat='~s')
        
            # Display the chart
            st.plotly_chart(fig)

        def year_quarter_wise_transaction_amount(selected_state ):
            mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
            cursor=mydb.cursor()

            if selected_state == "Select a state":
                query = '''select year,
                                quarter,
                                sum(transaction_amount) as  transaction_amount
                            from aggregate_transaction
                            group by year,
                                    quarter 
                            order by year,
                                    quarter'''
            else:
                query = f'''select year,
                                quarter,
                                sum(transaction_amount) as  transaction_amount
                            from aggregate_transaction
                            where state = '{selected_state}'
                            group by year,
                                    quarter 
                            order by year,
                                    quarter'''
                
            cursor.execute(query)
            mydb.commit()
            data  = cursor.fetchall()
            df = pd.DataFrame(data, columns = ['year', 'quarter', 'transaction_amount'])
            df = df.astype({'year': int, 'quarter': object, 'transaction_amount' : int})

            fig =  px.line(df, x = 'quarter', y= 'transaction_amount', color = 'year',
                        symbol='year', title=' Year Quarter Wise Transaction Amount')
            st.plotly_chart(fig)

        def year_type_wise_transaction_amount(selected_state):
            mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
            cursor=mydb.cursor()
            if selected_state == "Select a state":
                query = '''select year, 
                                transaction_type,
                                sum(transaction_amount) as  transaction_amount
                            from aggregate_transaction
                            group by year,
                                    transaction_type 
                            order by year,
                                    transaction_type'''
            else:
                query = f'''select year, 
                                transaction_type,
                                sum(transaction_amount) as  transaction_amount
                            from aggregate_transaction
                            where state = '{selected_state}'
                            group by year,
                                    transaction_type 
                            order by year,
                                    transaction_type'''
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns =['year', 'transaction_type', 'transaction_amount'])
            df = df.astype({'year': int, 'transaction_type': object, 'transaction_amount': int})

            fig = px.line(df, x = 'year', y = 'transaction_amount', color = 'transaction_type',
                        symbol = 'transaction_type', title = 'Year Type Wise Transaction Amount')
            
            st.plotly_chart(fig)

        def quarter_type_wise_transaction_amount(selected_state):
            mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
            cursor=mydb.cursor()
            if selected_state == "Select a state":
                query = '''select quarter, 
                                transaction_type,
                                sum(transaction_amount) as  transaction_amount
                            from aggregate_transaction
                            group by quarter,
                                    transaction_type 
                            order by quarter,
                                    transaction_type'''
            else:
                query = f'''select quarter, 
                                transaction_type,
                                sum(transaction_amount) as  transaction_amount
                            from aggregate_transaction
                            where state = '{selected_state}'
                            group by quarter,
                                    transaction_type 
                            order by quarter,
                                    transaction_type'''
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns =['quarter', 'transaction_type', 'transaction_amount'])
            df = df.astype({'quarter': object, 'transaction_type': object, 'transaction_amount': int})

            fig = px.line(df, x = 'quarter', y = 'transaction_amount', color = 'transaction_type',
                        symbol = 'transaction_type', title = 'Quarter Type Wise Transaction Amount')
            
            st.plotly_chart(fig)

        def  district_wise_transaction_amount_distribution(selected_state):
            mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="postgres",
                                database="Phonepe",
                                port="5432")
            cursor=mydb.cursor()
           
            query = f'''select 
                            district_name,
                            sum(transaction_amount) as transaction_amount
                        from map_transaction
                        where state = '{selected_state}'
                        group by
                                district_name
                        order by
                                district_name'''
                
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['district',  'transaction_amount'])
            df = df.astype({'district' : object, 'transaction_amount' : int})

            fig = px.pie(df, values='transaction_amount', names='district',
                         title=f'District Wise Transaction Amount Distribution of state = {selected_state}', 
                         hover_data=['transaction_amount'])
            fig.update_traces(textposition='inside', textinfo='percent+label+value', texttemplate='%{label}: %{value:.2s} (%{percent})')
            st.plotly_chart(fig)
        
        def amount_spend_per_transaction(selected_state):
            mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="postgres",
                                database="Phonepe",
                                port="5432")
            cursor=mydb.cursor()


            if selected_state == 'Select a state':
                query = '''select 
                                  year,
                                  (sum(transaction_amount) / sum(transaction_count)) as amount_spend_per_transaction
                            from aggregate_transaction
                            group by  year
                            order by  year;'''
            else:
                query = f'''select 
                                  year,
                                  (sum(transaction_amount) / sum(transaction_count)) as amount_spend_per_transaction
                            from aggregate_transaction
                            where state = '{selected_state}'
                            group by  year
                            order by  year;'''
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns= ['year', 'amount_spend_per_transaction'])
            df= df.astype({'year' : object, 'amount_spend_per_transaction': int})

            fig = px.bar(df, x = 'year', y = 'amount_spend_per_transaction',
                         text = 'amount_spend_per_transaction', color = 'year')
            st.plotly_chart(fig)

        year_wise_transaction_amount(selected_state)
        quarter_wise_total_transaction_amount(selected_state)
        year_quarter_wise_transaction_amount(selected_state)
        year_type_wise_transaction_amount(selected_state)
        quarter_type_wise_transaction_amount(selected_state)
        if selected_state != 'Select a state':
            district_wise_transaction_amount_distribution(selected_state)
            st.subheader(f'Year Wise Amount spend per Transaction for state: {selected_state}')
        else:
          st.subheader('Year Wise Amount spend per Transaction for states')
        amount_spend_per_transaction(selected_state)
            



    def state_wise_total_transaction_count():

        mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="Phonepe",
                        port="5432")
        cursor=mydb.cursor()

        query = '''select 
                        state, 
                        sum(transaction_count) as total_transaction_count
                    from aggregate_transaction
                    group by state'''
        cursor.execute(query)
        mydb.commit()
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['state', 'total_transaction_count'])
        df= df.astype({'state' : str, 'total_transaction_count': int})

        fig = px.choropleth_mapbox(df, locations='state', 
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                color_continuous_scale="Viridis",
                hover_name='state', color = 'total_transaction_count',
                hover_data=['total_transaction_count'],
                mapbox_style='carto-positron', zoom = 2.75,
                center = {'lat' : 24, 'lon' : 78}, opacity=0.65)
        st.plotly_chart(fig) 

    def state_transaction_count(selected_state = "Select a state"):

        def year_wise_transaction_count(selected_state):
            mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
            cursor=mydb.cursor()
            if selected_state == "Select a state":
                query = '''select year,
                                sum(transaction_count) year_wise_transaction_count
                        from aggregate_transaction
                        group by year
                        order by year'''
            else:
                query = f'''select year,
                                sum(transaction_count) year_wise_transaction_count
                        from aggregate_transaction
                    where state= '{selected_state}'
                        group by year
                        order by year;'''
                print(query)
                
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns= ['year', 'transaction_count'])
            df= df.astype({'year' : int, 'transaction_count': int})

            fig = px.bar(df, x = 'year', y = 'transaction_count', text = 'transaction_count',color = 'year',
                        title='Year Wise Transaction Count')
            fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
        
            # Display the chart
            st.plotly_chart(fig)   

        def quarter_wise_total_transaction_count(selected_state):
            mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
            cursor=mydb.cursor()
            if selected_state == 'Select a state':
                query = '''select quarter,
                                sum(transaction_count) quarter_wise_transaction_count
                        from aggregate_transaction
                        group by quarter
                        order by quarter '''
            else:
                query = f'''select quarter,
                                sum(transaction_count) quarter_wise_transaction_count
                            from aggregate_transaction
                            where state = '{selected_state}'
                            group by quarter
                            order by quarter'''
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns = ['quarter', 'quarter_wise_transaction_count'])
            df = df.astype({'quarter' : int, 'quarter_wise_transaction_count' : int})
            df['quarter'] = 'Q'+df['quarter'].astype('str')

            fig = px.bar(df, x = 'quarter', y = 'quarter_wise_transaction_count', text = 'quarter_wise_transaction_count',
                        color = 'quarter', title='Quarter Wise Transaction Count' )
            fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
            fig.update_xaxes(title_text='Quarter')
            fig.update_yaxes(tickformat='~s')
        
            # Display the chart
            st.plotly_chart(fig)                   
        
        def year_quarter_wise_transaction_count(selected_state):
            mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
            cursor=mydb.cursor()

            if selected_state == "Select a state":
                query = '''select year,
                                quarter,
                                sum(transaction_amount) as  transaction_amount
                            from aggregate_transaction
                            group by year,
                                    quarter 
                            order by year,
                                    quarter'''
            else:
                query = f'''select year,
                                quarter,
                                sum(transaction_count) as  transaction_count
                            from aggregate_transaction
                            where state = '{selected_state}'
                            group by year,
                                    quarter 
                            order by year,
                                    quarter'''
                
            cursor.execute(query)
            mydb.commit()
            data  = cursor.fetchall()
            df = pd.DataFrame(data, columns = ['year', 'quarter', 'transaction_count'])
            df = df.astype({'year': int, 'quarter': object, 'transaction_count' : int})

            fig =  px.line(df, x = 'quarter', y= 'transaction_count', color = 'year',
                        symbol='year', title=' Year Quarter Wise Transaction Count')
            
            # plotting the graph
            st.plotly_chart(fig)
        
        def year_type_wise_transaction_count(selected_state):
            mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
            cursor=mydb.cursor()
            if selected_state == "Select a state":
                query = '''select year, 
                                transaction_type,
                                sum(transaction_count) as  transaction_count
                            from aggregate_transaction
                            group by year,
                                    transaction_type 
                            order by year,
                                    transaction_type'''
            else:
                query = f'''select year, 
                                transaction_type,
                                sum(transaction_count) as  transaction_count
                            from aggregate_transaction
                            where state = '{selected_state}'
                            group by year,
                                    transaction_type 
                            order by year,
                                    transaction_type'''
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns =['year', 'transaction_type', 'transaction_count'])
            df = df.astype({'year': int, 'transaction_type': object, 'transaction_count': int})

            fig = px.line(df, x = 'year', y = 'transaction_count', color = 'transaction_type',
                        symbol = 'transaction_type', title = 'Year Type Wise Transaction Count')
            
            st.plotly_chart(fig)
        
        def quarter_type_wise_transaction_count(selected_state):
            mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
            cursor=mydb.cursor()
            if selected_state == "Select a state":
                query = '''select quarter, 
                                transaction_type,
                                sum(transaction_count) as  transaction_count
                            from aggregate_transaction
                            group by quarter,
                                    transaction_type 
                            order by quarter,
                                    transaction_type'''
            else:
                query = f'''select quarter, 
                                transaction_type,
                                sum(transaction_count) as  transaction_count
                            from aggregate_transaction
                            where state = '{selected_state}'
                            group by quarter,
                                    transaction_type 
                            order by quarter,
                                    transaction_type'''
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns =['quarter', 'transaction_type', 'transaction_count'])
            df = df.astype({'quarter': object, 'transaction_type': object, 'transaction_count': int})

            fig = px.line(df, x = 'quarter', y = 'transaction_count', color = 'transaction_type',
                        symbol = 'transaction_type', title = 'Quarter Type Wise Transaction Count')
            
            st.plotly_chart(fig)

        def  district_wise_transaction_count_distribution(selected_state):
            mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="postgres",
                                database="Phonepe",
                                port="5432")
            cursor=mydb.cursor()
           
            query = f'''select 
                            district_name,
                            sum(transaction_count) as transaction_count
                        from map_transaction
                        where state = '{selected_state}'
                        group by
                                district_name
                        order by
                                district_name'''
                
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['district',  'transaction_count'])
            df = df.astype({'district' : object, 'transaction_count' : int})

            fig = px.pie(df, values='transaction_count', names='district',
                         title=f'District Wise Transaction Count Distribution of state = {selected_state}', 
                         hover_data=['transaction_count'])
            fig.update_traces(textposition='inside', textinfo='percent+label+value', texttemplate='%{label}: %{value:.2s} (%{percent})')
            st.plotly_chart(fig)

        
        year_wise_transaction_count(selected_state)
        quarter_wise_total_transaction_count(selected_state)
        year_quarter_wise_transaction_count(selected_state)
        year_type_wise_transaction_count(selected_state)
        quarter_type_wise_transaction_count(selected_state)
        if selected_state != 'Select a state':
            district_wise_transaction_count_distribution(selected_state)



    def state_wise_total_users_count():
        mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
        cursor=mydb.cursor()

        query = '''select state,
                   sum(users_count) as users_count
                from aggregate_user
                group by state'''
        cursor.execute(query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns = ['state', 'Users_count'])
        df = df.astype({'state' : str, 'Users_count': int})

        fig = px.choropleth_mapbox(df, locations='state', 
                    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',
                    color_continuous_scale="Viridis",
                    hover_name='state', color = 'Users_count',
                    hover_data=['Users_count'],
                    mapbox_style='carto-positron', zoom = 2.75,
                    center = {'lat' : 24, 'lon' : 78}, opacity=0.65)
        st.plotly_chart(fig)

    def avg_app_opens():
            mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="postgres",
                                database="Phonepe",
                                port="5432")
            cursor=mydb.cursor()
            
            query = '''with T1 as (
                                SELECT state,
                                        SUM(registered_users) AS total_registered_users
                                FROM map_user
                                GROUP BY state),
                            T as (
                                SELECT state,
                                        SUM(app_opens) AS total_app_opens
                                FROM map_user
                                GROUP BY state)
                        select  T1.state,
                                T1.total_registered_users ,
                                T.total_app_opens,
                                (T.total_app_opens / T1.total_registered_users) as per_user_app_open_ratio
                        from T1 
                        inner join T on T1.state = T.state
                        order by per_user_app_open_ratio desc;'''
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns= ['state', 'total_registered_users', 'total_app_opens', 'ratio_of_app_opened_by_total_user'])
            df = df.astype({'state' : object, 'total_registered_users' : int,
                             'total_app_opens' : int, 'ratio_of_app_opened_by_total_user' : float})
            
            
            
            fig = px.bar(df, y= 'ratio_of_app_opened_by_total_user', x='state', 
                         text_auto='.3s',  color = 'state'
                        )
            fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
            fig.update_layout(width = 1000, height = 600)
            st.plotly_chart(fig)

    def state_users_count(selected_state = "Select a state"):

        def year_wise_users_count(selected_state):
            mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="postgres",
                                database="Phonepe",
                                port="5432")
            cursor=mydb.cursor()


            if selected_state == "Select a state":
                query= '''select year,
                                sum(users_count) as users_count
                        from aggregate_user
                        group by year
                        order by year'''
            else:
                query= f'''select year,
                                sum(users_count) as users_count
                        from aggregate_user
                        where state = '{selected_state}'
                        group by year
                        order by year'''
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns = ['year', 'users_count'])
            df = df.astype({'year' : object, 'users_count' : int})

            fig = px.line(df, y = 'year', x = 'users_count', markers='year',
                             title = 'Year Wise Users Count')
                
            st.plotly_chart(fig)

        def quarter_wise_users_count(selected_state):
            mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="postgres",
                                database="Phonepe",
                                port="5432")
            cursor=mydb.cursor()

            if selected_state == 'Select a state':
                query = '''select quarter,
                                sum(users_count) users_count
                            from aggregate_user
                            group by quarter
                            order by quarter'''
                
            else:
                query = f'''select quarter,
                                sum(users_count) users_count
                            from aggregate_user
                            where state = '{selected_state}'
                            group by quarter
                            order by quarter'''
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns = ['quarter', 'users_count'])
            df = df.astype({'quarter' : int, 'users_count' : int})
            df['quarter'] = 'Q'+df['quarter'].astype('str')

            fig = px.bar(df, x = 'quarter', y = 'users_count',text = 'users_count',color = 'quarter',
                             title = 'Quarter Wise Users Count')
            fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
                
            st.plotly_chart(fig)

        def brand_wise_users_count(selected_state):
            mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="postgres",
                                database="Phonepe",
                                port="5432")
            cursor=mydb.cursor()

            if selected_state == 'Select a state':
                query = '''select brand,
                                sum(users_count) as users_count
                            from aggregate_user
                            group by brand
                            order by users_count desc
                            limit 10'''
            else:
                query = f'''select brand,
                                sum(users_count) as users_count
                            from aggregate_user
                            where brand in ( 'Xiaomi' ,
                                            'Samsung',
                                            'Vivo',
                                            'Oppo',
                                            'Others',
                                            'Realme',
                                            'Apple',
                                            'Motorola',
                                            'OnePlus',
                                            'Huawei') 
                            and    state = '{selected_state}'
                            group by  brand
                            order by  brand desc'''
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['brand',  'users_count'])
            df = df.astype({'brand' : object, 'users_count' : int})

            fig = px.pie(df, values='users_count', names='brand',
             title='Brand Wise User Count',
             hover_data=['users_count'])#, labels={'lifeExp':'life expectancy'})
            fig.update_traces(textposition='inside', textinfo='percent+label+value', texttemplate='%{label}: %{value:.2s} (%{percent})')
            st.plotly_chart(fig)
        
        def year_wise_brand_user_count(selected_state):
            mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="postgres",
                                database="Phonepe",
                                port="5432")
            cursor=mydb.cursor()

            if selected_state == 'Select a state':
                query = '''select brand,
                                year,
                                sum(users_count) as users_count
                            from aggregate_user
                            where brand in ( 'Xiaomi' ,
                                            'Samsung',
                                            'Vivo',
                                            'Oppo',
                                            'Others',
                                            'Realme',
                                            'Apple',
                                            'Motorola',
                                            'OnePlus',
                                            'Huawei')
                            group by year,
                                    brand
                            order by 
                                    brand desc, year'''
            else:
                query = f'''select brand,
                                year,
                                sum(users_count) as users_count
                            from aggregate_user
                            where brand in ( 'Xiaomi' ,
                                            'Samsung',
                                            'Vivo',
                                            'Oppo',
                                            'Others',
                                            'Realme',
                                            'Apple',
                                            'Motorola',
                                            'OnePlus',
                                            'Huawei') and state = '{selected_state}'
                            group by year,
                                    brand
                            order by 
                                    brand desc, year'''
                
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns= ['brand', 'year', 'users_count'])
            df= df.astype({'brand' : str, 'year' : object, 'users_count': int})

            fig = px.bar(df, x = 'brand', y='users_count', color = 'year', text='users_count',
                title = 'Year wise Brand User Count',  text_auto=True)
            fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
            st.plotly_chart(fig)



        

                
        year_wise_users_count(selected_state)
        quarter_wise_users_count(selected_state)
        brand_wise_users_count(selected_state)
        year_wise_brand_user_count(selected_state)
        


class district: 

    def district_transaction_amount(selected_district = 'Select a district'):

        def year_wise_district_transaction_amount(selected_district):
            mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="Phonepe",
                        port="5432")
            cursor=mydb.cursor()

            if selected_district == 'Select a district':
                query = '''select year,
                            sum(transaction_amount) as transaction_amount
                    from aggregate_transaction
                    group by year
                    order by year '''
            else:
                query = f'''select 
                            year,
                            sum(transaction_amount) as transaction_amount
                        from map_transaction
                        where district_name = '{selected_district}'
                        group by district_name,
                                year
                        order by district_name,
                                year'''

            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns= ['year', 'transaction_amount'])
            df= df.astype({'year' : int, 'transaction_amount': int})

            fig = px.bar(df, x = 'year', y = 'transaction_amount', text_auto='.2s', color = 'year')
            
            fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)

            # Display the chart
            st.plotly_chart(fig)
        
        def quarter_wise_district_transaction_amount(selected_district):
            mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="Phonepe",
                        port="5432")
            cursor=mydb.cursor()

            if selected_district == 'Select a district':
                query = '''select quarter,
                                  sum(transaction_amount) as transaction_amount
                            from aggregate_transaction
                            group by quarter
                            order by quarter'''
            else:
                query = f'''select quarter,
                                  sum(transaction_amount) as transaction_amount
                            from map_transaction
                            where district_name = '{selected_district}'
                            group by quarter
                            order by quarter'''
            
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns= ['quarter', 'transaction_amount'])
            df= df.astype({'quarter' : int, 'transaction_amount': int})
            df['quarter'] = 'Q'+df['quarter'].astype('str')

            fig = px.bar(df, x = 'quarter', y = 'transaction_amount', text_auto='.2s', color = 'quarter')
            
            fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)

            # Display the chart
            st.plotly_chart(fig)
        
        def year_wise_amount_spend_per_transaction(selected_district):
            mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="Phonepe",
                        port="5432")
            cursor=mydb.cursor()

            if selected_district == 'Select a district':
                query = '''select 
                                  year,
                                  (sum(transaction_amount) / sum(transaction_count)) as amount_spend_per_transaction
                            from aggregate_transaction
                            group by  year
                            order by  year;'''
            else:
                query = f'''select 
                                  year,
                                  (sum(transaction_amount) / sum(transaction_count)) as amount_spend_per_transaction
                            from map_transaction
                            where district_name = '{selected_district}'
                            group by  year
                            order by  year;'''

            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns= ['year', 'amount_spend_per_transaction'])
            df= df.astype({'year' : object, 'amount_spend_per_transaction': int})

            fig = px.bar(df, x = 'year', y = 'amount_spend_per_transaction',
                         text = 'amount_spend_per_transaction', color = 'year')
            st.plotly_chart(fig)  
            
        if selected_district == 'Select a district':
            st.header('Year Wise Distribution of Total Transaction Amount of each Districts')
            year_wise_district_transaction_amount(selected_district)
            st.header('Quarter Wise Distribution of Total Transaction Amount of each Districts')
            quarter_wise_district_transaction_amount(selected_district)
            st.header('Year Wise Distribution of Amount Spend Per Transaction for Districts')
            year_wise_amount_spend_per_transaction(selected_district)
        else:
            st.subheader(f'Year Wise Distribution of Total Transaction Amount of each District: {selected_district}')
            year_wise_district_transaction_amount(selected_district)
            st.subheader(f'Quarter Wise Distribution of Total Transaction Amount of each District: {selected_district}')
            quarter_wise_district_transaction_amount(selected_district)
            st.subheader(f'Year Wise Distribution of Amount Spend Per Transaction for District: {selected_district}')
            year_wise_amount_spend_per_transaction(selected_district)


    def district_transaction_count(selected_district = 'Select a district'):

        def year_wise_district_transaction_count(selected_district):
            mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="Phonepe",
                        port="5432")
            cursor=mydb.cursor()

            if selected_district == 'Select a district':
                query = '''select year,
                            sum(transaction_count) as transaction_count
                    from aggregate_transaction
                    group by year
                    order by year '''
            else:
                query = f'''select 
                            year,
                            sum(transaction_count) as transaction_count
                        from map_transaction
                        where district_name = '{selected_district}'
                        group by district_name,
                                year
                        order by district_name,
                                year'''

            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns= ['year', 'transaction_count'])
            df= df.astype({'year' : int, 'transaction_count': int})

            fig = px.bar(df, x = 'year', y = 'transaction_count', text_auto='.2s', color = 'year',
                        title='Year Wise Distribution of Transaction Count of  Districts ')
            
            fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)

            # Display the chart
            st.plotly_chart(fig)
        
        def quarter_wise_district_transaction_count(selected_district):
            mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="Phonepe",
                        port="5432")
            cursor=mydb.cursor()

            if selected_district == 'Select a district':
                query = '''select quarter,
                                  sum(transaction_count) as transaction_count
                            from aggregate_transaction
                            group by quarter
                            order by quarter'''
            else:
                query = f'''select quarter,
                                  sum(transaction_count) as transaction_count
                            from map_transaction
                            where district_name = '{selected_district}'
                            group by quarter
                            order by quarter'''
            
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns= ['quarter', 'transaction_count'])
            df= df.astype({'quarter' : int, 'transaction_count': int})
            df['quarter'] = 'Q'+df['quarter'].astype('str')

            fig = px.bar(df, x = 'quarter', y = 'transaction_count', text_auto='.2s', color = 'quarter',
                        title=f'Quarter Wise Distribution of Transaction Count of  Districts ')
            
            fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)

            # Display the chart
            st.plotly_chart(fig)

        year_wise_district_transaction_count(selected_district)
        quarter_wise_district_transaction_count(selected_district)         

    def district_users(selected_district = 'Select a district'):

        def district_registered_users(selected_district):
            mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="postgres",
                                database="Phonepe",
                                port="5432")
            cursor=mydb.cursor()
            
            if selected_district == 'Select a district':
                query = '''select year,
                                sum(registered_users) as total_users
                            from map_user
                            group by year
                            order by year'''
            else:
                query = f'''select year,
                                sum(registered_users) as total_users
                            from  map_user
                            where district_name = '{selected_district}'
                            group by
                                    year
                            order by year'''
                
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns= ['year', 'total_users'])
            df= df.astype({'year' : int, 'total_users': int})

            fig = px.bar(df, x = 'year', y = 'total_users', color = 'year', text_auto='.2s')
            
            fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)

            # Display the chart
            st.plotly_chart(fig)

        def district_app_opens(selected_district):

            mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="postgres",
                                database="Phonepe",
                                port="5432")
            cursor=mydb.cursor()
            
            if selected_district == 'Select a district':
                query = '''select quarter,
                                sum(app_opens) as total_app_opens
                        from  map_user
                        group by quarter
                        order by quarter'''
            else:
                query = f'''select quarter,
                                sum(app_opens) as total_app_opens
                        from  map_user
                        where district_name = '{selected_district}'
                        group by quarter
                        order by quarter'''
                
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns= ['quarter', 'total_app_opens'])
            df= df.astype({'quarter' : object, 'total_app_opens': int})
            df['Qs'] = 'Q'+df['quarter'].astype('str')

            fig = px.pie(df, values='total_app_opens', names='Qs',
             hover_data=['total_app_opens'])
            fig.update_traces(textposition='inside', textinfo='percent+label+value', texttemplate='%{label}: %{value:.2s} (%{percent})')
            st.plotly_chart(fig)

        def district_year_quarter_app_opens(selected_district):

            mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="postgres",
                                database="Phonepe",
                                port="5432")
            cursor=mydb.cursor()
            
            if selected_district == 'Select a district':
                query = '''select year,
                                  quarter,
                                  sum(registered_users) as registered_users
                            from map_user
                            group by year, 
                                     quarter
                            order by year,
                                     quarter'''
                
            else:
                query = f'''select year,
                                   quarter,
                                   sum(registered_users) as registered_users
                            from map_user
                            where district_name = '{selected_district}'
                            group by year, 
                                     quarter
                            order by year, 
                                     quarter'''
            
            cursor.execute(query)
            mydb.commit()
            data  = cursor.fetchall()
            df = pd.DataFrame(data, columns = ['year', 'quarter', 'registered_users'])
            df = df.astype({'year': int, 'quarter': object, 'registered_users' : int})
            df['quarter'] = 'Q'+df['quarter'].astype('str')

            fig =  px.line(df, x = 'quarter', y= 'registered_users', color = 'year',
                        symbol='year')
            
            # plotting the graph
            st.plotly_chart(fig)
                
        def year_wise_average_app_opened(selected_district):
            
            mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="postgres",
                                database="Phonepe",
                                port="5432")
            cursor=mydb.cursor()
        
            query = f'''with T1 as (
                                SELECT district_name,
                                    year,
                                        SUM(registered_users) AS total_registered_users
                                FROM map_user
                                where year in (2019, 2020, 2021, 2022, 2023)
                                GROUP BY district_name,
                                    year),
                            T as (
                                SELECT district_name, 
                                    year,
                                    SUM(app_opens) AS total_app_opens
                                FROM map_user
                                where year in (2019, 2020, 2021, 2022, 2023)
                                GROUP BY district_name, 
                                    year)
                        select T1.district_name,
                            T1.year,
                            T1.total_registered_users ,
                            T.total_app_opens,
                            (T.total_app_opens /  T1.total_registered_users) as per_user_app_open
                        from T1 
                        inner join T on T1.district_name = T.district_name AND
                        T1.year = T.year
                        where T1.district_name = '{selected_district}'
                        order by T1.year;'''
            
            cursor.execute(query)
            mydb.commit()
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns= ['district_name', 'year', 
                                              'total_registered_users', 'total_app_opens', 'per_user_app_open'])
            df = df.astype({'district_name': str, 'year' : object, 'total_registered_users' : int,
                             'total_app_opens' : int, 'per_user_app_open' : float})
            
            fig = px.bar(df, y= 'per_user_app_open', x='year', color = 'year',  text_auto='.3s',
                        title = f'Year Wise Average number of time App opened by user of district: {selected_district}')
            fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
            fig.add_annotation( text="Note: App opens data starts from year 2019",
            xref="paper", yref="paper",
            x=0, y=1,  # Coordinates in the paper space (1, 1) is the top-right corner
            showarrow=False, font=dict(size=12, color="black"),
            align="left", bordercolor="black",
            borderwidth=1,  borderpad=4,
            bgcolor="white", opacity=0.8
            )
            
            st.plotly_chart(fig)


        if selected_district == 'Select a district':
            st.header('Year Wise Distribution of Total Users of each Districts')
            district_registered_users(selected_district)
            st.header('Quarter Wise App Opened')
            district_app_opens(selected_district)
            st.header('Year Quarter Wise Registered Users Count')
            district_year_quarter_app_opens(selected_district)
            
        else:
            st.header(f'Year Wise Distribution of Total Users of District: {selected_district}')
            district_registered_users(selected_district)
            st.header(f'Quarter Wise App Opened by district: {selected_district}')
            district_app_opens(selected_district)
            st.header(f'Year Quarter Wise Registered Users Count for district: {selected_district}')
            district_year_quarter_app_opens(selected_district)
            year_wise_average_app_opened(selected_district)

  

class top_10:
    
    def top_10_transaction_amount_state(year, quarter):
        mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
        cursor=mydb.cursor()

        query = f'''select state,
                        sum(transaction_amount) as transaction_amount
                    from top_transaction_district
                    where year = '{year}'
                    and quarter = {quarter}
                    group by state 
                    order by transaction_amount desc
                    limit 10;'''
        cursor.execute(query)
        mydb.commit()
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns = ['state', 'transaction_amount'])
        df = df.astype({'state' : str, 'transaction_amount' : float})

        fig = px.bar(df, x='state', y = 'transaction_amount', text_auto='.2s', color = 'state',
                     title = f'Top 10 States transaction amounts for year  {year} and quarter  {quarter}')
        
        fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)

        st.plotly_chart(fig)

    def top_10_transaction_count_state(year, quarter):
        mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
        cursor=mydb.cursor()

        query = f'''select state,
                        sum(transaction_count) as transaction_count
                    from top_transaction_district
                    where year = '{year}'
                    and quarter = {quarter}
                    group by state 
                    order by transaction_count desc
                    limit 10;'''
        cursor.execute(query)
        mydb.commit()
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns = ['state', 'transaction_count'])
        df = df.astype({'state' : str, 'transaction_count' : int})

        fig = px.bar(df, x='state', y = 'transaction_count', text = 'transaction_count',color = 'state',
                     title = f'Top 10 States transaction counts for year  {year} and quarter  {quarter}')
        fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')

        st.plotly_chart(fig)
 
    def top_10_transaction_amount_district(year, quarter):
        mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
        cursor=mydb.cursor()

        query = f'''select district_name,
                        sum(transaction_amount) as transaction_amount, 
                        state
                    from top_transaction_district
                    where year = '{year}'
                    and quarter = {quarter}
                    group by district_name,
                             state
                    order by transaction_amount desc
                    limit 10;'''
        cursor.execute(query)
        mydb.commit()
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns = ['district', 'transaction_amount', 'state'])
        df = df.astype({'district' : str, 'transaction_amount' : int, 'state' : str})
        df['district_state'] = df.apply(lambda row: f"{row['district']} ({row['state']})", axis=1)

        fig = px.bar(df, x='district_state', y = 'transaction_amount', text_auto='.2s', color = 'state',
                     title = f'Top 10 District transaction amounts for year  {year} and quarter  {quarter}')

        fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)

        fig.update_xaxes(title_text='District (State)')
        st.plotly_chart(fig)

    def top_10_transaction_count_district(year, quarter):
        mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
        cursor=mydb.cursor()

        query = f'''select district_name,
                        sum(transaction_count) as transaction_count, 
                        state
                    from top_transaction_district
                    where year = '{year}'
                    and quarter = {quarter}
                    group by district_name,
                             state
                    order by transaction_count desc
                    limit 10;'''
        cursor.execute(query)
        mydb.commit()
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns = ['district', 'transaction_count', 'state'])
        df = df.astype({'district' : str, 'transaction_count' : int, 'state' : str})
        df['district_state'] = df.apply(lambda row: f"{row['district']} ({row['state']})", axis=1)

        fig = px.bar(df, x='district_state', y = 'transaction_count', text = 'transaction_count',color = 'district_state',
                     title = f'Top 10 District transaction counts for year  {year} and quarter  {quarter}')
        fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
        fig.update_xaxes(title_text='District (State)')
        st.plotly_chart(fig)

    def top_10_transaction_amount_pincodes(year, quarter):
        mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
        cursor=mydb.cursor()

        query = f'''select pincode,
                        sum(transaction_amount) as transaction_amount, 
                        state
                    from top_transaction_pincode
                    where year = '{year}'
                    and quarter = {quarter}
                    group by pincode,
                             state
                    order by transaction_amount desc
                    limit 10;'''
        cursor.execute(query)
        mydb.commit()
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns = ['pincode', 'transaction_amount', 'state'])
        df = df.astype({'pincode' : str, 'transaction_amount' : int, 'state' : str})
        df['pincode_state'] = df.apply(lambda row: f"{row['pincode']} ({row['state']})", axis=1)

        fig = px.bar(df, x='pincode_state', y = 'transaction_amount', text = 'transaction_amount',color = 'pincode_state',
                     title = f'Top 10 Pincodes transaction amounts for year  {year} and quarter  {quarter}')
        fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
        fig.update_xaxes(title_text='Pincode (State)')
        st.plotly_chart(fig)

    def top_10_transaction_count_pincodes(year, quarter):
        mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
        cursor=mydb.cursor()

        query = f'''select pincode,
                        sum(transaction_count) as transaction_count, 
                        state
                    from top_transaction_pincode
                    where year = '{year}'
                    and quarter = {quarter}
                    group by pincode,
                             state
                    order by transaction_count desc
                    limit 10;'''
        cursor.execute(query)
        mydb.commit()
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns = ['pincode', 'transaction_count', 'state'])
        df = df.astype({'pincode' : str, 'transaction_count' : int, 'state' : str})
        df['pincode_state'] = df.apply(lambda row: f"{row['pincode']} ({row['state']})", axis=1)

        fig = px.bar(df, x='pincode_state', y = 'transaction_count', text = 'transaction_count', color = 'pincode_state',
                     title = f'Top 10 Pincodes transaction counts for year  {year} and quarter  {quarter}')
        fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
        fig.update_xaxes(title_text='Pincode (State)')
        st.plotly_chart(fig)
    
    def top_10_user_state(year, quarter):
        mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
        cursor=mydb.cursor()

        query = f'''select state,
                        sum(registered_users_count) as user_count
                    from top_user_district
                    where year = '{year}'
                    and quarter = {quarter}
                    group by state 
                    order by user_count desc
                    limit 10;'''
        cursor.execute(query)
        mydb.commit()
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns = ['state', 'users_count'])
        df = df.astype({'state' : str, 'users_count' : int})

        fig = px.bar(df, x='state', y = 'users_count', text = 'users_count',  color = 'state',
                     title = f'Top 10 States user counts for year  {year} and quarter  {quarter}')
        fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')

        st.plotly_chart(fig)

    def top_10_user_district(year, quarter):
        mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
        cursor=mydb.cursor()

        query = f'''select district_name,
                        sum(registered_users_count) as user_count,
                        state
                    from top_user_district
                    where year = '{year}'
                    and quarter = {quarter}
                    group by district_name,
                             state 
                    order by user_count desc
                    limit 10;'''
        cursor.execute(query)
        mydb.commit()
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns = ['district_name', 'users_count', 'state'])
        df = df.astype({'district_name' : str, 'users_count' : int, 'state' : str})
        df['district_state'] = df.apply(lambda row: f"{row['district_name']} ({row['state']})", axis=1)

        fig = px.bar(df, x='district_state', y = 'users_count', text = 'users_count', color = 'district_state',
                     title = f'Top 10 Districts user counts for year  {year} and quarter  {quarter}')
        fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
        fig.update_xaxes(title_text= 'Distrcit (State)')


        st.plotly_chart(fig)

    def top_10_user_pincode(year, quarter):
        mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="postgres",
                            database="Phonepe",
                            port="5432")
        cursor=mydb.cursor()

        query = f'''select pincode,
                        sum(registered_users_count) as user_count,
                        state
                    from top_user_pincode
                    where year = '{year}'
                    and quarter = {quarter}
                    group by pincode,
                             state 
                    order by user_count desc
                    limit 10;'''
        cursor.execute(query)
        mydb.commit()
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns = ['pincode', 'users_count', 'state'])
        df = df.astype({'pincode' : str, 'users_count' : int, 'state' : str})
        df['pincode_state'] = df.apply(lambda row: f"{row['pincode']} ({row['state']})", axis=1)


        fig = px.bar(df, x='pincode_state', y = 'users_count', text = 'users_count', color = 'pincode_state',
                     title = f'Top 10 Pincodes user counts for year  {year} and quarter  {quarter}')
        fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
        fig.update_xaxes(title_text= 'Pincode (State)')

        st.plotly_chart(fig)


st.title("**PHONEPE DATA VISUALISATION**")
# Main selectbox from where we can choose particular category visualisation we want to  watch
choice = st.selectbox("", ("Select one", "State", "District", "Top 10"))

# Visualisation related to State Category
if choice == "State":
    transactions, users = st.tabs(['Transactions', 'Users'])

    with transactions:
        option = st.radio('Choose a option:', ['Transaction Amount', 'Transaction Count'])

        if option == 'Transaction Amount':
            st.header("State Wise Total Transaction Amount")
            state.state_wise_total_transaction_amount()
            selected_state = st.selectbox("State",["Select a state"]+state_list(), key= 'State Transaction Amount')
            state.state_transaction_amount(selected_state)

        elif option == 'Transaction Count':
            st.header("State Wise Total Transaction Count")
            state.state_wise_total_transaction_count()
            selected_state = st.selectbox("State",["Select a state"]+state_list(), key = 'State Transaction Count')
            state.state_transaction_count(selected_state)

    with users:
        option = st.radio('Choose an otpion:', ['Users Count'])

        if option == 'Users Count':
            st.header("State Wise Total Users Count")
            state.state_wise_total_users_count()
            st.header('Average Number of Times App Opened by User')
            state.avg_app_opens()
            selected_state = st.selectbox("State", ['Select a state']+state_list(), key='State Users Count')
            state.state_users_count(selected_state)

# Visualisation related to District Category                 
elif choice == "District":
    transactions, users = st.tabs(['Transactions' , 'Users'])

    with transactions:
        option = st.radio('Choose an otpion:', ['Transaction Amount', 'Transaction Count' ])
        if option == 'Transaction Amount':
            selected_district = st.selectbox("District",['Select a district']+district_list(), key = 'District Transaction Amount')
            district.district_transaction_amount(selected_district)

        elif option == 'Transaction Count':
            selected_district = st.selectbox("District ",['Select a district']+district_list(), key = 'District Transaction Count')
            district.district_transaction_count(selected_district)

    with users:
        option = st.radio('', ['Registered Users'])
        if option == 'Registered Users':
            selected_district = st.selectbox("District",['Select a district']+district_list(), key = 'District Users Count')
          
            district.district_users(selected_district)

# Visualisation related to top 10 categories
elif choice == "Top 10":
    transactions, users = st.tabs([ 'Transactions' , 'Users'])

    with transactions:
        year = st.selectbox("Year", year_list(), key='Top 10  Year transaction')
        quarter = st.selectbox("Quarter", [1,  2,  3, 4], key='Top 10  quarter transaction')
        state, district, pincodes = st.tabs(['States', 'Districts' , 'Pincodes'])
        with state:
            radio = st.radio("", ('Transaction Amount', 'Transaction Count'), key = 'state')
            if radio == "Transaction Amount":
                top_10.top_10_transaction_amount_state(year, quarter)
            if radio == "Transaction Count":
                top_10.top_10_transaction_count_state(year, quarter)

        with district:
            radio = st.radio("", ('Transaction Amount', 'Transaction Count'), key = 'district')
            if radio == "Transaction Amount":
                top_10.top_10_transaction_amount_district(year,quarter)
            if radio == "Transaction Count":
                top_10.top_10_transaction_count_district(year, quarter)

        with pincodes:
            radio = st.radio("", ('Transaction Amount', 'Transaction Count'), key = 'pincode')
            if radio == "Transaction Amount":
                top_10.top_10_transaction_amount_pincodes(year,quarter)
            if radio == "Transaction Count":
                top_10.top_10_transaction_count_pincodes(year, quarter)

    with users:
        year = st.selectbox("Year", year_list(), key='Top 10  Year user')
        quarter = st.selectbox("Quarter", [1,  2,  3, 4], key='Top 10  quarter user')
        state, district, pincodes = st.tabs(['States', 'Districts' , 'Pincodes'])
        with state:
            top_10.top_10_user_state(year, quarter)
        with district:
            top_10.top_10_user_district(year, quarter)
        with pincodes:
            top_10.top_10_user_pincode(year, quarter)

        
        


