#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import statsmodels.api as sm
import plotly.figure_factory as ff
import mysql.connector
from mysql_work import mysql_work
import streamlit as st
import sqlalchemy as db
import locale

st.set_page_config(layout="wide")
st.title('Phonepe Pulse Data Visualization and Exploration')
def load_map(df, colorColumn):
    fig = px.choropleth(
        df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='State',
        color=colorColumn,
        scope="asia",
    )
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, width=900, height=800)
    fig.update_geos(fitbounds="locations", visible=False, resolution=50)
    col1.plotly_chart(fig)
    # fig.show()


engine = db.create_engine('mysql+mysqlconnector://root:12345678@127.0.0.1/Phone_pe', echo=True)
connection = engine.connect()
metadata = db.MetaData()

def getAggTransData(quater, year):
    Agg_trans = db.Table('agg_trans', metadata, autoload=True, autoload_with=engine)
    Agg_trans_query = db.select([Agg_trans.columns.State,Agg_trans.columns.Transaction_type,Agg_trans.columns.Transaction_count]).where(db.and_(Agg_trans.columns.Year == year, Agg_trans.columns.Quarter == quater)).order_by(db.desc(Agg_trans.columns.Transaction_count))
    result = connection.execute(Agg_trans_query).fetchall()
    return pd.DataFrame(result)

def getAggTrans(quater, year):
    Agg_trans = db.Table('agg_trans', metadata, autoload=True, autoload_with=engine)
    Agg_trans_query = db.select([Agg_trans.columns.State,db.func.sum(Agg_trans.columns.Transaction_count).label('TransactionCount')]).where(db.and_(Agg_trans.columns.Year == year, Agg_trans.columns.Quarter == quater)).group_by(Agg_trans.columns.State).order_by(db.desc(db.func.sum(Agg_trans.columns.Transaction_count))).limit(10)
    result = connection.execute(Agg_trans_query).fetchall()
    return pd.DataFrame(result)

def getAggUsers(quater, year):
    Agg_User = db.Table('agg_user', metadata, autoload=True, autoload_with=engine)
    Agg_User_query = db.select([Agg_User.columns.State,db.func.sum(Agg_User.columns.Count).label('RegisteredUsersCount')]).where(db.and_(Agg_User.columns.Year == year, Agg_User.columns.Quarter == quater)).group_by(Agg_User.columns.State).order_by(db.desc(db.func.sum(Agg_User.columns.Count)))
    result = connection.execute(Agg_User_query).fetchall()
    return pd.DataFrame(result)

def getMapTrans(quater, year):
    Map_Trans = db.Table('map_trans', metadata, autoload=True, autoload_with=engine)
    Map_Trans_query = db.select([Map_Trans]).where(db.and_(Map_Trans.columns.Year == year, Map_Trans.columns.Quarter == quater))
    result = connection.execute(Map_Trans_query).fetchall()
    return pd.DataFrame(result)

def getMapUsers(quater, year):
    Map_User = db.Table('map_user', metadata, autoload=True, autoload_with=engine)
    Map_User_query = db.select([Map_User]).where(db.and_(Map_User.columns.Year == year, Map_User.columns.Quarter == quater))
    result = connection.execute(Map_User_query).fetchall()
    return pd.DataFrame(result)

def getTopTransDistrict(quater, year):
    Top_Trans_District = db.Table('top_trans_district', metadata, autoload=True, autoload_with=engine)
    Top_Trans_District_query = db.select([Top_Trans_District.columns.State,Top_Trans_District.columns.District,Top_Trans_District.columns.Count]).where(db.and_(Top_Trans_District.columns.Year == year, Top_Trans_District.columns.Quarter == quater)).order_by(db.desc(Top_Trans_District.columns.Count)).limit(10)
    result = connection.execute(Top_Trans_District_query).fetchall()
    return pd.DataFrame(result)

def getTopTransPincode(quater, year):
    Top_Trans_Pincode = db.Table('top_trans_pincode', metadata, autoload=True, autoload_with=engine)
    Top_Trans_Pincode_query = db.select([Top_Trans_Pincode.columns.State,Top_Trans_Pincode.columns.Pincode,Top_Trans_Pincode.columns.Count]).where(db.and_(Top_Trans_Pincode.columns.Year == year, Top_Trans_Pincode.columns.Quarter == quater)).order_by(db.desc(Top_Trans_Pincode.columns.Count)).limit(10)
    result = connection.execute(Top_Trans_Pincode_query).fetchall()
    return pd.DataFrame(result)

def getTopUsersDistrict(quater, year):
    Top_User_District = db.Table('top_user_district', metadata, autoload=True, autoload_with=engine)
    Top_User_District_query = db.select([Top_User_District.columns.State,Top_User_District.columns.District,Top_User_District.columns.RegisteredUsers]).where(db.and_(Top_User_District.columns.Year == year, Top_User_District.columns.Quarter == quater)).order_by(db.desc(Top_User_District.columns.RegisteredUsers)).limit(10)
    result = connection.execute(Top_User_District_query).fetchall()
    return pd.DataFrame(result)

def getTopUsersPincode(quater, year):
    Top_user_Pincode = db.Table('top_user_pincode', metadata, autoload=True, autoload_with=engine)
    Top_User_Pincode_query = db.select([Top_user_Pincode.columns.State,Top_user_Pincode.columns.Pincode,Top_user_Pincode.columns.RegisteredUsers]).where(db.and_(Top_user_Pincode.columns.Year == year, Top_user_Pincode.columns.Quarter == quater)).order_by(db.desc(Top_user_Pincode.columns.RegisteredUsers)).limit(10)
    result = connection.execute(Top_User_Pincode_query).fetchall()
    return pd.DataFrame(result)

def getTopUsersStates(quater, year):
    Top_User_States = db.Table('top_user_district', metadata, autoload=True, autoload_with=engine)
    Top_User_States_query = db.select([Top_User_States.columns.State,db.func.sum(Top_User_States.columns.RegisteredUsers).label('Count')]).where(db.and_(Top_User_States.columns.Year == year, Top_User_States.columns.Quarter == quater)).group_by(Top_User_States.columns.State).order_by(db.func.sum(Top_User_States.columns.RegisteredUsers).desc()).limit(10)
    result = connection.execute(Top_User_States_query).fetchall()
    return pd.DataFrame(result)

tab1stats, tab2insights = st.tabs(["Dashboard", "Insights"])
with tab1stats:
    user_col,quater_col,year_col,dummy_col = tab1stats.columns(4, gap="small")

    filter_kind = user_col.selectbox(
        "Kind of data to see",
        ("Users", "Transactions"))


    filter_quater = quater_col.selectbox(
        "Quarter of data to see",
        ("1", "2", "3", "4"))


    filter_year = year_col.selectbox(
        "Year of data to see",
        ("2018","2019","2020","2021","2022","2023", "2024"))


    btn_search = st.button("Search")

    # df = pd.read_csv("https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/active_cases_2020-07-17_0800.csv")
    if btn_search:

        df = None
        col1, col2 = st.columns(spec=[0.7,0.3])

        if filter_kind == 'Users':
            df = getMapUsers(filter_quater, filter_year)
            try:
                load_map(df, 'RegisteredUsers')
            except:
                pass 

            df1 = getTopUsersStates(filter_quater, filter_year)
            df2 = getTopUsersDistrict(filter_quater, filter_year)
            df3 = getTopUsersPincode(filter_quater, filter_year)
            df4 = getAggUsers(filter_quater, filter_year)
            # print(df4)
            try:
                total = df4['RegisteredUsersCount'].sum()
            except:
                total = 0
                pass
                
            col2.header(":violet[Users]")
            col2.text(f"Registered PhonePe users till Q{filter_quater} {filter_year}")
            col2.title(f':violet[{total:,}]')
            #print('df444......',df4)

            tab1, tab2, tab3 = col2.tabs(["States", "Districts", "Pincodes"])

            with tab1:
                st.header("Top 10 States")
                st.dataframe(df1, hide_index=True)

            with tab2:
                st.header("Top 10 Districts")
                st.table(df2)

            with tab3:
                st.header("Top 10 Pincodes")
                st.table(df3)

            #load_map(df3, 'RegisteredUsers')
        elif filter_kind == 'Transactions':
            df = getMapTrans(filter_quater, filter_year)
            load_map(df, 'Count')
            df1 = getAggTrans(filter_quater, filter_year)
            #st.title("Top 10 States")
            #st.table(df1)
            #load_map(df1, 'Transaction_amount')
            df2 = getTopTransDistrict(filter_quater, filter_year)
            #st.title("Top 10 Districts")
            #st.table(df2)
            #load_map(df2, 'Count')
            df3 = getTopTransPincode(filter_quater, filter_year)
            #st.title("Top 10 Pincodes")
            #st.table(df3)
            #load_map(df3, 'Count')
            df4 = getAggTrans(filter_quater, filter_year)
            
            total = df4['TransactionCount'].sum()
            col2.header(":violet[Transactions]")
            col2.text(f"Registered PhonePe trasactions till Q{filter_quater} {filter_year}")
            col2.title(f':violet[{total:,}]')

            df5 = getAggTransData(filter_quater, filter_year)
            df5 = df5.drop_duplicates(subset=['State','Transaction_type', 'Transaction_count'], keep=False)
            # print(df5)
            df5_types = df5.groupby('Transaction_type').agg({'Transaction_count':'sum'}).sort_index().reset_index()
            # print(df5_types)
            df5_types.rename(columns={'Transaction_type': "Type", "Transaction_count": "Count"}, inplace=True)
            col2.dataframe(df5_types, hide_index=True)

            tab1, tab2, tab3 = col2.tabs(["States", "Districts", "Pincodes"])

            with tab1:
                st.header("Top 10 States")
                st.dataframe(df1, hide_index=True)

            with tab2:
                st.header("Top 10 Districts")
                st.table(df2)

            with tab3:
                st.header("Top 10 Pincodes")
                st.table(df3)
    




with tab2insights:
    sql_handler = mysql_work()
    agg_trans_df = sql_handler.get_uniques()

    def q1_avgtrans_user_chart(st_df):
        fig1 = px.line(st_df,x='YEAR-Quarter',y='AVG_TRANSACTION_AMOUNT/USER',title='Average Transaction Amount per User Over Time',
                            labels={'YEAR-Quarter': 'Year-Quarter', 'AVG_TRANSACTION_AMOUNT/USER': 'Avg Transaction Amount/User (Rs)'})
        fig1.update_xaxes(type='category',  tickmode='auto', showgrid=True,gridcolor='gray', showline=True, linecolor='gray' )
        fig1.update_yaxes(showgrid=True,gridcolor='gray',showline=True,linecolor='gray')
        fig1.update_traces(mode='lines+markers', line=dict(width=2), marker=dict(size=8, symbol='circle', line=dict(width=2)))
        fig1.update_layout(plot_bgcolor='white',legend=dict(orientation='h', y=1.05),title_x=0.5,font=dict(family='Arial', size=12))
        st.plotly_chart(fig1, theme='streamlit', use_container_width=True)
    def q1_avgtrans_count_chart(st_df):
        fig2 = px.line(st_df,x='YEAR-Quarter',y='AVG_TRANSACTION_AMOUNT/TRANSACTION',title='Average Transaction Amount per Transaction Over Time',
                            labels={'YEAR-Quarter': 'Year-Quarter', 'AVG_TRANSACTION_AMOUNT/TRANSACTION': 'Avg Transaction Amount/Transaction (Rs)'})
        fig2.update_xaxes(type='category',  tickmode='auto', showgrid=True,gridcolor='gray', showline=True, linecolor='gray' )
        fig2.update_yaxes(showgrid=True,gridcolor='gray',showline=True,linecolor='gray')
        fig2.update_traces(mode='lines+markers', line=dict(width=2), marker=dict(size=8, symbol='circle', line=dict(width=2)))
        fig2.update_layout(plot_bgcolor='white',legend=dict(orientation='h', y=1.05),title_x=0.5,font=dict(family='Arial', size=12))
        st.plotly_chart(fig2, theme='streamlit', use_container_width=True)
    def q1_bar_chart(bar_df):
        bar_df = bar_df.sort_values(by='Percentage_Users', ascending=False)
        # Create the horizontal bar chart using Plotly Express
        fig3 = px.bar(bar_df, x='Percentage_Users', y='BRAND', text='TRANSACTION_COUNT', orientation='h')
        fig3.update_layout(
            title=f"Percentage Device Users for the {', '.join(bar_df['STATE'].unique())}",
            xaxis_title='Percentage Users',
            yaxis_title='Brand',
            showlegend=True,
            bargap=0.1,  # Adjust the gap between bars
            yaxis_categoryorder='total ascending',   )
        fig3.update_traces(texttemplate='%{text}', textposition='inside')
        st.plotly_chart(fig3, theme='streamlit', use_container_width=True)
    def q1_pie_chart(pie_df,input_state):
        fig4 = px.pie(pie_df, values='Percentage_Users', names='TRANSACTION_TYPE', hole=0.4, title=f"Transaction Type in % for {input_state} ")
        st.plotly_chart(fig4, theme=None,use_container_width=True)
    def q1_bubble_bar_chart(bubble_df,input_state):
        fig5 = px.scatter(bubble_df, x='DISTRICT', y='AVG_TRANSACTION_COUNT', size='AVG_USERS', title='AVG_TRANSACTION',
                    labels={'DISTRICT': 'District', 'AVG_TRANSACTION_COUNT': 'Avg Transaction Count'},
                    size_max=40)  # Adjust size_max to control bubble size
        fig5.update_traces(text=bubble_df['AVG_USERS'].apply(lambda x: f'{x:.2f} lakhs'))
        fig5.update_xaxes(tickformat=".0s", title_text="District")
        fig5.update_yaxes(tickformat=".0s", title_text="Transaction Count (Crores)")
        fig5.update_traces(marker=dict(line=dict(width=2, color='DarkSlateGrey')), selector=dict(mode='markers'))
        st.plotly_chart(fig5, theme='streamlit',use_container_width=True)
        fig6 = px.bar(bubble_df, x='DISTRICT', y='AVG_TRANSACTION_COUNT', title=f'AVG_TRANSACTION_COUNT of {input_state}',
                labels={'DISTRICT': 'District', 'AVG_TRANSACTION_COUNT': 'Avg Transaction Count'})
        fig6.update_xaxes(title_text="District")
        fig6.update_yaxes(title_text="Avg Transaction Count")
        st.plotly_chart(fig6, theme='streamlit',use_container_width=True)
    def display_timeseries_nation(user_df,sta_df):
        fig7 = px.line(user_df, x='YEAR-Quarter', y='USERS_ADDED', title='Users Added Over Time',
        markers=True, line_shape='linear',render_mode='svg',  
        color_discrete_sequence=['blue'])
        # Customize the layout for a more appealing chart
        fig7= px.line(user_df, x='YEAR-Quarter', y='USERS_ADDED', title="INDIA'S Users Added Over Time")
        fig7.update_traces(mode='lines+markers', line=dict(width=2), marker=dict(size=8, symbol='circle', line=dict(width=2)))
        fig7.update_layout(xaxis_title='Year-Quarter',yaxis_title='Users Added',
            xaxis=dict(tickvals=user_df['YEAR-Quarter'].tolist(), tickangle=45),
            yaxis=dict(showgrid=True, zeroline=False),
            legend=dict(title='Legend'),)
        st.plotly_chart(fig7, theme='streamlit',use_container_width=True)
        filtered_df = sta_df[sta_df['YEAR'].isin([2018, 2023])]
        fig8 = go.Figure()
        fig8.add_trace(go.Scatter( x=filtered_df[filtered_df['YEAR'] == 2018]['REGISTERED_USERS'],
                y=filtered_df[filtered_df['YEAR'] == 2018]['STATE'],
                mode='markers',marker=dict(size=16,color="gray", ),text="2018",
                hovertemplate='<b>%{y}</b><br>Registered Users: %{x}<br>Year: 2018',))

        fig8.add_trace(go.Scatter(x=filtered_df[filtered_df['YEAR'] == 2023]['REGISTERED_USERS'],
                y=filtered_df[filtered_df['YEAR'] == 2023]['STATE'],mode='markers',
                marker=dict(size=16,color="lightskyblue", 
                ),text="2023",hovertemplate='<b>%{y}</b><br>Registered Users: %{x}<br>Year: 2023',))

        #lines connecting 2018 and 2023
        for state in filtered_df['STATE'].unique():
            x_values_2018 = filtered_df[(filtered_df['YEAR'] == 2018) & (filtered_df['STATE'] == state)]['REGISTERED_USERS']
            x_values_2023 = filtered_df[(filtered_df['YEAR'] == 2023) & (filtered_df['STATE'] == state)]['REGISTERED_USERS']
            y_value = state
            #Line color
            fig8.add_shape(go.layout.Shape(type="line",x0=x_values_2018.iloc[0],y0=y_value,x1=x_values_2023.iloc[0],
                    y1=y_value,line=dict(color="green",width=2,),))
        fig8.update_layout(title='Users Growth For Diferent States',xaxis_title='Registered Users',
            yaxis_title='State',showlegend=False,height=1200, )
        st.plotly_chart(fig8, theme='streamlit',use_container_width=True)
    def display_scatter(year_df):
        fig9= px.scatter(year_df, x='REGISTERED_USERS', y='TRANSACTION_AMOUNT', trendline='ols')
        fig9.update_layout(title='REGISTERED_USERS vs. TRANSACTION_AMOUNT')
        st.plotly_chart(fig9, theme='streamlit',use_container_width=True)
        fig10= px.scatter(year_df, x='TRANSACTION_COUNT', y='TRANSACTION_AMOUNT', trendline='ols')
        fig10.update_layout(title='TRANSACTION_COUNT vs. TRANSACTION_AMOUNT')
        st.plotly_chart(fig10, theme='streamlit',use_container_width=True)
    def display_multi_line_trans_type(tr_type_df):
        fig11 = px.line(tr_type_df, x='YEAR', y='TRANSACTION_COUNT', color='TRANSACTION_TYPE', 
                title='Transaction Count by Year and Type')
        fig11.update_xaxes(title_text='Year')
        fig11.update_yaxes(title_text='Transaction Count',type='log')
        st.plotly_chart(fig11, theme='streamlit',use_container_width=True)
        fig12 = px.line(tr_type_df, x='YEAR', y='TRANSACTION_AMOUNT', color='TRANSACTION_TYPE', 
                    title='Transaction Amount by Year and Type')
        fig12.update_xaxes(title_text='Year')
        fig12.update_yaxes(title_text='Transaction Amount', type='log')
        st.plotly_chart(fig12, theme='streamlit',use_container_width=True)
    def display_stack_bar(state_trans_df):
        fig13 = go.Figure()
        fig13.add_trace(go.Bar(x=state_trans_df['AVG_TRANSACTION_AMOUNT/TRANSACTION'],
            y=state_trans_df['STATE'],orientation='h',
            name='AVG_TRANSACTION_AMOUNT/TRANSACTION',marker=dict(color='royalblue')))
        fig13.add_trace(go.Bar(x=state_trans_df['AVG_TRANSACTION_AMOUNT/USER'],
            y=state_trans_df['STATE'],orientation='h',
            name='AVG_TRANSACTION_AMOUNT/USER',marker=dict(color='orange')))
        fig13.update_layout(title='Average Transaction Amount by State',
            xaxis_title='Average Amount',yaxis_title='State',
            barmode='relative',bargap=0.3,height=1400,width=1000)
        st.plotly_chart(fig13, theme='streamlit',use_container_width=True)
    def display_top5_state_bar(state_trans_df):
        state_trans_df=state_trans_df.sort_values(by='AVG_TRANSACTION_AMOUNT',ascending=False)
        top_5_states=state_trans_df.head(5)
        fig14 = px.bar(top_5_states,x='STATE',y='AVG_TRANSACTION_AMOUNT',
            color='STATE',title='Top 5 states by AVG_TRANSACTION_AMOUNT (in crores)',
            labels={'STATE': 'STATE', 'AVG_TRANSACTION_AMOUNT': 'AVG Amount (in crores)'},
            hover_name='STATE')
        fig14.update_traces(texttemplate='%{y:.2f} crores', textposition='outside')
        st.plotly_chart(fig14, theme='streamlit',use_container_width=True)
    def display_top5_district_pin_bar(dis_trans_df,pin_trans_df):
        dis_trans_df = dis_trans_df.sort_values(by='AVG_TRANSACTION_AMOUNT(in crores)', ascending=False)
        #for top 5 districts
        top_5_districts = dis_trans_df.head(5)
        fig15 = px.bar(top_5_districts,x='DISTRICT',y='AVG_TRANSACTION_AMOUNT(in crores)',
            color='STATE',title='Top 5 Districts by AVG_TRANSACTION_AMOUNT (in crores)',
            labels={'DISTRICT': 'District', 'AVG_TRANSACTION_AMOUNT(in crores)': 'AVG Amount (in crores)'},
            hover_name='STATE')
        fig15.update_traces(texttemplate='%{y:.2f} crores', textposition='outside')
        st.plotly_chart(fig15, theme='streamlit',use_container_width=True)
        pin_trans_df= pin_trans_df.sort_values(by='AVG_TRANSACTION_AMOUNT(in crores)', ascending=False)
        
        top_5_pin = pin_trans_df.head(5)
        fig16 = px.bar(top_5_pin,x='PINCODE',y='AVG_TRANSACTION_AMOUNT(in crores)',
            color='STATE',title='Top 5 pincodes by AVG_TRANSACTION_AMOUNT (in crores)',
            labels={'PINCODE': 'Pincode', 'AVG_TRANSACTION_AMOUNT(in crores)': 'AVG Amount (in crores)'},
            hover_name='STATE')
        fig16.update_traces(texttemplate='%{y:.2f} crores', textposition='outside')
        st.plotly_chart(fig16, theme='streamlit',use_container_width=True)
    def display_last5_state_district_pin_bar(state_trans_df,dis_trans_df,pin_trans_df):
        state_trans_df=state_trans_df.sort_values(by='AVG_TRANSACTION_AMOUNT',ascending=True)
        last_5_states=state_trans_df.head(5)
        fig17= px.bar(last_5_states,x='STATE',y='AVG_TRANSACTION_AMOUNT',
            color='STATE',title='Least 5 states by AVG_TRANSACTION_AMOUNT (in crores)',
            labels={'STATE': 'STATE', 'AVG_TRANSACTION_AMOUNT': 'AVG Amount (in crores)'},
            hover_name='STATE')
        fig17.update_traces(texttemplate='%{y:.2f} crores', textposition='outside')
        st.plotly_chart(fig17, theme='streamlit',use_container_width=True)
        dis_trans_df = dis_trans_df.sort_values(by='AVG_TRANSACTION_AMOUNT(in crores)', ascending=True)
        #for districts
        last_5_districts = dis_trans_df.head(5)
        fig18 = px.bar(last_5_districts,x='DISTRICT',y='AVG_TRANSACTION_AMOUNT(in crores)',
            color='STATE',title='Least 5 Districts by AVG_TRANSACTION_AMOUNT (in crores)',
            labels={'DISTRICT': 'District', 'AVG_TRANSACTION_AMOUNT(in crores)': 'AVG Amount (in crores)'},
            hover_name='STATE')
        fig18.update_traces(texttemplate='%{y:.2f} crores', textposition='outside')
        st.plotly_chart(fig18, theme='streamlit',use_container_width=True)
        #for pincode 
        pin_trans_df= pin_trans_df.sort_values(by='AVG_TRANSACTION_AMOUNT(in crores)', ascending=True)
        last_5_pin = pin_trans_df.head(5)
        fig19 = px.bar(last_5_pin,x='PINCODE',y='AVG_TRANSACTION_AMOUNT(in crores)',
            color='STATE',title='Least 5 pincodes by AVG_TRANSACTION_AMOUNT (in crores)',
            labels={'PINCODE': 'Pincode', 'AVG_TRANSACTION_AMOUNT(in crores)': 'AVG Amount (in crores)'},
            hover_name='STATE')
        fig19.update_traces(texttemplate='%{y:.2f} crores', textposition='outside')
        st.plotly_chart(fig19, theme='streamlit',use_container_width=True)
    def display_ff_trans_table(state_avgtrans_user_df):
        state_avgtrans_user_df=state_avgtrans_user_df.sort_values(by='AVG_TRANSACTION_AMOUNT',ascending=False)
        state_trans_list=state_avgtrans_user_df.head(20)
        filtered_state_trans_list = state_trans_list[['STATE', 'AVG_TRANSACTION_AMOUNT', 'REGISTERED_USERS']]
        table_data = [filtered_state_trans_list.columns] + filtered_state_trans_list.values.tolist()
        fig20 = ff.create_table(table_data, height_constant=25)
        fig20.update_layout(title='State Transaction Data',margin=dict(l=10, r=10, t=60, b=10),)
        st.plotly_chart(fig20, theme='streamlit',use_container_width=True)
    def display_avg_trans_top(state_avgtrans_user_df):
        #top values
        state_avgtrans_user_df=state_avgtrans_user_df.sort_values(by='AVG_TRANSACTION_AMOUNT/TRANSACTION',ascending=False)
        top_avgtrans_df=state_avgtrans_user_df.head(7)
        fig21 = px.bar(top_avgtrans_df,x='STATE',y='AVG_TRANSACTION_AMOUNT/TRANSACTION',
            color='STATE',title='Top 7 State transferring most Amount per Transaction',
            labels={'STATE': 'State', 'AVG_TRANSACTION_AMOUNT/TRANSACTION': 'AVG_TRANSACTION_AMOUNT/TRANSACTION'},
            hover_name='REGISTERED_USERS')
        fig21.update_traces(texttemplate='%{y:.2f} crores', textposition='outside')
        st.plotly_chart(fig21, theme='streamlit',use_container_width=True)
    def display_avg_trans_least(state_avgtrans_user_df):
        #least values
        state_avgtrans_user_df=state_avgtrans_user_df.sort_values(by='AVG_TRANSACTION_AMOUNT/TRANSACTION',ascending=True)
        last_avgtrans_df=state_avgtrans_user_df.head(7)
        fig22 = px.bar(last_avgtrans_df,x='STATE',y='AVG_TRANSACTION_AMOUNT/TRANSACTION',
            color='STATE',title='Least 7 State doing less Amount of transactions per Transaction',
            labels={'STATE': 'State', 'AVG_TRANSACTION_AMOUNT/TRANSACTION': 'AVG_TRANSACTION_AMOUNT/TRANSACTION'},
            hover_name='REGISTERED_USERS')
        fig22.update_traces(texttemplate='%{y:.2f} crores', textposition='outside')
        st.plotly_chart(fig22, theme='streamlit',use_container_width=True)
    def display_avg_user_top(state_avgtrans_user_df):
        #top values
        state_avgtrans_user_df=state_avgtrans_user_df.sort_values(by='AVG_TRANSACTION_AMOUNT/USER',ascending=False)
        top_avguser_df=state_avgtrans_user_df.head(7)
        fig23 = px.bar(top_avguser_df,x='STATE',y='AVG_TRANSACTION_AMOUNT/USER',
            color='STATE',title='Top 7 State transferring most Amount based on a user',
            labels={'STATE': 'State', 'AVG_TRANSACTION_AMOUNT/USER': 'AVG_TRANSACTION_AMOUNT/USER'},
            hover_name='REGISTERED_USERS')
        fig23.update_traces(texttemplate='%{y:.2f} crores', textposition='outside')
        st.plotly_chart(fig23, theme='streamlit',use_container_width=True)
    def display_avg_user_least(state_avgtrans_user_df):
        #least values
        state_avgtrans_user_df=state_avgtrans_user_df.sort_values(by='AVG_TRANSACTION_AMOUNT/USER',ascending=True)
        last_avguser_df=state_avgtrans_user_df.head(7)
        fig24= px.bar(last_avguser_df,x='STATE',y='AVG_TRANSACTION_AMOUNT/USER',
            color='STATE',title='Least 7 State doing less Amount of transactions based on a user',
            labels={'STATE': 'State', 'AVG_TRANSACTION_AMOUNT/USER': 'AVG_TRANSACTION_AMOUNT/USER'},
            hover_name='REGISTERED_USERS')
        fig24.update_traces(texttemplate='%{y:.2f} crores', textposition='outside')
        st.plotly_chart(fig24, theme='streamlit',use_container_width=True)    
    tab2insights.title("Insights from data")
    question_dict={'What are the common insights of a state ':1,
                   'How the Phonepe performing over period and in different states':2,
                   'How are Transaction amount,Registered Users,Transaction Count are related':3,
                   'What are major Transaction over the year':4,
                   'What are the Average Transactions Insights over State':5,
                   'What are the top 5 districts,state,pincode had done highest transaction amount':6,
                   'What are the least 5 districts,state,pincode had done highest transaction amount':7,
                   'Which states are transferring more money (display in table)' :8,
                   'States having high as well as low transactions amount in a Transaction(limited to 7)':9,
                   'States having high as well as low transactions amount by a User(limited to 7)':10 }
    question=tab2insights.selectbox('Select the insight questions from phonepe data',question_dict.keys())
    answer=question_dict.get(question)
    if answer==1:
        input_state=st.selectbox('Select the State for insights',agg_trans_df['MYSQL_STATES'].unique())
        st_df=sql_handler.get_state_insights(input_state)
        bar_df=sql_handler.get_state_insights_bar(input_state)
        pie_df=sql_handler.get_state_insights_donut(input_state)
        bubble_df=sql_handler.get_state_insights_bubble_bar(input_state)
        q1_avgtrans_count_chart(st_df)
        
        q1_bar_chart(bar_df)
        q1_bubble_bar_chart(bubble_df,input_state)
        col1,col2=st.columns((2))
        with col1:
            q1_pie_chart(pie_df,input_state)
        with col2:
            q1_avgtrans_user_chart(st_df)
        
    elif answer==2:
        user_df,sta_df=sql_handler.get_nation_deatils()
        display_timeseries_nation(user_df,sta_df)
    elif answer==3:
        year_df=sql_handler.get_relative_insights()
        display_scatter(year_df)
    elif answer==4:
        trans_type_df=sql_handler.get_trans_type_insights()
        display_multi_line_trans_type(trans_type_df)
    elif answer==5:
        state_avgtrans_user_df=sql_handler.get_avgtrans_user_count()
        display_stack_bar(state_avgtrans_user_df)
    elif answer==6:
        dis_trans_df,pin_trans_df=sql_handler.get_top5_districts()
        state_avgtrans_user_df=sql_handler.get_avgtrans_user_count()
        display_top5_state_bar(state_avgtrans_user_df)
        display_top5_district_pin_bar(dis_trans_df,pin_trans_df)
    elif answer==7:
        dis_trans_df,pin_trans_df=sql_handler.get_top5_districts()
        state_avgtrans_user_df=sql_handler.get_avgtrans_user_count()
        display_last5_state_district_pin_bar(state_avgtrans_user_df,dis_trans_df,pin_trans_df)
    elif answer==8:
        state_avgtrans_user_df=sql_handler.get_avgtrans_user_count()
        display_ff_trans_table(state_avgtrans_user_df)
    elif answer==9:
        state_avgtrans_user_df=sql_handler.get_avgtrans_user_count()
        display_avg_trans_top(state_avgtrans_user_df)
        display_avg_trans_least(state_avgtrans_user_df)
    elif answer==10:
        state_avgtrans_user_df=sql_handler.get_avgtrans_user_count()
        display_avg_user_top(state_avgtrans_user_df)
        display_avg_user_least(state_avgtrans_user_df)
        