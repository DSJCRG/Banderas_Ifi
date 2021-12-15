# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd 
import pyodbc
import sqlalchemy
import datetime as dt 
import numpy as np



#Se definen las conexiones

def get_engine():
    connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'ko_ice_reader',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
    res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
    return res
    
def get_query(query):
    engine = get_engine()
    res = pd.read_sql(query, con=engine)
    return res


# %%
query = """ 

select distinct tt.store_id, tt.user_id, tt.date_id, tt.establishment_type, tt.establishment_size, tt.first_position_points, tt.first_position_objetives, tt.first_position_cooler_coca, tt.first_position_complishment, tt.is_reported, tt.ice_points, tt.LocationId, YEAR(srl.VisitStartDateTime) as year, MONTH(srl.VisitStartDateTime) as month, DAY(VisitStartDateTime) as day, sl.Question, sl.Response, srl.* 
from dbo.SceneReportList srl 
join dbo.SurveyList sl on sl.SessionUid = srl.SessionUId
join dbo.a_tt_10_CMP_r_traditional_data_01 tt on tt.SessionUid = srl.SessionUid
where YEAR(srl.VisitStartDateTime) = '2021' and srl.LocationClusterName = 'Traditional' and srl.UserRole = 'Auditor' and sl.Question = 'Tipo Enfriador Detallista' and YEAR(CONVERT(varchar, date_id, 103))='2021' and srl.Scene = 'Enfriador' and srl.SubSceneType in ('Enfriador Coca-Cola', 'Enfriador Detallista')

"""


# %%
df = pd.DataFrame(get_query(query))
#df = df.sort_values(by="VisitStartDateTime",ascending=False)
#df


# %%
df['date_format']=pd.to_datetime(df[['year','month','day']])
#df.info()


# %%
df=df.drop_duplicates(subset=['SessionUid'])
#df.info()


# %%
df['first_position_cooler_coca'] = df['first_position_cooler_coca'].astype(int)
df['first_position_complishment'] = df['first_position_complishment'].astype(int)
#df.info()


# %%
df=df.drop(columns=['IsQAEnvironment'])
#df.info()


# %%
table = pd.pivot_table(df, values=['first_position_complishment'], index=['UserName', 'OutletCode', 'user_id', 'date_id', 'is_reported', 'date_format', 'month', 'day', 'Question', 'Response', 'SessionUid'], columns=['SubSceneType'], aggfunc={'first_position_complishment':len},
                     fill_value=0)
table=pd.DataFrame(table)
#table


# %%
table = table.reset_index()


# %%
df1 = pd.DataFrame(data=table)


# %%
array_df=df1.values


# %%
df2 = pd.DataFrame(array_df, 
             columns=['UserName', 'OutletCode', 'user_id', 'date_id', 'is_reported', 'date_format', 'month', 'day', 'Question', 'Response', 'SessionUid', 'Enfriador Coca-Cola', 'Enfriador Detallista'])


# %%
ifi=df2.groupby(['UserName', 'OutletCode', 'user_id', 'date_id', 'is_reported', 'date_format', 'SessionUid', 'Question', 'Response', 'month', 'day'])['Enfriador Coca-Cola', 'Enfriador Detallista'].sum()


# %%
table2 = ifi.reset_index()


# %%
table2=table2.sort_values(['OutletCode', 'date_format'])


# %%
table2["Coca-Cola"] = table2["Enfriador Coca-Cola"].shift(1)
table2["Detallista"] = table2["Enfriador Detallista"].shift(1)


# %%
table2 = table2.fillna(0)


# %%
table2 = table2.astype({"Coca-Cola": int, "Detallista": int})


# %%
table2["OutletCodeChanged"] = table2["OutletCode"].shift(1, fill_value=table2["OutletCode"].head(1)) != table2["OutletCode"]


# %%
table2["OutletCodeChanged"] = table2["OutletCodeChanged"].astype(object)


# %%
for OutletCodeChanged in table2:
      s = table2['OutletCodeChanged'].unique()
      for i in s:
          if i == 'True':
              Flag_CC = 'change'
              
          else: 
              Flag_CC = table2["Enfriador Coca-Cola"] - table2["Coca-Cola"]

              table2['Flag_CC']=Flag_CC


# %%
for OutletCodeChanged in table2:
      s = table2['OutletCodeChanged'].unique()
      for i in s:
          if i == 'True':
              Flag_Det = 'change'
              
          else: 
              Flag_Det = table2["Enfriador Detallista"] - table2["Detallista"]

              table2['Flag_Det']=Flag_Det


# %%
table_final = table2[['UserName','user_id','OutletCode','date_id', 'is_reported', 'SessionUid', 'Question', 'Response', 'month', 'day', 'OutletCodeChanged', 'Enfriador Coca-Cola', 'Coca-Cola', 'Flag_CC', 'Enfriador Detallista', 'Detallista', 'Flag_Det']]


# %%
query = """ 

select distinct sl.SessionUId, sl.LocationId from dbo.SessionList sl
where YEAR(sl.VisitStartDateTime) = '2021' 
 

        """


# %%
dff = pd.DataFrame(get_query(query))
#df = df.sort_values(by="VisitStartDateTime",ascending=False)
dff=dff.rename(columns={"SessionUId": "SessionUid"})


# %%
Ifi_historico = pd.merge(table_final, dff, on='SessionUid', how='inner')


# %%
Ifi_historico=Ifi_historico[['UserName', 'user_id', 'OutletCode', 'LocationId', 'date_id', 'is_reported','SessionUid', 'Question', 'Response', 'month', 'day', 'OutletCodeChanged', 'Enfriador Coca-Cola', 'Flag_CC', 'Enfriador Detallista', 'Flag_Det']]


# %%
Ifi_historico = Ifi_historico.fillna(0)


# %%
print("flags-tt - Borrando los datos de la tabla")
query_clear_1stPos_tt = """ 
    
DELETE FROM Primera_Posicion_tt

        """


# %%
def get_engine():
    connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        'XXXXX',
                        'koice',
                        'ko_ice',
                        1433)
    res = sqlalchemy.engine.create_engine(connection_string, connect_args={'autocommit': True})
    return res
    
def get_query(query):
    engine = get_engine()
    res = pd.read_sql(query, con=engine)
    return res

eng = get_engine()
conn = eng.connect()
conn.execute(query_clear_1stPos_tt)
conn.close()
#get_query(query_clear_Exh_as_cc_comp)


print("flags-as - Llenando la tabla")


# %%
engine = get_engine()
Ifi_historico.to_sql('Primera_Posicion_tt', con=engine, schema='reportes', if_exists='append', index=False)


