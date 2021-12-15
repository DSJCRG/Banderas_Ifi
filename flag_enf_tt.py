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
                        XXXXX',
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

select distinct tt.LocationId, tt.user_id, tt.date_id, tt.is_reported, YEAR(srl.VisitStartDateTime) as year, MONTH(srl.VisitStartDateTime) as month, DAY(VisitStartDateTime) as day, sl.Question, sl.Response, srl.* from dbo.SceneReportList srl 
join dbo.SurveyList sl on sl.SessionUid = srl.SessionUId
join dbo.a_tt_10_CMP_r_traditional_data_01 tt on tt.SessionUid = srl.SessionUid
where YEAR(srl.VisitStartDateTime) = '2021' and srl.LocationClusterName = 'Traditional' and srl.UserRole = 'Auditor' and sl.Question = '¿La tienda tiene libre acceso a Clientes?' 
 
"""


# %%
df = pd.DataFrame(get_query(query))
#df = df.sort_values(by="VisitStartDateTime",ascending=False)
#df


# %%
df.info()


# %%
df['date_format']=pd.to_datetime(df[['year','month','day']])
#df.info()


# %%
df=df.drop_duplicates(subset=['SessionUid'])
#df


# %%
table = pd.pivot_table(df, values='Status', index=['UserName', 'OutletCode', 'user_id', 'date_id', 'is_reported', 'date_format', 'month', 'day', 'Question', 'Response', 'SessionUid'],
                    columns=['SubSceneType'], aggfunc={'Status':len}, fill_value=0)
table=pd.DataFrame(table)
#table


# %%
table = table.reset_index()
#table


# %%
ifi=table.groupby(['UserName', 'OutletCode', 'user_id', 'date_id', 'is_reported', 'date_format', 'SessionUid', 'Question', 'Response', 'month', 'day'])['Enfriador Coca-Cola', 'Enfriador Competencia', 'Enfriador Detallista'].sum()

#ifi  


# %%
table2 = ifi.reset_index()
#table2


# %%
table2=table2.sort_values(['OutletCode', 'date_format'])


# %%
table2["Coca-Cola"] = table2["Enfriador Coca-Cola"].shift(1)
table2["Competencia"] = table2["Enfriador Competencia"].shift(1)
table2["Detallista"] = table2["Enfriador Detallista"].shift(1)
#table2.dtypes


# %%
table2 = table2.fillna(0)
#table2


# %%
table2 = table2.astype({"Coca-Cola": int, "Competencia": int, "Detallista": int})
#table2


# %%
table2["OutletCodeChanged"] = table2["OutletCode"].shift(1, fill_value=table2["OutletCode"].head(1)) != table2["OutletCode"]
#table2.dtypes


# %%
table2["OutletCodeChanged"] = table2["OutletCodeChanged"].astype(object)
#table2.dtypes


# %%
for OutletCodeChanged in table2:
      s = table2['OutletCodeChanged'].unique()
      for i in s:
          if i == 'True':
              Flag_CC = 'change'
              
          else: 
              Flag_CC = table2["Enfriador Coca-Cola"] - table2["Coca-Cola"]

              table2['Flag_CC']=Flag_CC
              
      #print(Flag_CC)     


# %%
for OutletCodeChanged in table2:
      s = table2['OutletCodeChanged'].unique()
      for i in s:
          if i == 'True':
              Flag_Comp = 'change'
              
          else: 
              Flag_Comp = table2["Enfriador Competencia"] - table2["Competencia"]

              table2['Flag_Comp']=Flag_Comp
              
      #print(Flag_Comp)    


# %%
for OutletCodeChanged in table2:
      s = table2['OutletCodeChanged'].unique()
      for i in s:
          if i == 'True':
              Flag_Det = 'change'
              
          else: 
              Flag_Det = table2["Enfriador Detallista"] - table2["Detallista"]

              table2['Flag_Det']=Flag_Det
              
      #print(Flag_Det)      


# %%
table_final = table2[['UserName','user_id','OutletCode','date_id', 'is_reported', 'SessionUid', 'Question', 'Response', 'month', 'day', 'OutletCodeChanged', 'Enfriador Coca-Cola', 'Coca-Cola', 'Flag_CC', 'Enfriador Competencia', 'Competencia', 'Flag_Comp', 'Enfriador Detallista', 'Detallista', 'Flag_Det']]
#table_final


# %%
query = """ 

select distinct sl.SessionUId, sl.LocationId from dbo.SessionList sl
where YEAR(sl.VisitStartDateTime) = '2021' 
 

        """


# %%
dff = pd.DataFrame(get_query(query))
#df = df.sort_values(by="VisitStartDateTime",ascending=False)
dff=dff.rename(columns={"SessionUId": "SessionUid"})
dff.dtypes


# %%
Ifi_historico = pd.merge(table_final, dff, on='SessionUid', how='inner')
#Ifi_historico


# %%
Ifi_historico=Ifi_historico[['UserName', 'user_id', 'OutletCode', 'LocationId', 'date_id', 'is_reported','SessionUid', 'Question', 'Response', 'month', 'day', 'OutletCodeChanged', 'Enfriador Coca-Cola', 'Flag_CC', 'Enfriador Competencia', 'Flag_Comp', 'Enfriador Detallista', 'Flag_Det']]


# %%
Ifi_historico = Ifi_historico.fillna(0)


# %%
print("flags-tt - Borrando los datos de la tabla")
query_clear_Enf_tt = """ 
    
DELETE FROM Ifi_Enfriadores_tot

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
conn.execute(query_clear_Enf_tt)
conn.close()
#get_query(query_clear_Exh_as_cc_comp)


print("flags-as - Llenando la tabla")


# %%
engine = get_engine()
Ifi_historico.to_sql('Ifi_Enfriadores_tot', con=engine, schema='reportes', if_exists='append', index=False)


# %%



