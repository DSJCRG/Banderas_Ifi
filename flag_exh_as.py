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
                        'jrivas',
                        '8khHAM=B=q$ZGr6N',
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
query_1 = """ 
    
select distinct YEAR(VisitStartDateTime) as year, MONTH(VisitStartDateTime) as month, DAY(VisitStartDateTime) as day, sessionuid, sceneuid, VisitStartDateTime, locationId, SceneType, SubSceneType, TipoMueble, FabMueble, productid from dbo.as_ir_report    
where YEAR(VisitStartDateTime) = '2021' and SubSceneType = 'Exhibiciones Especiales' and FabMueble in ('Exhibición Coca-Cola', 'Exhibición Competencia')

        """


# %%
df = pd.DataFrame(get_query(query_1))
#df = df.sort_values(by="VisitStartDateTime",ascending=False)
#df


# %%
df['date_format']=pd.to_datetime(df[['year','month','day']])
#df.info()


# %%
df=df.rename(columns={"sessionuid": "SessionUid"})


# %%
df=df.drop_duplicates(subset=['SessionUid'])


# %%
query_2 = """ 
    
select distinct SessionUid, LocationId, UserId, DateId, ChainDsc, Format, Clasification, ICE, exh_ko, exh_share, is_reported from dbo.r_autoservicios_v3    


        """


# %%
df2 = pd.DataFrame(get_query(query_2))


# %%
dff = pd.merge(df2, df, on='SessionUid', how='inner')


# %%
table = pd.pivot_table(dff, values=['exh_ko','exh_share'], index=['UserId', 'LocationId', 'is_reported', 'date_format', 'month', 'day', 'SessionUid'],
                     fill_value=0)
table=pd.DataFrame(table)
#table


# %%
table = table.reset_index()


# %%
table['exh_comp']=table['exh_share']-table['exh_ko']


# %%
moni=table.groupby(['UserId','LocationId', 'SessionUid', 'is_reported', 'date_format', 'month', 'day'])['exh_ko', 'exh_comp'].sum()


# %%
table2 = moni.reset_index()


# %%
table2=table2.sort_values(['LocationId', 'date_format'])


# %%
table2["Coca-Cola"] = table2["exh_ko"].shift(1)
table2["Competencia"] = table2["exh_comp"].shift(1)
#table2.dtypes


# %%
table2 = table2.fillna(0)


# %%
table2 = table2.astype({"Coca-Cola": int, "Competencia": int, "LocationId": object})


# %%
table2["LocationIdChanged"] = table2["LocationId"].shift(1, fill_value=table2["LocationId"].head(1)) != table2["LocationId"]
#table2.dtypes


# %%
table2["LocationIdChanged"] = table2["LocationIdChanged"].astype(object)


# %%
for OutletCodeChanged in table2:
      s = table2['LocationIdChanged'].unique()
      for i in s:
          if i == 'True':
              Flag_CC = 'change'
              
          else: 
              Flag_CC = table2["exh_ko"] - table2["Coca-Cola"]

              table2['Flag_CC']=Flag_CC
              
      #print(Flag_CC)   


# %%
for OutletCodeChanged in table2:
      s = table2['LocationIdChanged'].unique()
      for i in s:
          if i == 'True':
              Flag_Comp = 'change'
              
          else: 
              Flag_Comp = table2["exh_comp"] - table2["Competencia"]

              table2['Flag_Comp']=Flag_Comp
              
      #print(Flag_Comp)  


# %%
table_final = table2[['UserId','LocationId','SessionUid', 'is_reported', 'date_format', 'month', 'day', 'LocationIdChanged', 'exh_ko', 'Flag_CC', 'exh_comp', 'Flag_Comp']]
table_final


# %%
table_final = table_final.fillna(0)


# %%
print("flags-tt - Borrando los datos de la tabla")
query_clear_Exh_as = """ 
    
DELETE FROM reportes.Exhibidores_as

        """


# %%
def get_engine():
    connection_string = "mssql+pyodbc://{0}@{2}:{1}@{2}.database.windows.net:{4}/{3}?driver=ODBC+Driver+17+for+SQL+Server".format(
                        'jrivas',
                        '8khHAM=B=q$ZGr6N',
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
conn.execute(query_clear_Exh_as)
conn.close()
#get_query(query_clear_Exh_as_cc_comp)


print("flags-as - Llenando la tabla")


# %%
engine = get_engine()
table_final.to_sql('Exhibidores_as', con=engine, schema='reportes', if_exists='append', index=False)


# %%



