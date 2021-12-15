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

query_1 = """ 
   
select distinct YEAR(VisitStartDateTime) as year, MONTH(VisitStartDateTime) as month, DAY(VisitStartDateTime) as day, sessionuid, sceneuid, VisitStartDateTime, locationId, SceneType, SubSceneType, TipoMueble, FabMueble, productid from dbo.as_ir_report    
where YEAR(VisitStartDateTime) = '2021' and SceneType = 'Enfriador' and FabMueble in ('Enfriador Coca-Cola')

        """


# %%
df = pd.DataFrame(get_query(query_1))


# %%
df.head()


# %%
df['date_format']=pd.to_datetime(df[['year','month','day']])


# %%
df=df.rename(columns={"sessionuid": "SessionUid"})
df.dtypes


# %%
df.duplicated(subset='SessionUid', keep='first').sum()


# %%
df=df.drop_duplicates(subset=['SessionUid'])
df.info()


# %%
query_2 = """ 
   
   select distinct SessionUid, LocationId, UserId, DateId, ChainDsc, Format, Clasification, ICE, FL_Base_100, is_reported from dbo.r_autoservicios_v3    

        """


# %%
df2 = pd.DataFrame(get_query(query_2))


# %%
dff = pd.merge(df2, df, on='SessionUid', how='inner')
dff.info()


# %%
table = pd.pivot_table(dff, values=['FL_Base_100'], index=['UserId', 'LocationId', 'is_reported', 'date_format', 'month', 'day', 'SessionUid'],
                     fill_value=0)
table=pd.DataFrame(table)
table


# %%
table = table.reset_index()
table


# %%
table.info()


# %%
moni=table.groupby(['UserId','LocationId', 'SessionUid', 'is_reported', 'date_format', 'month', 'day'])['FL_Base_100'].sum()

moni  


# %%
table2 = moni.reset_index()
table2


# %%
table2=table2.sort_values(['LocationId', 'date_format'])
table2


# %%
table2["Last_MonthFL_Base_100"] = table2["FL_Base_100"].shift(1)
table2.dtypes


# %%
table2 = table2.fillna(0)


# %%
table2.head(10)


# %%
table2 = table2.astype({"FL_Base_100": int, "Last_MonthFL_Base_100": int, "LocationId": object})


# %%
table2.head(10)


# %%
table2.info()


# %%
table2["LocationIdChanged"] = table2["LocationId"].shift(1, fill_value=table2["LocationId"].head(1)) != table2["LocationId"]
table2.dtypes


# %%
table2["LocationIdChanged"] = table2["LocationIdChanged"].astype(object)
table2.dtypes


# %%
table2.head()


# %%
for OutletCodeChanged in table2:
      s = table2['LocationIdChanged'].unique()
      for i in s:
          if i == 'True':
              Flag_CC = 'change'
              
          else: 
              Flag_CC = table2["FL_Base_100"] - table2["Last_MonthFL_Base_100"]

              table2['Flag_CC']=Flag_CC


# %%
table2.info()


# %%
table2.head()


# %%
table_final = table2[['UserId','LocationId','SessionUid', 'is_reported', 'date_format', 'month', 'day', 'LocationIdChanged', 'FL_Base_100', 'Flag_CC']]
table_final


# %%
table_final = table_final.fillna(0)


# %%
print("flags-as - Borrando los datos de la tabla")
query_clear_EnfTot_Piso_Ventas_as_as = """ 
    
DELETE FROM reportes.EnfTot_Piso_Ventas_as

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
conn.execute(query_clear_EnfTot_Piso_Ventas_as_as)
conn.close()
#get_query(query_clear_Exh_as_cc_comp)


print("flags-as - Llenando la tabla")


# %%
engine = get_engine()
table_final.to_sql('EnfTot_Piso_Ventas_as', con=engine, schema='reportes', if_exists='append', index=False)


