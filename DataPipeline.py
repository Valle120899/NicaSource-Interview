#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Importing libraries
import numpy as np
import pandas as pd
import warnings
import psycopg2
import sqlalchemy


# In[2]:


warnings.filterwarnings('ignore')


# In[3]:


def CommonInf(Introduction, ds):
    #Common information
    string = """
    {}
    
    Common information:
    Head:
    {}
    
    Null values:
    {}
    
    Row size:
    {}
        """.format(Introduction, ds.head(5), ds.isnull().sum(), ds.shape[0] )
    return string


# In[4]:


def NanValues(ds):
    #Verify how many null rows there are in the ds doesnt matter the column
    NullValues = ds.isnull().sum().sum()
    
    #Validation about null values
    if(NullValues > 0):
        return ds.dropna()
    else:
        return ds


# In[5]:


def dateFormat(ds):
    ds['DateSales']= pd.to_datetime(ds['DateSales'])
    #Casting the dateSales column with the standart date format yyyy/mm/dd
    ds['DateSales'] = ds['DateSales'].dt.strftime('%Y/%m/%d')
    return ds


# In[6]:


def CategoricalRow(ds):
    #Factorizing the model column in order to have a different number per category
    ds['Model'] = pd.factorize(ds['Model'])[0] 
    return pd.DataFrame(ds)


# In[7]:


def YearSale(ds):
    #Cast to datetime the datesales column
    ds['DateSales']= pd.to_datetime(ds['DateSales'])
    #Extracting only the year in a new column called year sale
    ds['Year Sale'] = ds['DateSales'].dt.strftime('%Y')
    return ds


# In[8]:


def DoubletoInt(ds):
    #Cast only the year column to int type
    ds['year'] = ds['year'].astype(int)
    return ds


# In[9]:


def ExtractAndTransform():
    ds = pd.read_csv('dataset.csv')
    #print(CommonInf('Initial dataset', ds))
    
    #Validation for nan values
    ds = NanValues(ds)
    #print(CommonInf('Dataset after null validation',ds))
    
    #Validation for standart date format yyyy/mm/dd
    ds = dateFormat(ds)
    
    #Year of sale
    ds = YearSale(ds)
    
    #Converting model to numeric values
    ds = CategoricalRow(ds)
    
    #Casting year column from double to int
    ds= DoubletoInt(ds)
    
    #print(ds.head(5))
    
    #return de final dataframe
    return ds
    


# In[10]:


def CreatingTable(tableName, cursor):
    sql = """
        CREATE TABLE IF NOT EXISTS {}(
            ID INT PRIMARY KEY,
            CAR VARCHAR NOT NULL,
            MODEL VARCHAR NOT NULL,
            VIN VARCHAR NOT NULL,
            YEAR INT NOT NULL,
            DATESALES DATE NOT NULL,
            ADDRESS VARCHAR NOT NULL,
            YEARSALE INT NOT NULL
        );
    """.format(tableName)
    cursor.execute(sql)


# In[11]:


def InsertingData(df, tableName, engine):
    # Escribir el dataframe a la base de datos
    df.to_sql(tableName, engine, if_exists='append', index=True)


# In[12]:


def ReadingData(cursor, tableName):
    cursor.execute('select * from {} limit 10;'.format(tableName))
    for i in cursor.fetchall():
        print(i)


# In[13]:


def InsertInPostgresql(df,User, Password, Host, Database, Port, tableName):
    # establish connections
    conn_string = 'postgresql://{}:{}@{}/{}'.format(User, Password, Host, Database)
    engine = sqlalchemy.create_engine(conn_string)
    
    conn = psycopg2.connect(
        database=Database,
        user=User, 
        password=Password, 
        host=Host, 
        port= Port
    )
  
    conn.autocommit = True
    cursor = conn.cursor()
    
    CreatingTable(tableName, cursor)
    
    InsertingData(df, tableName, engine)
    
    ReadingData(cursor, tableName)


# In[ ]:





# In[ ]:





# In[14]:


#Main function
if __name__ == "__main__":
    df = ExtractAndTransform()
    try:
        InsertInPostgresql(df,'postgres', 'root', 'localhost', 'NicaSource', 5434 ,'carsales')
    except Exception as e: 
        print('Error: {}'.format(e))

