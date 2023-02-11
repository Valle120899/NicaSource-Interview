#!/usr/bin/env python
# coding: utf-8

# In[18]:


#Importing libraries
import numpy as np
import pandas as pd
import warnings
import psycopg2
import sqlalchemy
import argparse


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
    print(CommonInf('Initial dataset', ds))
    
    #Validation for nan values
    ds = NanValues(ds)
    print(CommonInf('Dataset after null validation',ds))
    
    #Validation for standart date format yyyy/mm/dd
    ds = dateFormat(ds)
    
    #Year of sale
    ds = YearSale(ds)
    
    #Converting model to numeric values
    ds = CategoricalRow(ds)
    
    #Casting year column from double to int
    ds= DoubletoInt(ds)
    
    print('Dataframe to insert:')
    print(ds.head(5))
    
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
    #Write the dataframe in append mode to avoid delete previous data
    df.to_sql(tableName, engine, if_exists='append', index=True)


# In[12]:


def ReadingData(cursor, tableName):
    #Reading 10 rows'
    print('Final insert review')
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
        host=Host
    )
  
    conn.autocommit = True
    cursor = conn.cursor()
    
    #Principal function to create the table if it isn't exist
    CreatingTable(tableName, cursor)
    
    #Principal function to insert the dataframe in the table
    InsertingData(df, tableName, engine)
    
    #Function to read the data from postgresql table
    ReadingData(cursor, tableName)
    
    conn.close()


# In[22]:


#Main function
if __name__ == "__main__":
    
    #Main variables 
    ap = argparse.ArgumentParser()
    ap.add_argument("--User", required= True, help= "User in postgresql Database")
    ap.add_argument("--Pass", required= True, help= "User's Password")
    ap.add_argument("--Host", required= True, help= "Principal host")
    ap.add_argument("--Db", required= True, help= "Database sink")
    ap.add_argument("--Port", required= True, help= "Port")
    ap.add_argument("--Table", required= True, help= "Table name")
    
    args = vars(ap.parse_args())
    
    User = args['User']
    Password=args['Pass']
    Host=args['Host']
    Database=args['Db']
    Port=int(args['Port'])
    TableName=args['Table']

    df = ExtractAndTransform()
    try:
        InsertInPostgresql(df,User, Password, Host, Database, Port ,TableName)
    except Exception as e: 
        print('Error: {}'.format(e))

    

