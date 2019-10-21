#gathering data from SQL

import pandas as pd
import env 
#This query will get those 4 columns that were suggested for the first iteration.
#Transactions have 2017/05 or 2017/06 in their transactiondate column
# sqft cant be null 


def get_db_url(db):
    return f'mysql+pymysql://{env.user}:{env.password}@{env.host}/{db}'

def get_zillow_bite():
    query = '''
    SELECT 
    p.id,p.bathroomcnt as bathrooms,
    p.bedroomcnt as bedrooms, 
    p.calculatedfinishedsquarefeet as sq_ft, 
    p.taxvaluedollarcnt
    FROM propertylandusetype pl
    JOIN
    properties_2017 p ON p.propertylandusetypeid = pl.propertylandusetypeid
    JOIN
    predictions_2017 pred ON pred.id = p.id
    WHERE 
    p.propertylandusetypeid in (279,261) 
    AND 
    (pred.transactiondate LIKE '%%2017-05%%' OR pred.transactiondate LIKE '%%2017-06%%')
    AND
    p.calculatedfinishedsquarefeet IS NOT NULL
    ;
    '''
    df = pd.read_sql(query, get_db_url('zillow'))
    return df

def get_zillow_data():
    query = '''
    SELECT 
    p.id,
    p.bathroomcnt as bathrooms,
    p.bedroomcnt as bedrooms, 
    p.calculatedfinishedsquarefeet as sq_ft, 
    p.taxvaluedollarcnt,
    p.lotsizesquarefeet
    FROM propertylandusetype pl
    JOIN
    properties_2017 p ON p.propertylandusetypeid = pl.propertylandusetypeid
    JOIN
    predictions_2017 pred ON pred.id = p.id
    WHERE 
    p.propertylandusetypeid in (279,261) 
    AND 
    (pred.transactiondate LIKE '%%2017-05%%' OR pred.transactiondate LIKE '%%2017-06%%')
    AND
    p.calculatedfinishedsquarefeet IS NOT NULL
    ;
    '''
    df = pd.read_sql(query, get_db_url('zillow'))
    return df

def wrangle_zillow():
    query = """
    SELECT  
    poolcnt, 
    fireplacecnt, 
    fullbathcnt, 
    garagecarcnt, 
    regionidcounty, 
    heatingorsystemtypeid, 
    bedroomcnt, 
    calculatedfinishedsquarefeet, 
    taxvaluedollarcnt 
    FROM properties_2017
    JOIN predictions_2017 USING(parcelid) 
    WHERE (predictions_2017.transactiondate BETWEEN '2017-05-01' AND '2017-06-01')
    AND propertylandusetypeid = 261
    ;
    """
    df = pd.read_sql(query, get_db_url('zillow'))
    df['fireplacecnt'] = df['fireplacecnt'].fillna(0)
    df['poolcnt'] = df['poolcnt'].fillna(0)
    df['garagecarcnt'] = df['garagecarcnt'].fillna(0)
    df['heatingorsystemtypeid'] = df['heatingorsystemtypeid'].fillna(0)
    df['regionidcounty'] = df['regionidcounty'].apply(lambda x: 1 if x == 3101 else 0)
    df = df.dropna()
    return df 

def get_tax_distro_6037():
    query_6037 = """
    SELECT taxamount/taxvaluedollarcnt as tax_rate_6037, fips
    FROM properties_2017,
    JOIN predictions_2017 USING (parcelid),
    WHERE (propertylandusetypeid = 261) 
        AND (transactiondate BETWEEN '2017-05-01' and '2017-06-30') 
        AND fips = 6037,
    ;
    """
    df = pd.read_sql(query_6037, get_db_url('zillow'))
    df = df.dropna()
    return df

def get_tax_distro_6059():
    query_6059 = """
    SELECT taxamount/taxvaluedollarcnt as tax_rate_6059, fips
    FROM properties_2017,
    JOIN predictions_2017 using(parcelid),
    WHERE (propertylandusetypeid = 261) 
        AND (transactiondate BETWEEN '2017-05-01' and '2017-06-30') 
        AND fips = 6059,
    ;
    """
    df = pd.read_sql(query_6059, get_db_url('zillow'))
    df = df.dropna()
    return df

def get_tax_distro_6111():
    query_6111 = """
    SELECT taxamount/taxvaluedollarcnt as tax_rate_6111, fips
    FROM properties_2017,
    JOIN predictions_2017 using(parcelid),
    WHERE (propertylandusetypeid = 261) 
        AND (transactiondate BETWEEN '2017-05-01' and '2017-06-30') 
        AND fips = 6111,
    ;
    """
    df = pd.read_sql(query_6111, get_db_url('zillow'))
    df = df.dropna()
    return df