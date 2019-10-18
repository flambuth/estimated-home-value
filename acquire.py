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
    p.id,p.bathroomcnt as bathrooms,p.bedroomcnt as bedrooms, p.calculatedfinishedsquarefeet as sq_ft, p.taxvaluedollarcnt
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
    p.id,p.bathroomcnt as bathrooms,
    p.bedroomcnt as bedrooms, 
    p.calculatedfinishedsquarefeet as sq_ft, 
    p.taxvaluedollarcnt,
    p.fips,
    p.lotsizesquarefeet,
    p.propertycountylandusecode,
    p.regionidcity,
    p.regionidzip
    FROM propertylandusetype pl
    JOIN
    properties_2017 p ON p.propertylandusetypeid = pl.propertylandusetypeid
    JOIN
    predictions_2017 p17 ON p17.id = p.id
    WHERE 
    p.propertylandusetypeid in (279,261) 
    AND 
    (p17.transactiondate LIKE '%%2017-05%%' OR p17.transactiondate LIKE '%%2017-06%%')
    AND
    p.calculatedfinishedsquarefeet IS NOT NULL
    ;
    '''
    df = pd.read_sql(query, get_db_url('zillow'))
    return df