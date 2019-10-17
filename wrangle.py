import pandas as pd
import numpy as np

import env 

def get_db_url(db):
    return f'mysql+pymysql://{env.user}:{env.password}@{env.host}/{db}'

# def get_data_from_mysql():
#     query = '''
#     SELECT *
#     FROM customers
#     JOIN internet_service_types USING (internet_service_type_id)
#     WHERE contract_type_id = 3
#     '''

#     df = pd.read_sql(query, get_db_url('telco_churn'))
#     return df

def clean_data(df):
    df = df[['customer_id', 'total_charges', 'monthly_charges', 'tenure']]
    df.total_charges = df.total_charges.str.strip().replace('', np.nan).astype(float)
    df = df.dropna()
    return df
  
# def wrangle_telco():
#     df = get_data_from_mysql()
#     df = clean_data(df)
#     return df
    
# def wrangle_telco():
#     return clean_data(get_data_from_mysql())


# SELECT prop2017.id, calculatedfinishedsquarefeet, bedroomcnt, bathroomcnt
# FROM properties_2017 AS prop2017
# 	JOIN predictions_2017 AS pred2017
# 	ON prop2017.id = pred2017.id
# WHERE calculatedfinishedsquarefeet != 'Null'
# LIMIT 100


#Selects bathroomcount, bedroomcnt, sq_ft and the target: taxvaluedollarscnt
def get_zillow_data():
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
#Transactions have 2017/05 or 2017/06 in their transactiondate column
# sqft cant be null 

    df = pd.read_sql(query, get_db_url('zillow'))
    return df