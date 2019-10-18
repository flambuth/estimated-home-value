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
    p.id,p.bathroomcnt as bathrooms,
    p.bedroomcnt as bedrooms, 
    p.calculatedfinishedsquarefeet as sq_ft, 
    p.taxvaluedollarcnt,
    p.lotsizesquarefeet
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


#####NEGLIGABLE#####
#fullbathcnt
# data = wrangle_zillow()
# data['fireplacecnt'] = data['fireplacecnt'].fillna(0)
# data['poolcnt'] = data['poolcnt'].fillna(0)
# data['regionidcounty'] = data['regionidcounty'].apply(lambda x: 1 if x == 3101 else 0)
# data['garagecarcnt'] = data['garagecarcnt'].fillna(0)
# #data['heatingorsystemtypeid'] = data['heatingorsystemtypeid'].fillna(0)
# print(len(data))
# data = data.dropna()
# train, test = train_test_split(data, random_state = 123)