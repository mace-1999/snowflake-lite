'''
Class to easily connect to snowflake using python and query the data.
'''

import snowflake.connector
import getpass
import pandas as pd


class Snowflake_Connection:
    def __init__(self, username : str,  sso : bool = False ):
        self.password = None
        self.username = username
        self.conn = None
        self.sso = sso
        if not sso:
            self.password = getpass.getpass('Please enter the password for your snowflake account: ')


    def print_account(self):
        print(self.username , self.password)

    def connect_to_account(self, ACCOUNT, WAREHOUSE, DATABASE, SCHEMA):
        '''
        Default connection to snowflake using standard
        username and password connection.
        Account is the account indentifier in snowflake.
        return: cursor
        '''
        if self.sso:
            conn = snowflake.connector.connect(
            user=self.username,
            account=ACCOUNT,
            database=DATABASE,
            schema=SCHEMA,
            warehouse=WAREHOUSE,
            authenticator='externalbrowser'
            )

        else:
            conn = snowflake.connector.connect(
            user=self.username,
            password=self.password,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA
            )
        # ensure the conn is in the class object
        self.conn = conn
        print('Connection Successful.')
        return True


    def make_query_and_return_results(self, query):
        '''
        Make queries to the snowflake db and return the results.
        '''
        cur = self.conn.cursor()
        print(f'Executing query...\n{query}')
        cur.execute(query)
        print('Query Complete. Putting into pandas df.')
        query_id = cur.sfqid
        cur.get_results_from_sfqid(query_id)
        results = cur.fetchall()
        print(f'{results[0]}')

        cur.close()
        return results

    def close_connection(self):
        '''
        Close the account connection to Snowflake.
        '''
        self.conn.close()
        print('Connection Closed.')
        return True

    def make_query_and_return_pandas_df(self, query):
        '''
        Query the snowflake db and return
        a pandas df of the results.
        '''
        cur = self.conn.cursor()
        cur.execute(query)
        print('Query Complete.')
        dat = cur.fetchall()
        df = pd.DataFrame(dat, columns=[col[0] for col in cur.description])
        cur.close()
        return df
    

    def make_query_and_return_pandas_df_batched(self, query):
        '''
        Query the snowflake db and return
        a pandas df of the results.
        '''
        cur = self.conn.cursor()
        cur.execute(query)
        print('Query Complete.')
        df = pd.DataFrame()
        dat = cur.fetchmany(200000)
        df = pd.DataFrame(dat, columns=[col[0] for col in cur.description])
        num = len(dat)
        while len(dat) > 0:
            dat = cur.fetchmany(200000)
            df_tmp = pd.DataFrame(dat, columns=[col[0] for col in cur.description])
            df = pd.concat([df, df_tmp])
            num += 500000
            print(f'Collecting {num}')

        cur.close()
        return df
    
