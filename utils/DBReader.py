import yaml
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import datetime
from math import ceil
from tqdm import tqdm

config_path = 'config.yml'

with open(config_path) as fp:
    config = yaml.load(fp, Loader=yaml.FullLoader)

def summarize_column(db_connection, schemas, table, column):
    '''
    Get summary statistics for column. 
    
    Calculate the frequency of each options and calculate its percentage. 
    
    Note: for multiple choice answers, the percentage is calculated as the following, which means the sum
    will of all option percentages will >100%
    
    percentage = frequency of each options / total *entities* answered
    
    
    :param:
    
    db_connection: str (required):
        database connection, choose which database to connect
    
    schemas: str (required):
        the schema name under which the given table / view is stored
    
    table: str (required):
        the name of the table / view where the variable is stored
    
    column: str (required):
        the column name to be summarized
    
    example:
    read_summary_table(db_connection = 'conn', 
                       schemas ='reg', 
                        table = 'regulatory_survey', 
                        column = 'd1_suptech_initiative')
    
        
    
    
    '''
    with psycopg2.connect(**config[db_connection]) as connection:
        #create a cursor
        c = connection.cursor()
        
        #write the query
        postgreSQL_select_Query = "SELECT * FROM split_options('{}','{}','{}');".format(schemas, table, column)
        
        c.execute(postgreSQL_select_Query)
        
        data = c.fetchall()
        # Extract the column names
        col_names = []
        for elt in c.description:
            col_names.append(elt[0])  
    output = pd.DataFrame(data, columns = col_names)
    return output



def update_table_multiple_conditions(db_connection, update_table_name, update_variable_list, table_key, dataframe, df_col_list, df_keys, schema_name=None):
    '''
    update table using values in dataframe by key column
    :param:

    db_connection: str (required)
        database connection, choose which database to connect
    
    update_table_name: str
        table name inside database
    
    update_variable_list: list
        a list of variables in database table
    
    dataframe: pandas.DataFrame
        dataframe to update the data
    
    df_col_list: list
        a list of columns in dataframe to update into table
        Note: the column order should be the same as variable order in update_variable_list
    
    df_keys: list
        a list of columns in the dataframe to be used as keys to identify the rows to update.
    
    schema_name: str
        if a table is placed under a given schema or not
    
    example:
    update_table('conn3', 'countries', ['income_grp'], 'country_id', 
                 update_countries, ['Income classifications (World Bank (2021))'], 
                 ['country_id', 'name'], schema_name=None)
    '''
    sql = []
    if schema_name:
        update_sql_1 = 'UPDATE "{}".{} SET '.format(schema_name, update_table_name)
    else:
        update_sql_1 = 'UPDATE {} SET '.format(update_table_name)
        
    #take care of '
    for col in df_col_list:
        if dataframe[col].dtype == object:
            dataframe[col] = dataframe[col].str.replace("'", "''")
    
    for _, row in dataframe.iterrows():
        #each row
        
        row_value = []
        for col in df_col_list:
            if isinstance(row[col], str):
                row_value.append("'" + row[col] + "'")
            elif pd.isnull(row[col]):
                row_value.append('NULL')
            else:
                row_value.append(str(row[col]))
                
        value_change_sql = []
        for index, value in enumerate(update_variable_list):
            value_change_sql.append('{} = {},'.format(value, row_value[index]))
        
        update_sql_2 = ''.join(value_change_sql)
        update_sql_2 = update_sql_2[0:-1]
        
        condition_sql = 'WHERE '
        for key in df_keys:
            if isinstance(row[key], str):
                condition_sql += '{} = \'{}\' AND '.format(key, row[key])
            else:
                condition_sql += '{} = {} AND '.format(key, row[key])
        condition_sql = condition_sql[:-5]
        row_sql = update_sql_1 + update_sql_2 + ' ' + condition_sql + ';'
        sql.append(row_sql)
        
    total_sql = '\n'.join(sql)
    
    with psycopg2.connect(**config[db_connection]) as connection:
        #create a cursor
        c = connection.cursor()
        c.execute(total_sql)
    
    return total_sql

def upsert_dataframe(db_connection, insert_table_name, insert_variable_list, dataframe, df_col_list, 
                     unique_column, schema_name=None, chunk_size=100000):
    '''
    ...

    chunk_size: int
        Number of rows to insert at a time.
    '''
    total_rows = dataframe.shape[0]
    chunks = ceil(total_rows / chunk_size)
    
    pbar = tqdm(total=total_rows, desc='Inserting rows') # Prepare a progress bar

    for chunk in range(chunks):
        df_chunk = dataframe.iloc[chunk * chunk_size: (chunk + 1) * chunk_size]

        sql = []
        if schema_name:
            variable = ','.join(insert_variable_list)
            insert_sql_1 = 'INSERT INTO "{}".{} ({}) VALUES '.format(schema_name, insert_table_name, variable)
        else:
            variable = ','.join(insert_variable_list)
            insert_sql_1 = 'INSERT INTO {} ({}) VALUES '.format(insert_table_name, variable)

        sql.append(insert_sql_1)

        for col in df_col_list:
            if df_chunk[col].dtype == object:
                df_chunk[col] = df_chunk[col].str.replace("'", "''")

        update_statements = []
        for var in insert_variable_list:
            if var != unique_column:
                update_statements.append(f"{var}=EXCLUDED.{var}")

        update_sql = ", ".join(update_statements)
        conflict_sql = f'ON CONFLICT ({unique_column}) DO UPDATE SET {update_sql}'

        for index, row in df_chunk.iterrows():        
            row_value = []
            for col in df_col_list:
                if isinstance(row[col], str):
                    row_value.append("'" + row[col] + "'")
                elif pd.isnull(row[col]):
                    row_value.append('NULL')
                else:
                    row_value.append(str(row[col]))

            insert_sql_2 = ','.join(row_value)
            row_sql = "({}),".format(insert_sql_2)
            sql.append(row_sql)

            # Update the progress bar
            pbar.update(1)
        
        total_sql = '\n'.join(sql)
        total_sql = total_sql[0:-1] + ' ' + conflict_sql + ';'

        with psycopg2.connect(**config[db_connection]) as connection:
            c = connection.cursor()
            c.execute(total_sql)

    # Close the progress bar
    pbar.close()


def update_table(db_connection, update_table_name,update_variable_list,table_key, dataframe, df_col_list,df_key, schema_name = None):
    '''
    update table using values in dataframe by key column
    :param:

    db_connection: str (required)
        database connection, choose which database to connect
    
    update_table_name: str
        table name inside database
    
    update_variable_list: list
        a list of variables in database table
    
    dataframe: pandas.DataFrame
        dataframe to update the data
    
    df_col_list: list
        a list of columns in dataframe to update into table
        Note: the column order should be the same as variable order in update_variable_list
    
    schema_name: str
        if a table is placed under a given schema or not
    
    example:
    update_table('conn3', 'countries',['income_grp'],'country_id', 
                update_countries, ['Income classifications (World Bank (2021))'],'country_id', 
                schema_name = None)
    '''
    sql = []
    if schema_name:
       
        update_sql_1 = 'UPDATE "{}".{} SET '.format(schema_name,update_table_name)
    
    else:
      
        update_sql_1 = 'UPDATE {} SET '.format(update_table_name)
        
 
    
    #take care of '
    for col in df_col_list:
        if dataframe[col].dtype == object:
            dataframe[col] = dataframe[col].str.replace("'","''")
    
    for index, row in dataframe.iterrows():
        #each row
        
        
        row_value = []
        for col in df_col_list:
            if isinstance(row[col],str):
                row_value.append("'"+row[col]+"'")
            #elif row[col] is None:
            elif pd.isnull(row[col]):
                row_value.append('NULL')
            else:
                row_value.append(str(row[col]))
                
                
       

        value_change_sql = []
        for index, value in enumerate(update_variable_list):
            
            value_change_sql.append('{}= {},'.format(value, row_value[index]))
        
        update_sql_2 = ''.join(value_change_sql)
        update_sql_2 =update_sql_2[0:-1]
            
      
        row_sql = []
        row_sql.append(update_sql_1)
        row_sql.append(update_sql_2)
        condition_sql = 'WHERE {} = {};'.format(table_key,row[df_key])
        row_sql.append(condition_sql)
        sql.append(' '.join(row_sql))
        
    total_sql = '\n'.join(sql)
   
    
    with psycopg2.connect(**config[db_connection]) as connection:
        #create a cursor
        c = connection.cursor()
        c.execute(total_sql)
    
    return total_sql


def insert_dataframe(db_connection, insert_table_name,insert_variable_list,dataframe, df_col_list,schema_name = None):
    '''
    insert dataframe directly to database
    :param:

    db_connection: str (required)
        database connection, choose which database to connect

    insert_table_name: str
        table name inside database
    
    insert_variable_list: list
        a list of variables in database table
    
    dataframe: pandas.DataFrame
        dataframe to insert the data
    
    df_col_list: list
        a list of columns in dataframe to insert into table
        Note: the column order should be the same as variable order in insert_variable_list
    
    schema_name: str
        if a table is placed under a given schema or not
    
    example:
    db_connection = 'conn3'
    schema_name = 'FMO'
    insert_table_name = 'benchmark_fintech_names'
    insert_variable_list = ['fintech_id','fintech_all_names']
    dataframe = data_to_insert
    df_col_list = ['Company ID','plat_name']

    total_sql = insert_dataframe(db_connection,insert_table_name,insert_variable_list,dataframe, df_col_list,schema_name)
    '''
    sql = []
    if schema_name:
        variable = ','.join(insert_variable_list)
        insert_sql_1 = 'INSERT INTO "{}".{} ({}) VALUES '.format(schema_name,insert_table_name,variable)
    
    else:
        variable = ','.join(insert_variable_list)
        insert_sql_1 = 'INSERT INTO {} ({}) VALUES '.format(insert_table_name,variable)
        
    sql.append(insert_sql_1)
    
    #take care of '
    for col in df_col_list:
        if dataframe[col].dtype == object:
            dataframe[col] = dataframe[col].str.replace("'","''")
    
    for index, row in dataframe.iterrows():
        #each row
        row_value = []
        for col in df_col_list:
            if isinstance(row[col],str):
                row_value.append("'"+row[col]+"'")
            #elif row[col] is None:
            elif pd.isnull(row[col]):
                row_value.append('NULL')
            else:
                row_value.append(str(row[col]))
                
                
        insert_sql_2 = ','.join(row_value)
        row_sql = "({}),".format(insert_sql_2)
        sql.append(row_sql)
    total_sql = '\n'.join(sql)
    total_sql = total_sql[0:-1] + ';'
    
    with psycopg2.connect(**config[db_connection]) as connection:
        #create a cursor
        c = connection.cursor()
        c.execute(total_sql)
    
    return total_sql
    

def read_table(db_connection,table,schemas = None, columns= None,all_col = False):
    ##使用 **dictionary 可以把字典解绑，一次性输入，config['atlas']是一个字典
    '''
    param: 
    db_connection: str (required)
        database connection, choose which database to connect
    table: str (required)
        from which table to query data

    column: list (optional)
        query selected columns from table

    all_col: Boolean (optional)
        if selected True, query all data from table
    
    example:
    query selected columns: 
    read_table(db_connection = 'conn4',
               schemas = 'FMO',
               table = 'benchmark_quantitative',
              columns = ['response_id','fintech_id','survey_year'])

    '''
    with psycopg2.connect(**config[db_connection]) as connection:
        #create a cursor
        c = connection.cursor()
    
        #write the query
        # if query all data
        if schemas is not None:
            if all_col == True:
                postgreSQL_select_Query = 'select * from "{}".{};'.format(schemas, table)
            elif columns: # if query selected columns
                selected_col = ','.join(columns)
                postgreSQL_select_Query = 'select {} from "{}".{};'.format(selected_col,schemas,table)
        else:
            if all_col == True:
                postgreSQL_select_Query = 'select * from {};'.format( table)
            elif columns: # if query selected columns
                selected_col = ','.join(columns)
                postgreSQL_select_Query = 'select {} from {};'.format(selected_col,table)
                

    
        #execute the query
        c.execute(postgreSQL_select_Query)
    
        #get the data
        all_data = c.fetchall()
    
        # Extract the column names
        col_names = []
        for elt in c.description:
            col_names.append(elt[0])  
    output = pd.DataFrame(all_data, columns = col_names)
    return output