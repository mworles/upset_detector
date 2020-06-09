import boto3
import io
import pandas as pd
import table_map
import transfer
from src.features import team

def s3_data(file_name):
    s3_resource = boto3.resource('s3')
    bucket_name = 'worley-upset-detector-public'
    
    s3_object = s3_resource.Object(bucket_name=bucket_name, key=file_name)
    response = s3_object.get()
    data_string = response['Body'].read().decode('utf-8')
    data = pd.read_csv(io.StringIO(data_string))
    return data


def convert_raw_file(file_name, key=table_map.KEY):
    df = s3_data(file_name)
    raw_name = file_name.replace('.csv', '')
    df = df.rename(columns=key[raw_name]['columns'])
    # make any other columns lowercase for consistency
    df.columns = df.columns.str.lower()

    return df

dba = transfer.DBAssist()

"""
for table in table_map.KEY:
    file_name = '{}.csv'.format(table)
    df = convert_raw_file(file_name)
    table_name = table_map.KEY[table]['new_name']

    try:
        dba.create_from_schema(table_name)
    except:
        dba.create_from_data(table_name, df)

    dba.insert_rows(table_name, df)
"""
dba.create_from_schema('tourney_success')
df = team.tourney_performance()
dba.insert_rows('tourney_success', df)
