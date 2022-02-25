import requests
import numpy as np 
import pandas as pd
# import matplotlib.pyplot as plt


def airtable_data_request(base_name, all_values = False, number_of_records=None):
    base_id = "appWzDISwYl2XFcEz"
    
    
    web_event_table = base_name
    
    airtable_api = "keyNPJfjXhznqn92h"
    url = "https://api.airtable.com/v0/"+ base_id + "/" + web_event_table + "?api_key=" + airtable_api
    
    if number_of_records is not None:
        params={"maxRecords": int(number_of_records)}
    else:
        params = ()
    auth_value = "Bearer" + airtable_api
    headers = {"Authorization": auth_value }
    
    # Gets response from airtable
    airtable_response = requests.get(url, params=params)
    airtable_json_output = airtable_response.json()

    airtable_records = airtable_json_output["records"]
    
    # putting all records to pandas dataframe
    airtable_rows = []
    airtable_index = []

    for value in airtable_records:
        # adds all values inside fields as each row
        airtable_rows.append(value["fields"])
        # list of ids
        airtable_index.append(value["id"])
    
    # Dataframe: rows are data and id is the index of the dataframe
    df = pd.DataFrame(airtable_rows, index=airtable_index)

    
    if all_values:
        print("use_offset")

    return df

df_web_airtable = airtable_data_request("Web events")
df_app_airtable = airtable_data_request("App events")

df_web_airtable.to_csv("data/web_event.csv")
df_app_airtable.to_csv("data/app_event.csv")

print(df_app_airtable.columns)


# import numpy as np 
# import pandas as pd
# import redshift_connector 

# # Connection credentials
# HOST_NAME = "data-eng-test-cluster.ctfgtxaoukqr.eu-west-1.redshift.amazonaws.com"
# PORT = 5439
# USER = "chohan"
# PASSWORD = "chohan_P@ssw0rd_Q7cm85"
# SCHEMA = "chohan"
# DB_NAME = "dev"

# def redshift_create_table(df_app_airtable):
#     # from sqlalchemy import create_engine
#     # import pandas as pd

#     # conn = create_engine('postgresql://chohan:password@data-eng-test-cluster.ctfgtxaoukqr.eu-west-1.redshift.amazonaws.com:5439/dev?password=chohan_P@ssw0rd_Q7cm85')

#     connection = redshift_connector.connect(
#         host= HOST_NAME,
#         user = USER,
#         password = PASSWORD,
#         database = DB_NAME
#     )

#     cursor = connection.cursor()
#     # cursor.execute("drop table event")
#     # print("dropped table")
#     # cursor.execute("create table event_two(ID varchar, CREATED_AT varchar, DEVICE_ID varchar, IP_ADDRESS varchar, USER_ID varchar, UUID varchar(1000), EVENT_TYPE varchar, EVENT_PROPERTIES varchar(3000), PLATFORM varchar(1000), DEVICE_TYPE varchar(3000))")
    
#     # # df_app_airtable.to_sql("event", con=conn, if_exists="append")

#     # sql_insert_list = []
#     # insert_query = "insert into event(ID, CREATED_AT, DEVICE_ID, IP_ADDRESS, USER_ID, UUID, EVENT_TYPE, EVENT_PROPERTIES, PLATFORM,DEVICE_TYPE) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#     # for index, rows in df_app_airtable.iterrows():
        
#     #     cursor.execute(insert_query,(rows["ID"],rows["CREATED_AT"],rows["DEVICE_ID"],rows["IP_ADDRESS"],rows["USER_ID"],rows["UUID"],rows["EVENT_TYPE"],rows["EVENT_PROPERTIES"],rows["PLATFORM"], rows["DEVICE_TYPE"]))

#     connection.commit()
#     # print("inserted into db")    
    
#     # To check if table exists.
#     # cursor.execute("SELECT DISTINCT tablename FROM pg_table_def WHERE schemaname = 'chohan'")
#     # results = cursor.fetchall()
#     # print(type(results))
#     # if results is not None:
#     #     for x in results:
#     #         print(type(x))
#     #         if "event_two" in x:
#     #             print("found table")
#     #     print(results)

    
#     # conn.execute("select * from event")
    
#     # results = conn.fetchall()

    
#     # print(results)

# redshift_create_table(df_app_airtable)