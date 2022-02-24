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

