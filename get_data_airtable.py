import requests
import numpy as np 
import pandas as pd
# import matplotlib.pyplot as plt


def airtable_data_request(all_values:False, number_of_records=None):
    base_id = "appWzDISwYl2XFcEz"
    app_event_table = "App events"
    web_event_table = "Web events"
    
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
    
    print(airtable_records)
    
    if all_values:
        print("use_offset")

airtable_data_request()
