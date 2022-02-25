import numpy as np 
import pandas as pd
import redshift_connector 

# Connection credentials
HOST_NAME = "data-eng-test-cluster.ctfgtxaoukqr.eu-west-1.redshift.amazonaws.com"
PORT = 5439
USER = "chohan"
PASSWORD = "chohan_P@ssw0rd_Q7cm85"
SCHEMA = "chohan"
DB_NAME = "dev"

def redshift_create_table():
    connection = redshift_connector.connect(
        host= HOST_NAME,
        user = USER,
        password = PASSWORD,
        database = DB_NAME
    )

    cursor = connection.cursor()

    cursor.execute("create table event(ID varchar, CREATED_AT varchar, DEVICE_ID varchar, IP_ADDRESS varchar, USER_ID varchar, UUID varchar, EVENT_TYPE varchar, EVENT_PROPERTIES varchar, PLATFORM varchar, DEVICE_TYPE varchar)")
    cursor.executemany("insert into book (bookname, author) values (%s, %s)",
                        [
                            ('One Hundred Years of Solitude', 'Gabriel García Márquez'),
                            ('A Brief History of Time', 'Stephen Hawking')
                        ]
                      )
    
    cursor.execute("select * from book")
    
    results = cursor.fetchall()

    
    print(results)

redshift_create_table()
