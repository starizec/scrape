import mysql.connector
from variables import *

conn = mysql.connector.connect(
            host="localhost",
            user=db_user,
            password=db_password,
            database=database,
            charset="utf8mb4", 
            use_unicode=True
        )