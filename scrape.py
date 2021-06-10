import requests
import time
import mysql.connector
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from datetime import datetime

scrape_time_start = time.time()

country_id = 1 #croatia
tag = "a" #search a href tag
headers = {'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0 Safari/605.1.15'}

#init db
conn = mysql.connector.connect(
            host="localhost",
            user="davormysqltn",
            password="z$ALE55OoS8V",
            database="tendersnetwork",
            charset="utf8mb4", 
            use_unicode=True
        )

#get all locations
locations_cursor = conn.cursor()
locations_sql = "SELECT id, location_url FROM locations"
locations_cursor.execute(locations_sql)
all_locations = locations_cursor.fetchall()
conn.commit()

get_all_locations_time = time.time() - scrape_time_start
#print(get_all_locations_time)

scrape_locations_no = 0
scrape_all_links = 0
scrape_new_links = 0

for location in all_locations:
    scrape_location_time_start = time.time()

    session = requests.Session() #start session
    retry = Retry(connect=2, backoff_factor=0.5)

    adapter = HTTPAdapter(max_retries=3)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.max_redirects = 5
    
    try:
        page = session.get(location[1], timeout=5, headers=headers) #scrape url
    except (requests.exceptions.ConnectionError, requests.exceptions.TooManyRedirects) as errors:
        page.status_code = 501
        pass
    
    scrape = BeautifulSoup(page.content, 'html.parser') #get page contents
    
    #messure location scrape time
    scrape_location_time_end = time.time() - scrape_location_time_start

    new_location_links = 0

    for a in scrape.find_all(tag, href=True):
        process_location_data_time_start = time.time()

        location_data_sql = "SELECT id FROM scrape_data WHERE scrape_url = %s"
        location_data_val = (a['href'], )
        location_data_cursor = conn.cursor(buffered=True)
        location_data_cursor.execute(location_data_sql, location_data_val)
        conn.commit()

        location_data_cursor.fetchall()

        row_count = location_data_cursor.rowcount

        if row_count < 1:
            #messure process location data time
            process_location_data_time = time.time() - process_location_data_time_start

            scrape_data_sql = "INSERT INTO scrape_data (location_id, country_id, scrape_url, scrape_text, data_scrape_time, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            scrape_data_val = (location[0], country_id, a['href'][:3000], a.text[:2048], process_location_data_time, datetime.today().strftime('%Y-%m-%d %H:%M:%S'), datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
            scrape_data_cursor = conn.cursor()
            scrape_data_cursor.execute(scrape_data_sql, scrape_data_val)
            conn.commit()

            new_location_links += 1
            scrape_new_links += 1
        
        scrape_all_links += 1
        
    scrape_locations_no += 1
        
    total_location_scrape_time = time.time() - scrape_location_time_start
    
    #save scrape location data
    location_data_sql = "INSERT INTO scrape_locations (location_id, country_id, location_http_status_code, location_scrape_time, location_all_links_count, location_new_links_count, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    location_data_val = (location[0], country_id, page.status_code, total_location_scrape_time, len(scrape.find_all(tag, href=True)), new_location_links, datetime.today().strftime('%Y-%m-%d %H:%M:%S'), datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    location_data_cursor = conn.cursor()
    location_data_cursor.execute(location_data_sql, location_data_val)
    conn.commit()

total_scrape_time = time.time() - scrape_time_start

#save scrape logs
scrape_log_sql = "INSERT INTO scrape (country_id, scrape_time, scrape_locations_count, scrape_all_links_count, scrape_new_links_count,  created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s)"
scrape_log_val = (country_id, total_scrape_time, scrape_locations_no, scrape_all_links, scrape_new_links, datetime.today().strftime('%Y-%m-%d %H:%M:%S'), datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
scrape_log_cursor = conn.cursor()
scrape_log_cursor.execute(scrape_log_sql, scrape_log_val)
conn.commit()