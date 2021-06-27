import requests
import time
import mysql.connector
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from datetime import datetime
from fake_useragent import UserAgent

import proxies

print('Scrape started - ' + datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

user_agent = UserAgent()
scrape_time_start = time.time()

#scrape settings
country_id = 1 #croatia
tag = "a" #search a href tag

#request lib
max_redirects = 5 #max redirects for request
max_retries = 2 #max retries for request
timeouts = (6, 10) #connection timeout, read timeout

#retry lib
retries = 1
backoff_factor = 0.3

#create proxies
proxies.getProxies()
"""
#init db
conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="tendersnetwork",
            charset="utf8mb4", 
            use_unicode=True
        )
"""
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
locations_sql = "SELECT id, location_url FROM locations ORDER BY id DESC"
locations_cursor.execute(locations_sql)
all_locations = locations_cursor.fetchall()
conn.commit()

#get all scrape data
location_data_sql = "SELECT scrape_url FROM scrape_data"
location_data_cursor = conn.cursor(buffered=True)
location_data_cursor.execute(location_data_sql)
conn.commit()

all_scrape_data = list(location_data_cursor.fetchall())

scrape_data = [i[0] for i in all_scrape_data]

get_all_locations_time = time.time() - scrape_time_start

scrape_locations_no = 0
scrape_all_links = 0
scrape_new_links = 0
scrape_404_count = 0
scrape_5xx_count = 0
scrape_started_at = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

for location in all_locations:
    scrape_location_time_start = time.time()

    session = requests.Session() #start session
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor)

    adapter = HTTPAdapter(max_retries=max_retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.max_redirects = max_redirects
    
    try:
        headers = {'user-agent': user_agent.random, 'Referer': 'http://google.hr', 'Connection': 'keep-alive'}

        if "skole.hr" in location[1]:
            proxy_ip = proxies.randomProxy(proxies.proxies)
            proxy = {
                "http": "http://" + proxy_ip,
                "https": "https://" + proxy_ip,
            }

            page = session.get(location[1], timeout=timeouts, proxies=proxy, headers=headers) #scrape url
            status_code = page.status_code

        else:
            page = session.get(location[1], timeout=timeouts, headers=headers) #scrape url
            status_code = page.status_code

    except requests.exceptions.TooManyRedirects:
        status_code = 310
        pass

    except requests.exceptions.ConnectionError:
        status_code = 503
        pass

    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout, requests.exceptions.Timeout):
        status_code = 408
        pass

    except (requests.exceptions.URLRequired, requests.exceptions.HTTPError):
        status_code = 404
        pass

    except requests.exceptions.SSLError:
        status_code = 495
        pass

    except requests.exceptions.RequestException as e:
        status_code = 500
        pass

    if("4" in str(status_code)[0]):
        scrape_404_count += 1

    if("5" in str(status_code)[0]):
        scrape_5xx_count += 1
    
    scrape = BeautifulSoup(page.content, 'html.parser') #get page contents
    
    #messure location scrape time
    scrape_location_time_end = time.time() - scrape_location_time_start

    new_location_links = 0

    for a in scrape.find_all(tag, href=True):
        process_location_data_time_start = time.time()

        if a['href'] not in scrape_data:
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
    location_data_val = (location[0], country_id, status_code, total_location_scrape_time, len(scrape.find_all(tag, href=True)), new_location_links, datetime.today().strftime('%Y-%m-%d %H:%M:%S'), datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    location_data_cursor = conn.cursor()
    location_data_cursor.execute(location_data_sql, location_data_val)
    conn.commit()

total_scrape_time = time.time() - scrape_time_start

#save scrape logs
scrape_log_sql = "INSERT INTO scrapes (country_id, scrape_time, scrape_locations_count, scrape_all_links_count, scrape_new_links_count, scrape_404_count, scrape_5xx_count, started_at, ended_at, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
scrape_log_val = (country_id, total_scrape_time, scrape_locations_no, scrape_all_links, scrape_new_links, scrape_404_count, scrape_5xx_count, scrape_started_at, datetime.today().strftime('%Y-%m-%d %H:%M:%S'), datetime.today().strftime('%Y-%m-%d %H:%M:%S'), datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
scrape_log_cursor = conn.cursor()
scrape_log_cursor.execute(scrape_log_sql, scrape_log_val)
conn.commit()

exit()