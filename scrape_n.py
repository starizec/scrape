import time
import requests
from datetime import datetime
from fake_useragent import UserAgent
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from urllib.parse import urlsplit

import proxies
from variables import *
from functions import *
from scrape_functions import *

scrape_time_start = time.time()

def startScrape(country_id):
    user_agent = UserAgent()

    if environment == "production":
        proxies.getProxies()

    locations = getLocations(country_id)
    scrape_data = getScrapeData(country_id)

    tender_links = []

    scrape_locations_no = 0
    scrape_all_links = 0
    scrape_new_links = 0
    scrape_404_count = 0
    scrape_5xx_count = 0

    scrape_started_at = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

    #start location scrape
    for location in locations:
        scrape_location_time_start = time.time()

        #start session
        session = requests.Session()
        
        #retry lib
        retries = 1
        backoff_factor = 0.3
        retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor)

        #request lib
        max_redirects = 5 #max redirects for request
        max_retries = 2 #max retries for request
        timeouts = (6, 10) #connection timeout, read timeout
        adapter = HTTPAdapter(max_retries=max_retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.max_redirects = max_redirects

        #request location
        try:
            headers = {'user-agent': user_agent.random, 'Referer': 'http://google.hr', 'Connection': 'keep-alive'}

            if "skole.hr" in location[1] and environment == "production":
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

        new_location_links = 0

        for a in scrape.find_all("a", href=True):
            process_location_data_time_start = time.time()

            if a['href'] not in scrape_data:
                #messure process location data time
                process_location_data_time = time.time() - process_location_data_time_start

                if('http' in a['href']):
                    tender_link = a['href'][:3000]
                else:
                    if a['href'][0] == "/":
                        tender_link = "{0.scheme}://{0.netloc}".format(urlsplit(location[1]))+a['href'][:3000]
                    else:
                        tender_link = "{0.scheme}://{0.netloc}".format(urlsplit(location[1]))+"/"+a['href'][:3000]

                if tender_link not in tender_links:
                    storeScrapeData(location[0], country_id, tender_link, a.text[:2048], process_location_data_time)

                    tender_links.append(tender_link)

                    new_location_links += 1
                    scrape_new_links += 1
            
            scrape_all_links += 1

        scrape_locations_no += 1
        
        total_location_scrape_time = time.time() - scrape_location_time_start

        storeScrapeLocationData(location[0], country_id, status_code, total_location_scrape_time, len(scrape.find_all("a", href=True)), new_location_links)

    total_scrape_time = time.time() - scrape_time_start

    storeScrapeLogs(country_id, total_scrape_time, scrape_locations_no, scrape_all_links, scrape_new_links, scrape_404_count, scrape_5xx_count, scrape_started_at)