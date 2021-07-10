import requests
import time
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from random import randrange
from bs4 import BeautifulSoup
from datetime import datetime

from mysql_connection import conn

proxies = []

max_time = 10 #in seconds
proxy_countries = {'DE', 'ES', 'FR', 'RS', 'HR', 'IT', 'NL', 'CZ', 'US'}
timeouts = (10, 12) #connection timeout, read timeout
max_retries = 2 #max retries for request
max_redirects = 2 #max redirects for request

needed_proxy_no = 20

header = {
		'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9',
        'Referer': 'http://google.hr', 
        'Connection': 'keep-alive'
	}

def getProxies():
    testDbProxies()
    db_proxies = getDbProxies()

    if len(db_proxies) < needed_proxy_no:
        getNewProxies()

    all_proxies = getDbProxies()

    for proxy in all_proxies:
        proxies.append({'ip': proxy[0], 'port': proxy[1]})

def getNewProxies():	
    url = 'https://sslproxies.org/'
    global header

    response = requests.get(url,headers=header)
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    for item in soup.select('#proxylisttable tbody tr'):
        if item.select('td')[2].get_text() in proxy_countries:
            proxy_test_time_start = time.time()

            status_code = testProxy(item.select('td')[0].get_text(), item.select('td')[1].get_text())

            proxy_test_time = time.time() - proxy_test_time_start

            if proxy_test_time < max_time:
                if status_code == 200:
                    storeProxy(item.select('td')[0].get_text(), item.select('td')[1].get_text(), item.select('td')[2].get_text())

def storeProxy(proxy_ip, proxy_port, proxy_region):
    proxy_sql = "INSERT INTO proxies (proxy_ip, proxy_port, proxy_region, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)"
    proxy_val = (proxy_ip, proxy_port, proxy_region, datetime.today().strftime('%Y-%m-%d %H:%M:%S'), datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    proxy_cursor = conn.cursor()
    proxy_cursor.execute(proxy_sql, proxy_val)
    conn.commit()

def deleteProxy(proxy_ip):
    proxy_sql = "DELETE FROM proxies WHERE proxy_ip = '{}'".format(proxy_ip)
    proxy_cursor = conn.cursor()
    proxy_cursor.execute(proxy_sql)
    conn.commit()

def getDbProxies():
    proxies_cursor = conn.cursor()
    proxies_sql = "SELECT proxy_ip, proxy_port, proxy_region FROM proxies ORDER BY id DESC"
    proxies_cursor.execute(proxies_sql)
    all_proxies = proxies_cursor.fetchall()
    conn.commit()

    return all_proxies

def testDbProxies():
    proxies_cursor = conn.cursor()
    proxies_sql = "SELECT proxy_ip, proxy_port, proxy_region FROM proxies ORDER BY id DESC"
    proxies_cursor.execute(proxies_sql)
    all_proxies = proxies_cursor.fetchall()
    conn.commit()
    
    for proxy in all_proxies:
        proxy_test_time_start = time.time()

        status_code = testProxy(proxy[0], proxy[1])

        proxy_test_time = time.time() - proxy_test_time_start

        if proxy_test_time > max_time:
            if status_code == 200:
                deleteProxy(proxy[0])

def randomProxy(proxies):
    rnd = randrange(len(proxies))
    return proxies[rnd]['ip'] + ':' + proxies[rnd]['port']

def testProxy(proxy_ip, proxy_port, test_url = "https://www.osijek.hr/"):
    proxy = formatProxy(proxy_ip, proxy_port)

    global header

    try:
        session = requests.Session() #start session
        retry = Retry(connect=2, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=max_retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.max_redirects = max_redirects

        proxy_test_site = session.get(test_url, headers=header, proxies=proxy, timeout=timeouts)
        
        status_code = proxy_test_site.status_code

    except requests.exceptions.RequestException as e:
        status_code = 500
        pass

    return status_code

def formatProxy(proxy_ip, proxy_port):
    proxy = {
                "http": "http://" + proxy_ip + ":" + proxy_port,
                "https": "https://" + proxy_ip + ":" + proxy_port
            }

    return proxy