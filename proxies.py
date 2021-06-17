import requests
import time
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from random import randrange
from bs4 import BeautifulSoup

proxies = []

max_time = 10 #in seconds
testing_url = "https://www.osijek.hr/"
proxy_countries = {'DE', 'ES', 'FR', 'RS', 'HR', 'IT', 'NL', 'CZ', 'US'}
timeouts = (10, 12) #connection timeout, read timeout
max_retries = 2 #max retries for request
max_redirects = 2 #max redirects for request

def getProxies():	
    url = 'https://sslproxies.org/'	
    header = {
		'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9',
        'Referer': 'http://google.hr', 
        'Connection': 'keep-alive'
	}

    response = requests.get(url,headers=header)
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    for item in soup.select('#proxylisttable tbody tr'):
        if item.select('td')[2].get_text() in proxy_countries:
            proxy = {
                "http": "http://" + item.select('td')[0].get_text() + ":" + item.select('td')[1].get_text(),
                "https": "https://" + item.select('td')[0].get_text() + ":" + item.select('td')[1].get_text()
            }

            proxy_test_time_start = time.time()

            try:
                session = requests.Session() #start session
                retry = Retry(connect=2, backoff_factor=0.5)
                adapter = HTTPAdapter(max_retries=max_retries)
                session.mount('http://', adapter)
                session.mount('https://', adapter)
                session.max_redirects = max_redirects

                proxy_test_site = session.get(testing_url, headers=header, proxies=proxy, timeout=timeouts)

            except requests.exceptions.RequestException as e:
                print(e)
                pass

            proxy_test_time = time.time() - proxy_test_time_start

            if proxy_test_time < max_time:
                if proxy_test_site.status_code == 200:
                    proxies.append({'ip': item.select('td')[0].get_text(), 'port': item.select('td')[1].get_text()})
                    print(proxy)


def randomProxy(proxies):
    rnd = randrange(len(proxies))
    return proxies[rnd]['ip'] + ':' + proxies[rnd]['port']