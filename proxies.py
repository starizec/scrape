from bs4 import BeautifulSoup
import requests
from random import randrange

proxies = []

def getProxies():	
    url = 'https://sslproxies.org/'	
    header = {
		'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9'
	}

    response = requests.get(url,headers=header)
    
    soup = BeautifulSoup(response.content, 'html.parser')

    for item in soup.select('#proxylisttable tr'):
	    try:
		    proxies.append({'ip': item.select('td')[0].get_text(), 'port': item.select('td')[1].get_text()})
	    except:
		    print('')


def randomProxy(proxies):
    rnd = randrange(len(proxies))
    return proxies[rnd]['ip'] + ':' + proxies[rnd]['port']