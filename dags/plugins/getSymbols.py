import requests
from bs4 import BeautifulSoup
import time

def fetch_symbols_from_yahoo():
    url = "https://finance.yahoo.com/markets/world-indices/"
    response = requests.get(url)
    time.sleep(2)
    soup = BeautifulSoup(response.text, 'html.parser')
    # result = soup.find_all(class_='symbol')
    result = soup.select('table.markets-table tr')
    
    # Find symbols and names
    symbols = []
    for tr in result:
        symbol = tr.select_one('td:first-child span.symbol')
        name = tr.select_one('td.tw-text-left div')
        
        # presuming name is nullable
        if symbol:
            symbols.append({symbol.text.strip(): name.text.strip()})

    return symbols

print(fetch_symbols_from_yahoo())