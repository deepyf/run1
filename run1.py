import yfinance as yf
import csv
import time
import random
from curl_cffi import requests

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 YaBrowser/24.4.0.0 Safari/537.36'
]

def fetch_ticker_data(session, ticker, max_retries=3):
    retry_delays = [(4, 4.5), (6, 6.5), (8, 8.5)]
    for attempt in range(max_retries + 1):
        try:
            user_agent = random.choice(USER_AGENTS)
            headers = {'User-Agent': user_agent}
            tk = yf.Ticker(ticker, session=session)
            info = tk.info
            data = {
                'currentPrice': info.get('currentPrice', ''),
                'targetMeanPrice': info.get('targetMeanPrice', ''),
                'numberOfAnalystOpinions': info.get('numberOfAnalystOpinions', ''),
                'marketCap': info.get('marketCap', ''),
                'industry': info.get('industry', ''),
                'sector': info.get('sector', '')
            }
            return {k: '' if v is None else v for k, v in data.items()}
        except Exception as e:
            if attempt < max_retries:
                min_delay, max_delay = retry_delays[attempt]
                delay = random.uniform(min_delay, max_delay)
                time.sleep(delay)
            else:
                return {field: '' for field in ['currentPrice', 'targetMeanPrice', 'numberOfAnalystOpinions', 'marketCap', 'industry', 'sector']}
    return {field: '' for field in ['currentPrice', 'targetMeanPrice', 'numberOfAnalystOpinions', 'marketCap', 'industry', 'sector']}

def main():
    with open('ghIn_1', mode='r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        symbols = [row['T'] for row in reader]
    
    session = requests.Session()
    
    with open('ghOut_1', mode='w', newline='', encoding='utf-8') as outfile:
        fieldnames = ['T', 'P', 'M', 'O', 'C', 'I', 'S']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for symbol in symbols:
            data = fetch_ticker_data(session, symbol)
            row = {
                'T': symbol,
                'P': data['currentPrice'],
                'M': data['targetMeanPrice'],
                'O': data['numberOfAnalystOpinions'],
                'C': data['marketCap'],
                'I': data['industry'],
                'S': data['sector']
            }
            writer.writerow(row)
            
            time.sleep(random.uniform(2, 2.5))

if __name__ == "__main__":
    main()
