import os
import time
import random
import csv
import pandas as pd
import yfinance as yf
from curl_cffi import requests as curl_requests

def _get_session(user_agent):
    s = curl_requests.Session()
    s.headers.update({
        'User-Agent': user_agent,
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    })
    return s

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 OPR/86.0.0.0',
    'Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 YaBrowser/24.12.120.0'
]

if not os.path.exists('ghIn_1'):
    raise SystemExit('ghIn_1 not found')

df = pd.read_csv('ghIn_1', encoding='utf-8', dtype=str)
if 'T' not in df.columns:
    raise SystemExit('Input must contain header T')

tokens = df['T'].fillna('').astype(str).tolist()
unique = []
seen = set()
for t in tokens:
    if t not in seen:
        seen.add(t)
        unique.append(t)
results = {}
retry_delays = [(4, 4.5), (6, 6.5), (8, 8.5)]
for symbol in unique:
    if symbol == '':
        results[symbol] = {'P': '', 'M': '', 'O': '', 'C': '', 'I': '', 'S': ''}
        continue
    attempts = 0
    success = False
    while attempts <= 3 and not success:
        ua = random.choice(user_agents)
        session = _get_session(ua)
        try:
            tck = yf.Ticker(symbol, session=session)
            info = getattr(tck, 'info', None) or {}
            if not info:
                info = {}
            cp = info.get('currentPrice') if info.get('currentPrice') is not None else info.get('regularMarketPrice')
            tmp = info.get('targetMeanPrice')
            noa = info.get('numberOfAnalystOpinions')
            mc = info.get('marketCap')
            ind = info.get('industry')
            sec = info.get('sector')
            results[symbol] = {
                'P': '' if cp is None else str(cp),
                'M': '' if tmp is None else str(tmp),
                'O': '' if noa is None else str(noa),
                'C': '' if mc is None else str(mc),
                'I': '' if ind is None else str(ind),
                'S': '' if sec is None else str(sec)
            }
            success = True
        except Exception:
            attempts += 1
            if attempts > 3:
                results[symbol] = {'P': '', 'M': '', 'O': '', 'C': '', 'I': '', 'S': ''}
                break
            low, high = retry_delays[attempts - 1]
            time.sleep(random.uniform(low, high))
    time.sleep(random.uniform(2, 2.5))

out_rows = []
for t in tokens:
    r = results.get(t, {'P': '', 'M': '', 'O': '', 'C': '', 'I': '', 'S': ''})
    out_rows.append([t, r['P'], r['M'], r['O'], r['C'], r['I'], r['S']])

with open('ghOut_1', 'w', encoding='utf-8', newline='') as f:
    w = csv.writer(f)
    w.writerow(['T', 'P', 'M', 'O', 'C', 'I', 'S'])
    w.writerows(out_rows)

