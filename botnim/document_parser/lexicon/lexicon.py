import requests
from pyquery import PyQuery as pq
import csv
import time

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:136.0) Gecko/20100101 Firefox/136.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Content-Type': 'application/json',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Priority': 'u=0',
    'Cookie': 'WSS_FullScreenMode=false; ASP.NET_SessionId=eqvbp1ldaamr2ifdm1acoryq; rbzid=ibrrtGNUvwcQR0B+ZmAUv5JGm+j+IaI1yldtf+VFdZkE9NdGbcNYjW5GliBjxNc+6WlokT2xDq3opFrotCw/6Ka+8WzsK7g1mJpj4TUV3pQyMyWIguRz6Pc//kqNsPVDbK0XurV8mZk3v50DY66CxaPvQrGsrp3WiA6kDiYzNzRtXQwXAg/xTgfZwDK4S2KOK+sP21Hj1S3vCtEAYOQhMFfnsC/s; waap_id=Sv+smIxDLlwfUf8YD6dk4X2CWTCWc5MgEBSjlz6EQZJ7V5GEMnwXRH7YXvsBhNd5ngO6td26kDnaVng18uVTjk567BcQyIO3BgaPTa7YfbDaeYB4lLXGJCcSsLEZN7z2pn3g+BOezbq++iezBbyLL/jiMNxdp8CV9j5tHEyMN7m9tyyzv/6Rcf/OgM+G3UTZLLgMutB2tZhM/olJxAHVYUUE7SXJ; deviceChannel=Default'
}

BASE = 'https://main.knesset.gov.il'
URL = f'{BASE}/about/lexicon/pages/default.aspx'
LINK_CLASS = 'td.lexColumns a'
CONTENT_CLASS = '.LexiconContent'

def scrape():
    response = requests.get(URL, headers=headers)
    if response.status_code != 200:
        print(response.text)
        raise Exception(f"Failed to load page: {response.status_code}")
    
    doc = pq(response.text)
    links = doc(LINK_CLASS)
    
    for link in links:
        href = pq(link).attr('href')
        print('LINK', href)
        link_text = pq(link).text()
        print('ITEM', link_text)
        if href:
            content_url = BASE + href
            content_response = requests.get(content_url, headers=headers)
            if content_response.status_code == 200:
                text = content_response.text
                content_doc = pq(text)
                content = content_doc(CONTENT_CLASS).text()
                content = content.replace('תוכן דף', '').strip()
                print('CONTENT', content)
                yield {
                    'link_text': link_text,
                    'content_url': content_url,
                    'content': content
                }
                time.sleep(5)  # Respectful scraping delay
            else:
                print(f"Failed to load content from {content_url}: {content_response.status_code}")

def scrape_lexicon(output_path):
    rows = []
    for entry in scrape():
        link_text = entry.get('link_text', '')
        content = entry.get('content', '')
        content_url = entry.get('content_url', '')
        formatted = f"{link_text}: {content}. \n\n[קישור למידע]({content_url})."
        rows.append([formatted])
    with open(output_path, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['מידע'])
        writer.writerows(rows)