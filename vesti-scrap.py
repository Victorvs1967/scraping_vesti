from requests import Session
from bs4 import BeautifulSoup
from urllib.parse import urlsplit, urljoin
from collections import deque
import pandas as pd
import time


def scrap(params):
    # start parser
    url = params['url']
    headers = params['headers']
    session = params['session']

    parts = urlsplit(url)
    base = parts.scheme + '://' + parts.netloc
    path = parts.path

    page_urls = deque([url])
    processed_links = set()

    page = 0
    while len(page_urls):

        # get pages content
        url = page_urls.popleft()
        try:
            time.sleep(1)
            response = session.get(url, headers=headers)
            if not response.ok:
                print(url, response.status_code)
                if response.status_code == 404:
                    break
            else:
                soup = BeautifulSoup(response.content, 'lxml')
                container = soup.find_all('div', {'class': 'row'})
        except Exception as e:
            print('Error:', e)
            continue
        
        # links array
        for line in container:
            sub_lines = line.find_all('a')
            for sub_line in sub_lines:
                if not 'special' in sub_line['href']:
                    link = urljoin(url, sub_line['href'].strip())
                    if link not in processed_links:
                        processed_links.add(link)
                
        page += 1
        page_urls.append(base + path + str(page))
        print('.', end='', flush=True)

    print(len(processed_links))

    # scraping data
    data = {'Title': [],
            'Author': [],
            'Date': [],
            'Time': [],
            'Photo': [],
            'Article': [],
            'URL': list(processed_links)}

    for i in range(0, len(data['URL'])):
        try:
            time.sleep(1)
            url = data['URL'][i]
            sub_res = session.get(url)
            if not sub_res.ok:
                print(sub_res.status_code)
        except Exception as e:
            print('\nError:', e)

        sub_soup = BeautifulSoup(sub_res.content, 'lxml')

        # get title
        main_article = sub_soup.find('div', {'class': 'main article article-text'})
        if main_article:
            main_article = main_article.find('h1')
            data['Title'].append(main_article.text.strip())
        else:
            data['Title'].append('')

        # get text
        article_texts = sub_soup.find('div', {'class': 'article-content'})
        if article_texts:           
            article_text = article_texts.find_all('p', text=True)
            a_text = ''
            for text in article_text:
                a_text += text.text + ' '
            data['Article'].append(a_text.strip())
        else:
            data['Article'].append('')

        # get author and date
        article_info = sub_soup.find('div', {'class': 'post-info'})
        if article_info:
            article_author = article_info.find('a')
            if article_author:
                data['Author'].append(article_author.text.strip())
            else:
                data['Author'].append('')
            article_date = article_info.find('span', {'class': 'date'})
            if article_date:
                datetime = article_date.text.split(',')
                data['Date'].append(datetime[0].strip())
                data['Time'].append(datetime[1].strip())
            else:
                data['Date'].append('')
                data['Time'].append('')
        else:
            data['Author'].append('')
            data['Date'].append('')
            data['Time'].append('')

        # get image link
        article_photo = sub_soup.find('div', {'class': 'photo'})
        if article_photo:
            article_photo = article_photo.find('img')
            if article_photo:
                data['Photo'].append(urljoin(url, article_photo.get('src').strip()))
            else:
                data['Photo'].append('')
        else:
            data['Photo'].append('')
                               
        print('.', end='', flush=True)

    print(flush=False)
    print(len(data['Title']), len(data['Author']), len(data['Article']), len(data['Date']), len(data['Time']), len(data['Photo']), len(data['URL']))

    df = pd.DataFrame(data)
    print(df.info)

    # save scraping data
    cols = ['Title', 'Author', 'Date', 'Time', 'Photo', 'Article', 'URL']

    print('Saving XLSX file...')

    df.to_excel('vesti.xlsx', columns=cols, index=False, engine='xlsxwriter')


def main():

    # start params
    params = {  'url': 'https://vesti-ukr.com/feed/1-vse-novosti/',
                'headers': {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/18.17763',
                            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'},
                'session': Session()}

    scrap(params)


if __name__ == '__main__':
    main()
