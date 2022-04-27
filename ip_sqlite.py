from bs4 import BeautifulSoup
import hashlib
import itertools
import requests
#import sqlite3
import pandas


'''
Assignment 2:
Base link: https://www.tripadvisor.com/
Libs to be used: requests, bs4 or lxml, sqlite . You cannot use any sort of browser automation.
Getting to Scrape crawl link:
    Choose any city to scrape data for eg: Bengaluru
    Then select  Things to do, select interval as 1 day trips. Eg Link
    Task Part 1:  (Replicate teal's tracker scraping)
                    Given all Trips suggested in above page, parse the following:
                    trip_name, trip_link
                    Add status column with default value PENDING
                    Add hsh column which is md5 hash of trip_name & trip_link.
                    Store every trip like this in sqlite db & table.
                    Call sqlite db as: ingestion_trip_advisior_{city_name}_things_to_do.db
                    Table name as: one_day_things_to_do_trip
                    Name this python file as: ip_sqlite.py
    Task Part 2:( Replicate teal's downloader scraping)
                    Read sqlite table created above:
                    Go to individual trip links & parse:
                    star_rating, total_reviews_given, trip_by, cost_per_adult, duration, available_languages, inclusions_list, exclusion_list, itineary_list
                    If parsing is success:
                    Append this row to a json file - This will have all trip data, you can use hsh from sqlite table as key & value as dict with all trip related meta info.
                    Update sqlite status of this particular row to COMPLETED
                    Can call json file as ingestion_ta_{city_name}_1_day_things_to_do_meta_data.json
                    If parsing failed:
                    Update sqlite status to a proper exception string. (Eg: IndexError, ValueError, etc)
    Name this python file as initial_populate.py
    An additional challenge for this is to do this part via multi processing.
'''



def get_soup(url):
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'}
    s = requests.Session()
    r = s.get(url, headers=headers)
    return  BeautifulSoup(r.text, 'lxml')



def scrape_one_day_trips(url, base_url):
    soup = get_soup(url)
    #soup = [data.decompose() for data in soup(['span'])]
    div_links = soup.find_all('div', {'class':'fVbwn cdAAV cagLQ eZTON'})
    div_names = soup.find_all('div', {'class':'bUshh o csemS'})
    result = list()
    for div_name, div_link in zip(div_names, div_links):
        link = div_link.find('a')['href']
        div_name.span.decompose()
        name = div_name.text.strip()
        fields = {
                'trip_name': name,
                'trip_link': f'{base_url}{link}',
                'status': 'PENDING',
                'hsh': hashlib.md5(f'{name}{base_url}{link}'.encode()).hexdigest()
                }
        result.append(fields)
        
    return result



def get_all_links_names(url, base_url):
    results = list()
    print(f'going to first page...: {url}')
    results.append(scrape_one_day_trips(url ,base_url))
    next = f'{base_url}{next_page(url)}'
    while next:
        print(f'going to next page ...: {next}')
        results.append(scrape_one_day_trips(next, base_url))
        next = f'{base_url}{next_page(next)}' if next_page(next) else False
        
    all_results = list(itertools.chain.from_iterable(results))
    return all_results



def next_page(url):
    soup = get_soup(url)
    next_page = soup.find('a', {'class':'dfuux u j z _F ddFHE bVTsJ emPJr', 'data-smoke-attr':'pagination-next-arrow'})
    return next_page['href'] if next_page else False
    


def main():
    url = 'https://www.tripadvisor.com/Attraction_Products-g297628-t11889-zfg11867-Bengaluru_Bangalore_District_Karnataka.html'
    base_url = 'https://www.tripadvisor.com'
    city_name = 'Bangalore'
    filename = f'ingestion_trip_advisior_{city_name}_things_to_do.csv'
    results = get_all_links_names(url, base_url)
    dataframe = pandas.DataFrame(results)
    dataframe.to_csv(filename, index=False)
    print(f'Entire Data: \n{results}')
    print(f'Got {len(results)} results')
    print(f'Results saved to {filename}')

if __name__=='__main__':
    main()























