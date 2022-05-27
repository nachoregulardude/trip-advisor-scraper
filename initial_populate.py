#!/usr/bin/env python3
import sqlite3
import json
import logging

from requests import Session
from bs4 import BeautifulSoup


logging.basicConfig(filename='initial_populate.log', level=logging.DEBUG,\
        format='%(asctime)s - %(message)s', datefmt='%d/%b/%Y %H:%M:%S')

"""
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
"""


def get_soup(url):
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'}
    session = Session()
    url_resp = session.get(url, headers=headers)
    logging.info(url_resp.status_code)
    return  BeautifulSoup(url_resp.text, 'lxml')


def trip_by(soup_of_page):
    trip_by = soup_of_page.find('a', {'class': 'bfQwA _G B- _S _T c G_ P0 ddFHE cnvzr bTBvn'})
    trip_by = trip_by.text.replace('By', '').strip()
    

def cost_per_adult(soup_of_page):
    cost_per_adult = soup_of_page.find('div', {'class': 'WlYyy cPsXC brhTq dTqpp'})
    cost_per_adult = cost_per_adult.text
    
    
def star_rating(soup_of_page):
    star_rating_and_total_reviews = soup_of_page.find('div', {'class':'RTVWf o W f u w eeCyE'})
    return star_rating_and_total_reviews['aria-label'].split(' ')[0]\
            if star_rating_and_total_reviews else 0


def total_reviews_given(soup_of_page):
    star_rating_and_total_reviews = soup_of_page.find('div', {'class':'RTVWf o W f u w eeCyE'})
    return  star_rating_and_total_reviews['aria-label'].split(' ')[4]\
            if star_rating_and_total_reviews else 0

        
def duration(soup_of_page):
    return soup_of_page.find('div', {'class': 'fxJux'}).text.split(':')[1]


def available_languages(soup_of_page):
    divs_under_content = soup_of_page.find('div', {'data-automation': 'WebPresentation_PoiAboutWeb'})
    divs_available_languages = divs_under_content.find_all('div', {'class': 'fbrwK'})
    return [f'{x.text} ' for x in divs_available_languages if 'available languages' in x.text.lower()][0]\
            .replace('Available languages', '').strip()

    
def inclusions_list(soup_of_page):
    div_inclusions_exclusions = soup_of_page.find('div', {'class': 'euJLv'})
    inclusions_ul_elements = div_inclusions_exclusions.find('ul', {'class': 'dGZhF'})
    return [li.text for li in inclusions_ul_elements]
    

def exclusion_list(soup_of_page):
    div_inclusions_exclusions = soup_of_page.find('div', {'class': 'euJLv'})
    exclusions_ul_elements = div_inclusions_exclusions.find_all('ul', {'class': 'dGZhF'})
    if (len(exclusions_ul_elements)) > 1:
        exclusions_ul_elements = exclusions_ul_elements[1]
        return [li.text for li in exclusions_ul_elements]
    return []
    
    
def itineary_list(soup_of_page):
    li_of_itineary_list = soup_of_page.select('div[data-automation*=itineraryItem_]')
    itineary_list = []
    for ele in li_of_itineary_list:
        span = ele.find('span', {'class':'bkpgm'})
        span.decompose() if span else span 
        itineary_list.append(ele.text.strip())
    return itineary_list

    
def scrape_data(soup_of_page):
    return {
            'star_rating': star_rating(soup_of_page), 
            'total_reviews_given': total_reviews_given(soup_of_page), 
            'trip_by': trip_by(soup_of_page), 
            'cost_per_adult': cost_per_adult(soup_of_page), 
            'duration': duration(soup_of_page), 
            'available_languages': available_languages(soup_of_page), 
            'inclusions_list': inclusions_list(soup_of_page), 
            'exclusion_list': exclusion_list(soup_of_page), 
            'itineary_list': itineary_list(soup_of_page)
            }


def main():
    city_name = 'bangalore'
    db_name = f'ingestion_trip_advisor_{city_name}_things_to_do.db'
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    try:
        cur.execute("""
                SELECT hsh, trip_link FROM one_day_things_to_do_trip WHERE status='PENDING'
                """)
        rows = cur.fetchall()
    except Exception as e:
        print(e)
    results = {}
    
    for values in rows:
        link = values[1]
        hsh = values[0]
        print(f'Scraping {link}...')
        soup_of_page = get_soup(link)
        data = scrape_data(soup_of_page)
        try:
            result = {hsh: data}
            results.update(result)
            cur.execute("""
                    UPDATE one_day_things_to_do_trip SET status = 'COMPLETED' WHERE hsh = ?
                    """, (hsh,))
        except Exception as e:
            print(e)
            cur.execute("""
                    UPDATE one_day_things_to_do_trip SET status = ? WHERE hsh = ?
                    """, (e, hsh))
        
    json_name = f'ingestion_ta_{city_name}_1_day_things_to_do_meta_data.json'
    with open(json_name, 'a') as a:
        json.dump(results, a)
    conn.commit()
    print(f"The {db_name} database has been updated.\n\
            The {json_name} file has been updated.\n\
            {len(results)} links have been scraped.")


if __name__=="__main__":
    main()
    
    

