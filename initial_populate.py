import sqlite3
import logging
import re


import requests
from bs4 import BeautifulSoup

from ip_sqlite import get_soup

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



            regex match the word after by
            get the other fiels
            learn to read from sqlite table and update values
"""

def get_data(soup_of_page):
    try:
        star_rating_and_total_reviews = soup_of_page.find('div', {'class':'RTVWf o W f u w eeCyE'})
        
        trip_by = soup_of_page.find('a', {'class': 'bfQwA _G B- _S _T c G_ P0 ddFHE cnvzr bTBvn'})
        trip_by = trip_by.text.replace('By', '').strip()
        
        cost_per_adult = soup_of_page.find('div', {'class': 'WlYyy cPsXC brhTq dTqpp'})
        cost_per_adult = cost_per_adult.text
        
        star_rating_and_total_reviews = soup_of_page.find('div', {'class':'RTVWf o W f u w eeCyE'})
        star_rating_and_total_reviews = star_rating_and_total_reviews['aria-label'].split(' ')
        star_rating = star_rating_and_total_reviews[0]
        total_reviews_given = star_rating_and_total_reviews[4]
        
        duration = soup_of_page.find('div', {'class': 'fxJux'}).text.split(':')[1]
        
        divs_under_content = soup_of_page.find('div', {'data-automation': 'WebPresentation_PoiAboutWeb'})
        divs_available_languages = divs_under_content.find_all('div', {'class': 'WlYyy cPsXC cspKb dTqpp'})
        div_available_languages = [x for x in divs_available_languages if 'available languages' in x.text.lower()][0] 
        available_languages = div_available_languages.find()
        
        div_inclusions_exclusions = soup_of_page.find('div', {'class': 'euJLv'})
        inclusions_ul_elements = div_inclusions_exclusions.find('ul', {'class': 'dGZhF'})
        inclusions_list = list()
        inclusions_list = [li.text for li in inclusions_ul_elements]
        
        exclusions_ul_elements = div_inclusions_exclusions.find_all('ul', {'class': 'dGZhF'})[1]
        exclusion_list = list()
        exclusion_list = [li.text for li in exclusions_ul_elements]
        
        divs_under_itineary_list = soup_of_page.find('section', {'id': 'tab-data-WebPresentation_AttractionProductItineraryPlaceholder'})
        divs_itineary_list = divs_under_itineary_list.find_all('li')
        itineary_list = list()
        count = 0
        for ele in divs_itineary_list:
            res = ele.find_all('span', {'class': 'crfhC'})
            itineary_list.append(res)
            count += 1
        itineary_list = [e.text for x in itineary_list[1:-1] for e in x][1:-1]
        
        fields = {
                'star_rating': star_rating, 
                'total_reviews_given': total_reviews_given, 
                'trip_by': trip_by, 
                'cost_per_adult': cost_per_adult, 
                'duration': duration, 
                'available_languages': available_languages, 
                'inclusions_list': inclusions_list, 
                'exclusion_list': exclusion_list, 
                'itineary_list': itineary_list
                }
        return fields
    
    except Exception as e:
        return e

url = 'https://www.tripadvisor.com/AttractionProductReview-g297628-d11486836-Private_Full_Day_Bangalore_City_Tour-Bengaluru_Bangalore_District_Karnataka.html'
# soup_of_page = get_soup(url)
# data = get_data(soup_of_page)
city_name = 'bangalore'
filename = f'ingestion_trip_advisior_{city_name}_things_to_do.db'
conn = sqlite3.connect(filename)
cur = conn.cursor()
cur.execute("""
        SELECT * FROM one_day_things_to_do_trip
        """)
rows = cur.fetchall()
print(rows)



    
    

