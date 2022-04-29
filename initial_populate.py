import sqlite3
import logging


from ip_sqlite import get_soup
import requests
from bs4 import BeautifulSoup

logging.basicConfig(filename='ip_sqlite.log', level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%d/%b/%Y %H:%M:%S')


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


def get_soup(url):
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'}
    session = requests.Session()
    url_resp = session.get(url, headers=headers)
    logging.info(url_resp.status_code)
    return  BeautifulSoup(url_resp.text, 'lxml')


def get_data(soup_of_page):
    try:
        trip_by = soup_of_page.find('a', {'class': 'bfQwA _G B- _S _T c G_ P0 ddFHE cnvzr bTBvn'})
        trip_by = trip_by.text.replace('By', '').strip()
        
        cost_per_adult = soup_of_page.find('div', {'class': 'WlYyy cPsXC brhTq dTqpp'})
        cost_per_adult = cost_per_adult.text
        
        star_rating_and_total_reviews = soup_of_page.find('div', {'class':'RTVWf o W f u w eeCyE'})
        star_rating_and_total_reviews = star_rating_and_total_reviews['aria-label'].split(' ')
        star_rating = float(star_rating_and_total_reviews[0])
        total_reviews_given = int(star_rating_and_total_reviews[4])
        
        duration = soup_of_page.find('div', {'class': 'fxJux'}).text.split(':')[1]
        
        divs_under_content = soup_of_page.find('div', {'data-automation': 'WebPresentation_PoiAboutWeb'})
        divs_available_languages = divs_under_content.find_all('div', {'class': 'fbrwK'})
        available_languages = [f'{x.text} ' for x in divs_available_languages if 'available languages' in x.text.lower()][0].replace('Available languages', '').strip()
        
        div_inclusions_exclusions = soup_of_page.find('div', {'class': 'euJLv'})
        inclusions_ul_elements = div_inclusions_exclusions.find('ul', {'class': 'dGZhF'})
        inclusions_list = list()
        inclusions_list = [li.text for li in inclusions_ul_elements]
        
        exclusions_ul_elements = div_inclusions_exclusions.find_all('ul', {'class': 'dGZhF'})[1]
        exclusion_list = list()
        exclusion_list = [li.text for li in exclusions_ul_elements]
        
        li_of_itineary_list = soup_of_page.select('div[data-automation*=itineraryItem_]')
        itineary_list = list()
        for ele in li_of_itineary_list:
            span = ele.find('span', {'class':'bkpgm'})
            span.decompose()
            itineary_list.append(ele.text.strip())
        
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

url = 'https://www.tripadvisor.com/AttractionProductReview-g297628-d11484011-Full_Day_Nandi_Hills_Countryside_Tour_by_Bike-Bengaluru_Bangalore_District_Karnata.html'
city_name = 'bangalore'
filename = f'ingestion_trip_advisior_{city_name}_things_to_do.db'
conn = sqlite3.connect(filename)
cur = conn.cursor()
cur.execute("""
        SELECT trip_link FROM one_day_things_to_do_trip WHERE status = PENDING
        """)
rows = cur.fetchall()
results = list()
rows = [link for tuple in rows for link in tuple]
print(rows)
quit()
for link in rows[0:3]:
    print(link)
    soup_of_page = get_soup(link)
    data = get_data(soup_of_page)
    if (isinstance(data, dict)):
        results.append(data)
        cur.execute("""
                UPDATE one_day_things_to_do_trip SET status = COMPLETED
                """)
    else:
        cur.execute("""
                UPDATE one_day_things_to_do_trip SET status = ?
                """, (data))
        
print(results)


    
    

