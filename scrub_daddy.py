import copy
import os
import json
import time
import logging
import base64
from datetime import datetime
from email.message import EmailMessage

import google.auth
import yaml
import requests
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)
logging.basicConfig(filename='kijiji_scrapper.log', level=logging.DEBUG)

start_time = time.time()
OUTPUT_FILENAME = "output.txt"
KIJIJI_SITE_URL="https://www.kijiji.ca"
GOOGLE_MAPS_URL="https://www.google.com/maps/place"

CONFIG = {
        "mode": "info",
        "object_weight": { 
            "type": "None",
            "cost": 0,
            "size": 0,
            "max_cost": 0,
            
        },
        "ideal_star_ranking": 3,
        "number_of_bedrooms": 1,
        "number_of_pages_scrubbed": 1,
}

PROVINCES = {
    "Alberta": {
        "url_part": "c37l9003",
        "search_for": "Alberta",
    },
    "Ontario": {
        "url_part": "c37l9004",
        "search_for": "Ontario",
    }
}


base_url = "https://www.kijiji.ca/b-apartments-condos/ontario"
end_url = "a27949001?pet-friendly=1&sort=dateDesc"

listing_template = {
    "title": "",
    "href": "",
    "price": {
        "cost": 0,
        "utilities_included": "Not stated"
    },
    "size": "Not stated",
    "move_in_date": "Not stated",
    "appliances": [],
    "air_conditioning": "Not stated",
    "parking": "Not stated",
    "outdoor_space_included": "Not stated",
    "location": {
        "location": "",
        "google_maps": ""
    }, 
    "number_of_bedrooms": "",
    "number_of_bathrooms": "",
    "type": "",
    "stars": 0,
    "posted": ""
}

listing_list=[]


LOADED_CONFIG = {}
if os.path.exists("config.yml"):
    stream = open("config.yml", "r")
    LOADED_CONFIG = yaml.safe_load(stream)

def recursive_config_key_pull(dictionary, keys_list, predecessor=""):
    for key, value in dictionary.items():
        if type(value) is dict:
            keys_list.append(f"{predecessor}.{key}" if predecessor else f"{key}")
            recursive_config_key_pull(value, keys_list, predecessor=key)
        else:
            keys_list.append(f"{predecessor}.{key}" if predecessor else f"{key}")
    return keys_list

def recursive_find_value_in_dict(dictionary, key):
    try:
        if "." in key:
            split_key = key.split(".")
            value = recursive_find_value_in_dict(dictionary[split_key[0]], split_key[1])
        else:
            value = dictionary.get(key)
        if type(value) is dict:
            return
        return value      
    except KeyError:
        return
    except AttributeError:
        return

def recursive_set_value_in_dict(dictionary, keys_list, start_key=None):
    for key in keys_list:
        try:
            if "." in key:
                split_key = key.split(".")
                recursive_set_value_in_dict(CONFIG[split_key[0]], [split_key[1]], split_key[0])        
            elif type(dictionary[key]) is dict:
                continue
            else:
                if start_key:
                    loaded_value = recursive_find_value_in_dict(LOADED_CONFIG[start_key], key)
                    if loaded_value:
                        CONFIG[start_key][key] = loaded_value
                else:
                    loaded_value = recursive_find_value_in_dict(LOADED_CONFIG, key)
                    if loaded_value:
                        CONFIG[key] = loaded_value
            continue
        except KeyError:
            return dictionary

def load_config():
    keys_list = []
    recursive_config_key_pull(CONFIG, keys_list)
    recursive_set_value_in_dict(CONFIG, keys_list)

def build_url(province, page_counter):
    url = ""
    parsed_end_url = f"{PROVINCES[province]['url_part']}{end_url}"

    if CONFIG['number_of_bedrooms'] == 3:
        bedroom_string = "2+bedroom+den__3+bedrooms"

    if page_counter<=0:
        return f"{base_url}/{bedroom_string}/{parsed_end_url}"
    else:
        return f"{base_url}/page-{page_counter}/{bedroom_string}/{parsed_end_url}"
    

def process(province):
    page_counter=0
    while page_counter < CONFIG['number_of_pages_scrubbed']:
        response = requests.get(build_url(province, page_counter))
        page_counter=page_counter+1
        if response.status_code == 200:
            logger.debug("Making initial call to Kijiji")
            soup = BeautifulSoup(response.text, 'html.parser')
            if len(soup) == 0:
                logger.error("Initial call  Kijiji failed")
                exit(2)
            logger.debug("Finished first call to Kijiji")
            
            logger.debug("Searching for listings")
            listings = soup.find_all('ul', class_='sc-68931dd3-0 dFkkEs')
            logger.debug("Found listings")

            counter=0
            try:
                for listing in listings:
                    cards = soup.find_all("a", class_='sc-bfab1803-0 brAkNc')
                    for card in cards:
                        if CONFIG['mode'] == "debug" and counter > 10:
                            return
                        listing_obj = copy.deepcopy(listing_template)
                        
                        listing_obj['href'] = f"{KIJIJI_SITE_URL}/{card.get('href')}"
                        listing_obj['title'] = card.get_text()
                        if listing_obj['title'] in [item['title'] for item in listing_list]:
                            continue
                        card_url = f"{listing_obj['href']}"
                        response = requests.get(card_url)

                        if response.status_code == 200:
                            listing_soup = BeautifulSoup(response.text, 'html.parser')
                            listing_soup_text = str(listing_soup)

                            if f"{PROVINCES[province]['search_for']}" not in listing_soup_text:
                                break
                            
                            listing_obj['posted'] = listing_soup.find(class_='datePosted-1776470403').get_text()
                            if listing_soup.find(class_='priceWrapper-3915768379').find("span"):
                                price_found = listing_soup.find(class_='priceWrapper-3915768379').find("span").get_text()
                                i_price_found = int(price_found[1:].replace(",",""))
                                if CONFIG['object_weight']['max_cost'] != 0:
                                    if i_price_found > CONFIG['object_weight']['max_cost']:
                                        break
                                listing_obj['price']['cost'] = price_found

                            listing_obj['price']['utilties'] = listing_soup.find(class_='attributeGroupContainer-1655609067').find("ul").get_text()
                            
                            listing_obj['location']['location'] = listing_soup.find(class_='address-2094065249').get_text()
                            listing_obj['location']['google_maps'] = f"{GOOGLE_MAPS_URL}/{listing_obj['location']['location'].replace(' ', '+')}"
                            
                            house_details = listing_soup.find_all(class_='noLabelValue-774086477')
                            listing_obj['type'] = house_details[0].get_text()
                            listing_obj['number_of_bedrooms'] = house_details[1].get_text().replace("Bedrooms: ", "")
                            listing_obj['number_of_bathrooms'] = house_details[2].get_text().replace("Bathrooms: ", "")

                            for item in listing_soup.find_all(class_='list-2534755251 disablePadding-2519548800'):
                                for listing_property in item.find_all("li"):
                                    if listing_property.find("h4"):
                                        item_property = listing_property.find("h4").get_text()
                                        if item_property == "Utilities Included":
                                            continue
                                        if item_property == "Wi-Fi and More":
                                            continue
                                        if item_property == "Appliances":
                                            for appliance_item in listing_property.find_all(class_='groupItem-1182798569'):
                                                target_item = appliance_item.get_text()
                                                if target_item != "":
                                                    listing_obj['appliances'].append(target_item)
                                        if item_property == "Personal Outdoor Space":
                                            if listing_property.find("ul").find("li"):
                                                listing_obj['outdoor_space_included'] = listing_property.find("ul").find("li").get_text()
                                    elif listing_property.find("dt"):
                                        item_property = listing_property.find("dt").get_text()
                                        if item_property == "Parking Included":
                                            listing_obj['parking'] = listing_property.find("dd").get_text()
                                        if item_property == "Agreement Type":
                                            continue
                                        if item_property == "Move-In Date":
                                            listing_obj['move_in_date'] = listing_property.find("dd").get_text()
                                        if item_property == "Pet Friendly":
                                            continue
                                        if item_property == "Size (sqft)":
                                            listing_obj['size'] = f"{listing_property.find('dd').get_text()} (sqft)"
                                        if item_property == "Furnished":
                                            continue
                                        if item_property == "Air Conditioning":
                                            listing_obj['air_conditioning'] = listing_property.find("dd").get_text()
                                        if item_property == "Smoking Permitted":
                                            continue
                        listing_obj = weigh_item(listing_obj)
                        listing_list.append(listing_obj)
                        counter=counter+1
                        logger.info(f"--- Item processed in: { (time.time() - start_time) / 60} minutes ---")
            except Exception as e:
                logging.warning(e)
            except AttributeError as e:
                logging.warning(e)
        else:
            logger.error("Failed to fetch the webpage")

def weigh_item(obj):
    # If price is <$2000
    # If type is house
    # If the house has a yard
    # If the house has parking
    # If the house is 1500+ sqft
    stars=0
    if "Please Contact" not in obj['price']['cost'] and "Swap/Trade" not in obj['price']['cost']:
        cost = int(obj['price']['cost'].replace("$", "").replace(",", ""))
        if cost <= 2000:
            stars=stars+1
    if obj['type'] == "House":
       stars=stars+1
    if  obj['outdoor_space_included'] == "Yard":
       stars=stars+1
    if int(obj['parking'].replace("+", "")) >= 1:
        stars=stars+1
    if obj['size'] != "Not Available (sqft)" and obj['size'] != "Please Contact (sqft)":
        if int(obj['size'].replace(" (sqft)", "").replace(",", "")) >= 1500:
            stars=stars+1

    obj['stars'] = stars
    return obj

def create_email_from_template(item):
    return f"""Title: {item['title']}
Type: {item['type']}
Price: {item['price']['cost']}
Date Posted: {item['posted']}
Utilities: {item['price']['utilities_included']}
Location: {item['location']['location']}
Google Maps: {item['location']['google_maps']}
Stars: {item['stars']}

Details:
    Bedrooms: {item['number_of_bedrooms']}
    Bathrooms: {item['number_of_bathrooms']}
    Size: {item['size']}
    Appliances: {item['appliances']}
    Air Conditioning: {item['air_conditioning']}
    Parking: {item['parking']}
    Outdoor Space: {item['outdoor_space_included']}
    Move In Date: {item['move_in_date']}

Link: {item['href']}

--------------------------------------------------

"""

def print_listings_to_file(province):
    high_stars_listings_list = [item for item in listing_list if item['stars'] >= 4]
    output_filename = f"{province}-{OUTPUT_FILENAME}"

    open(output_filename, 'w').close()
    with open(output_filename, "a") as fh:
        fh.write("##### HIGH STAR LISTINGS #####\n")
        for item in high_stars_listings_list:
            fh.write(create_email_from_template(item))

        fh.write("##### ALL LISTINGS #####\n")
        for item in listing_list:
            fh.write(create_email_from_template(item))

def email_listings(province):
    output_filename = f"{province}-{OUTPUT_FILENAME}"
    creds, _ = google.auth.default()

    try:
        service = build("gmail", "v1", credentials=creds)
        message = EmailMessage()
        message.set_content(open(output_filename, 'r').read())
        message["To"] = "philh@bitovi.com"
        message["From"] = "phillyp.henning@gmail.com"
        message["Subject"] = f"New Listings - {province}"

        # encoded message
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        create_message = {"raw": encoded_message}
        # pylint: disable=E1101
        send_message = (
            service.users()
            .messages()
            .send(userId="me", body=create_message)
            .execute()
        )


        return
        # sg = SendGridAPIClient(api_key=os.environ.get("SENDGRID_API_TOKEN"))
        # response = sg.client.mail.send.post(request_body=mail.get())


    except Exception as e:
        logger.error(e)
        exit(2)
    
def clean_listings():
    listing_list = []

if __name__ == "__main__":
    logger.info("scrapper started")
    try:
        load_config()
        for province in PROVINCES:
            process(province)
            print_listings_to_file(province)
            clean_listings()
            # email_listings(province)
    except KeyboardInterrupt:
        exit(0)
    logger.info("scrapper finished")