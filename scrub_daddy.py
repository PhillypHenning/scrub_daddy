import copy
import requests
import json
import yaml
import time
import logging
import smtplib

from bs4 import BeautifulSoup
from datetime import datetime
from email.message import EmailMessage

logger = logging.getLogger(__name__)
logging.basicConfig(filename='kijiji_scrapper.log', level=logging.DEBUG)

start_time = time.time()
output_filename = "output.txt"
KIJIJI_SITE_URL="https://www.kijiji.ca"
GOOGLE_MAPS_URL="https://www.google.com/maps/place"

CONFIG = {
        "mode": "info",
        "object_weight": { 
            "type": "House",
            "cost": 2000,
            "size": 1500,
            
        },
        "ideal_star_ranking": 4,
        "number_of_bedrooms": 3,
        "number_of_pages_scrubbed": 3
    }

# def recursive_config_key_pull(dictionary, keys_list, predecessor=""):
#     for key, value in dictionary.items():
#         if type(value) is dict:
#             keys_list.append(f"{predecessor}.{key}" if predecessor else f"{key}")
#             recursive_config_key_pull(value, keys_list, predecessor=key)
#         else:
#             keys_list.append(f"{predecessor}.{key}" if predecessor else f"{key}")
#     return keys_list

# def recursive_find_value_in_dict(dictionary, key):
#     # print(f"    key: {key} | dictionary: {dictionary}")
#     try:
#         if "." in key:
#             split_key = key.split(".")
#             recursive_find_value_in_dict(dictionary[split_key[0]], split_key[1])
#         value = dictionary.get(key)
#         print(f"    [0] value: {value} | key: {key}")
#         print(f"    dictionary.get(key): {dictionary.get(key)}")
#         print(f"    dictionary[key]: {dictionary[key]}")
#         if type(value) is dict:
#             print(f"Check? {key}")
#             return
#         print(f"    [1] value: {value}")
#         return value      
#     except KeyError:
#         return
#     except AttributeError:
#         return

# def recursive_set_value_in_dict(dictionary, keys_list):
#     for key in keys_list:
#         try:
#             # print(f"key: {key}")
#             if "." in key:
#                 split_key = key.split(".")
#                 # print(f"split_key[0]: {split_key[0]} | split_key[1]: {split_key[1]}")

#                 if type(dictionary[split_key[0]][split_key[1]]) is dict:
#                     recursive_set_value_in_dict(dictionary[split_key[0]], split_key[1])
#                 else:
#                     new_value = recursive_find_value_in_dict(LOADED_CONFIG, key)
#                     print(f"Setting key - key: {key} | value: {new_value}")
#                     dictionary[split_key[0]][split_key[1]] = new_value
#                     print()
#                     continue
#                 continue
#             else:
#                 if type(dictionary[key]) is dict:
#                     print()
#                     continue
#                 new_value = recursive_find_value_in_dict(LOADED_CONFIG, key)
            
#             print(f"Setting key - key: {key} | value: {new_value}")
#             dictionary[key] = new_value
#             print()
#         except KeyError:
#             return dictionary
#     return dictionary


# with open("config.yml", "r") as stream:
#     LOADED_CONFIG = yaml.safe_load(stream)
#     keys_list = []
#     recursive_config_key_pull(CONFIG, keys_list)

#     print("#~# BASELINE CONFIG #~#")
#     for key in keys_list:
#         recursive_find_value_in_dict(CONFIG, key)
#     print('---')
    
#     print("#~# LOADED CONFIG #~#")
#     for key in keys_list:
#         recursive_find_value_in_dict(LOADED_CONFIG, key)
#     print('---')

#     print("#~# PARSING CONFIG #~#")
#     PARSED_CONFIG=recursive_set_value_in_dict(CONFIG, keys_list)
#     print('---')

#     print(PARSED_CONFIG)

    # print("#~# PARSED CONFIG #~#")
    # recursive_find_value_in_dict(PARSED_CONFIG, key)
    # print('---')

    # print("#~# PROCESSED CONFIG #~#")
    # for key in keys_list:
    #     recursive_find_value_in_dict(CONFIG, key)

# https://www.kijiji.ca/b-apartments-condos/ontario/2+bedroom+den__3+bedrooms/c37l9004a27949001?pet-friendly=1&sort=dateDesc
# 
#
# Locations
#   alberta/<filter>/c37l9003
#   ontario/<filter>/c37l9004
#   britishcolumbia/<filter>/c37l9007
#   
# Filters
#   1 item: 2+bedroom+den
#   2 item: 2+bedroom+den__2+bedrooms
#   3 item: 2+bedroom+den__2+bedrooms__3+bedrooms
#
# Pet Friendly
#   pet-friendly=1

# url = "https://www.kijiji.ca/b-apartments-condos/ontario/2+bedroom+den__3+bedrooms/c37l9004a27949001?pet-friendly=1&sort=dateDesc"

base_url = "https://www.kijiji.ca/b-apartments-condos/ontario"
end_url = "c37l9004a27949001?pet-friendly=1&sort=dateDesc"

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
    "stars": 0
}

# listing_list=[]
listing_list = [{"title": "3 BDR Townhouse for Rent", "href": "https://www.kijiji.ca//v-apartments-condos/kitchener-waterloo/3-bdr-townhouse-for-rent/1647656161", "price": {"cost": "$2,650", "utilities_included": "Not stated", "utilties": "Not Included"}, "size": "1,350 (sqft)", "move_in_date": "December 1, 2023", "appliances": ["Laundry (In Unit)", "Dishwasher", "Fridge / Freezer"], "air_conditioning": "No", "parking": "0", "outdoor_space_included": "Yard", "location": {"location": "21 Holborn Drive, Kitchener, ON", "google_maps": "https://www.google.com/maps/place/21+Holborn+Drive,+Kitchener,+ON"}, "number_of_bedrooms": "3", "number_of_bathrooms": "1", "type": "Townhouse", "stars": 1}, {"title": "STUDENTS - Furnished 3 Bedroom, 3 Bath, Available May 1, 2024", "href": "https://www.kijiji.ca//v-apartments-condos/kingston-on/students-furnished-3-bedroom-3-bath-available-may-1-2024/1683388114", "price": {"cost": "$4,245", "utilities_included": "Not stated", "utilties": "Not Included"}, "size": "936 (sqft)", "move_in_date": "May 1, 2024", "appliances": ["Laundry (In Unit)", "Dishwasher", "Fridge / Freezer"], "air_conditioning": "Yes", "parking": "0", "outdoor_space_included": "", "location": {"location": "Division Street, Kingston, ON", "google_maps": "https://www.google.com/maps/place/Division+Street,+Kingston,+ON"}, "number_of_bedrooms": "3", "number_of_bathrooms": "3", "type": "Apartment", "stars": 0}, {"title": "3 Bedroom house prime Oakville with huge backyard and 2 parking", "href": "https://www.kijiji.ca//v-apartments-condos/oakville-halton-region/3-bedroom-house-prime-oakville-with-huge-backyard-and-2-parking/1689412432", "price": {"cost": "$3,250", "utilities_included": "Not stated", "utilties": "Not Included"}, "size": "1,300 (sqft)", "move_in_date": "May 1, 2024", "appliances": ["Laundry (In Building)", "Dishwasher", "Fridge / Freezer"], "air_conditioning": "Yes", "parking": "2", "outdoor_space_included": "Yard", "location": {"location": "Bridge Road, Oakville, Ontario", "google_maps": "https://www.google.com/maps/place/Bridge+Road,+Oakville,+Ontario"}, "number_of_bedrooms": "3", "number_of_bathrooms": "1", "type": "House", "stars": 3}, {"title": "Spacious Apartment in House (Not Basement)", "href": "https://www.kijiji.ca//v-apartments-condos/markham-york-region/spacious-apartment-in-house-not-basement/1685271679", "price": {"cost": "$2,150", "utilities_included": "Not stated", "utilties": "Not Included"}, "size": "1,500 (sqft)", "move_in_date": "April 1, 2024", "appliances": ["Laundry (In Unit)", "Dishwasher", "Fridge / Freezer"], "air_conditioning": "Yes", "parking": "1", "outdoor_space_included": "Yard", "location": {"location": "420 Williamson Rd, Markham, ON L6E 0M7", "google_maps": "https://www.google.com/maps/place/420+Williamson+Rd,+Markham,+ON+L6E+0M7"}, "number_of_bedrooms": "3", "number_of_bathrooms": "2", "type": "House", "stars": 4}, {"title": "2+1 Bedroom 1 Bath Brand New Apartment for Rent $1,699/ month", "href": "https://www.kijiji.ca//v-apartments-condos/kitchener-waterloo/2-1-bedroom-1-bath-brand-new-apartment-for-rent-1-699-month/1688977731", "price": {"cost": "$1,699", "utilities_included": "Not stated", "utilties": "Not Included"}, "size": "910 (sqft)", "move_in_date": "March 25, 2024", "appliances": ["Laundry (In Unit)", "Fridge / Freezer"], "air_conditioning": "Yes", "parking": "1", "outdoor_space_included": "Not stated", "location": {"location": "Kitchener, ON N2N 2V7", "google_maps": "https://www.google.com/maps/place/Kitchener,+ON+N2N+2V7"}, "number_of_bedrooms": "2 + Den", "number_of_bathrooms": "1", "type": "Basement", "stars": 2}, {"title": "1BR & 2BR Brand New Condo units at King St W & Blue Jays Way !!", "href": "https://www.kijiji.ca//v-apartments-condos/city-of-toronto/1br-2br-brand-new-condo-units-at-king-st-w-blue-jays-way/1679080891", "price": {"cost": "$2,000", "utilities_included": "Not stated", "utilties": "Not Included"}, "size": "1,000 (sqft)", "move_in_date": "March 31, 2024", "appliances": ["Laundry (In Unit)", "Dishwasher", "Fridge / Freezer"], "air_conditioning": "Yes", "parking": "0", "outdoor_space_included": "", "location": {"location": "Mercer St, Toronto, ON, M5V 1H2", "google_maps": "https://www.google.com/maps/place/Mercer+St,+Toronto,+ON,+M5V+1H2"}, "number_of_bedrooms": "2 + Den", "number_of_bathrooms": "2", "type": "Condo", "stars": 1}, {"title": "2 bedroom + den and 3 bedroom suites now leasing!!", "href": "https://www.kijiji.ca//v-apartments-condos/city-of-toronto/2-bedroom-den-and-3-bedroom-suites-now-leasing/1682801806", "price": {"cost": "$3,300", "utilities_included": "Not stated", "utilties": "HydroHeatWater"}, "size": "986 (sqft)", "move_in_date": "Not stated", "appliances": ["Laundry (In Building)", "Dishwasher", "Fridge / Freezer"], "air_conditioning": "No", "parking": "0", "outdoor_space_included": "", "location": {"location": "20 Antrim Crescent, Scarborough, ON, M1P 4N3", "google_maps": "https://www.google.com/maps/place/20+Antrim+Crescent,+Scarborough,+ON,+M1P+4N3"}, "number_of_bedrooms": "2 + Den", "number_of_bathrooms": "2", "type": "Apartment", "stars": 0}, {"title": "Coming Fall 2024 - Brand New 3 Bedroom Apartments", "href": "https://www.kijiji.ca//v-apartments-condos/ottawa/coming-fall-2024-brand-new-3-bedroom-apartments/1686594452", "price": {"cost": "Please Contact", "utilities_included": "Not stated", "utilties": "Not Included"}, "size": "Not Available (sqft)", "move_in_date": "Not stated", "appliances": ["Laundry (In Unit)", "Laundry (In Building)", "Dishwasher", "Fridge / Freezer"], "air_conditioning": "Yes", "parking": "0", "outdoor_space_included": "", "location": {"location": "245-265 Rideau Street, Ottawa, ON, K1N 5Y2", "google_maps": "https://www.google.com/maps/place/245-265+Rideau+Street,+Ottawa,+ON,+K1N+5Y2"}, "number_of_bedrooms": "3", "number_of_bathrooms": "2", "type": "Apartment", "stars": 0}, {"title": "$500 Move-in Bonus | Spacious & Bright 2 Bed + Den in Downtown", "href": "https://www.kijiji.ca//v-apartments-condos/kitchener-waterloo/500-move-in-bonus-spacious-bright-2-bed-den-in-downtown/1674036684", "price": {"cost": "$2,799", "utilities_included": "Not stated", "utilties": "HydroHeatWater"}, "size": "1,065 (sqft)", "move_in_date": "February 12, 2024", "appliances": ["Laundry (In Unit)", "Dishwasher", "Fridge / Freezer"], "air_conditioning": "Yes", "parking": "1", "outdoor_space_included": "", "location": {"location": "120 Benton Street, Kitchener, ON, N2G 0C7", "google_maps": "https://www.google.com/maps/place/120+Benton+Street,+Kitchener,+ON,+N2G+0C7"}, "number_of_bedrooms": "2 + Den", "number_of_bathrooms": "2", "type": "Apartment", "stars": 1}, {"title": "Brand New 2 Bed + Den Apartments Across from Waterloo Park", "href": "https://www.kijiji.ca//v-apartments-condos/kitchener-waterloo/brand-new-2-bed-den-apartments-across-from-waterloo-park/1667049710", "price": {"cost": "$3,020", "utilities_included": "Not stated", "utilties": "HydroHeatWater"}, "size": "1,322 (sqft)", "move_in_date": "Not stated", "appliances": ["Laundry (In Unit)", "Dishwasher", "Fridge / Freezer"], "air_conditioning": "Yes", "parking": "1", "outdoor_space_included": "", "location": {"location": "12 Merchant Ave, Waterloo, ON, N2L 0E6", "google_maps": "https://www.google.com/maps/place/12+Merchant+Ave,+Waterloo,+ON,+N2L+0E6"}, "number_of_bedrooms": "2 + Den", "number_of_bathrooms": "2", "type": "Apartment", "stars": 1}, {"title": "2 Bedroom + 2 Baths w/Den in Uptown Waterloo", "href": "https://www.kijiji.ca//v-apartments-condos/kitchener-waterloo/2-bedroom-2-baths-w-den-in-uptown-waterloo/1684151196", "price": {"cost": "$3,199", "utilities_included": "Not stated", "utilties": "HydroHeatWater"}, "size": "1,404 (sqft)", "move_in_date": "Not stated", "appliances": ["Laundry (In Unit)", "Dishwasher"], "air_conditioning": "Yes", "parking": "2", "outdoor_space_included": "", "location": {"location": "20 Barrel Yards Blvd., Waterloo, ON, N2L 0C3", "google_maps": "https://www.google.com/maps/place/20+Barrel+Yards+Blvd.,+Waterloo,+ON,+N2L+0C3"}, "number_of_bedrooms": "2 + Den", "number_of_bathrooms": "2", "type": "Apartment", "stars": 1}]

def build_url(page=0):
    url = ""

    if CONFIG['number_of_bedrooms'] == 3:
        bedroom_string = "2+bedroom+den__3+bedrooms"

    # assumes this is the first page being called
    if page == 0:
        return f"{base_url}/{bedroom_string}/{end_url}"
    else:
        return f"{base_url}/page-{page}/{bedroom_string}/{end_url}"

def process():
    page_counter = 0
    while page_counter < CONFIG['number_of_pages_scrubbed']:
        page_counter=page_counter+1
        response = requests.get(build_url(page_counter))

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
                            if listing_soup.find(class_='priceWrapper-3915768379').find("span"):
                                listing_obj['price']['cost'] = listing_soup.find(class_='priceWrapper-3915768379').find("span").get_text()
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

def print_listings_to_file():
    high_stars_listings_list = [item for item in listing_list if item['stars'] >= 4]

    open(output_filename, 'w').close()
    with open(output_filename, "a") as fh:
        fh.write("##### HIGH STAR LISTINGS #####\n")
        for item in high_stars_listings_list:
            fh.write(create_email_from_template(item))

        fh.write("##### ALL LISTINGS #####\n")
        for item in listing_list:
            fh.write(create_email_from_template(item))

def email_listings():
    msg = EmailMessage()
    with open(output_filename) as fh: 
        msg.set_content(fh.read())
    
    msg['Subject'] = 'New Listings'
    msg['From'] = "phillyp.henning@automated.solutions.com"
    msg['To'] = "phillyp.henning@gmail.com"
    
    try:
        smtp_server = smtplib.SMTP('localhost')
        smtp_server.send_message(msg)
    except Exception as e:
        logger.error(e)
        exit(2)
    finally:
        smtp_server.quit()

if __name__ == "__main__":
    logger.info("scrapper started")
    process()
    print_listings_to_file()
    # email_listings()
    logger.info("scrapper finished")