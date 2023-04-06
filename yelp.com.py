import csv
import os
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup
import logging
import traceback
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

os.environ['SLACK_BOT_TOKEN'] = 'xoxb-2374382758640-4998736138407-dhGD33rHJgYYzvlnp4Cnmwu5'

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s',
                    handlers=[logging.FileHandler('debug.log', 'w', 'utf-8')])
logging.debug('This is a debug message')


def extract_data_and_write_to_csv(driver, city):
    page = driver.page_source
    soup = BeautifulSoup(page, 'html.parser')

    name = extract_element(soup, 'h1.css-1se8maq')
    phone = extract_element(soup, 'div:nth-of-type(2) > .arrange__09f24__LDfbs.border-color--default__09f24__NPAKY'
                                  '.gutter-2__09f24__CCmUo.vertical-align-middle__09f24__zU9sE > '
                                  '.arrange-unit-fill__09f24__CUubG.arrange-unit__09f24__rqHTg.border-color'
                                  '--default__09f24__NPAKY > .css-1p9ibgf')
    adr = extract_element(soup, 'address')

    write_to_csv('yelp_motorcycle_rental-2\\' + city + '.csv', [name, phone, adr])


def extract_element(soup, selector):
    try:
        element = soup.select_one(selector)
        return element.text.strip()
    except:
        return ''


def write_to_csv(file_path, data):
    with open(file_path, 'a', newline="", encoding="UTF-8") as f:
        csvwriter = csv.writer(f)
        csvwriter.writerow(data)


with open('Uscities.csv', 'r') as file:
    reader = csv.reader(file)
    lsit_reader = list(reader)


def scrape_city(city, index):
    options = uc.ChromeOptions()
    options.add_argument("C:\\Users\\scrap\\PycharmProjects\\yelb\\selenium")
    driver = uc.Chrome(options=options)
    url = "https://www.yelp.com/search?find_desc=Motorcycle+Rental&find_loc=" + city
    driver.get(url)
    time.sleep(3)
    time.sleep(4)
    html_source = driver.page_source
    soup = BeautifulSoup(html_source, 'html.parser')
    tr = soup.find_all('div',
                       class_='padding-t3__09f24__TMrIW padding-r3__09f24__eaF7p padding-b3__09f24__S8R2d padding-l3__09f24__IOjKY border-color--default__09f24__NPAKY')

    os.makedirs('yelp_motorcycle_rental', exist_ok=True)

    for t in tr:
        try:
            if "Motorcycle" in t.p.text:
                new_url = 'https://www.yelp.com' + t.a['href']
                time.sleep(2)
                driver.get(new_url)
                time.sleep(2)

                extract_data_and_write_to_csv(driver, city)
                driver.back()
                time.sleep(2)
        except Exception as e:
            print("Error in Motorcycle keyword find", str(e))
            continue

        driver.get(url)
        page_links = driver.page_source
        page_soup = BeautifulSoup(page_links, 'html.parser')
        try:
            all_pages = page_soup.find_all('div', class_='pagination-link-container__09f24__RAlwO')
            all_links = []
            for page in all_pages[1:]:
                if page.find('a'):
                    link = page.find('a').attrs['href']
                    all_links.append(link)

            for p in all_links:
                driver.get(p)
                time.sleep(2)
                tr1 = soup.find_all('div',
                                    class_='padding-t3__09f24__TMrIW padding-r3__09f24__eaF7p '
                                           'padding-b3__09f24__S8R2d padding-l3__09f24__IOjKY '
                                           'border-color--default__09f24__NPAKY')
                for tl in tr1:
                    try:
                        if "Motorcycle" in tl.find('p', class_='css-dzq7l1').text or "Bike" in tl.find('p',
                                                                                                       class_='css'
                                                                                                              '-dzq7l1').text:
                            new_url = tl.find('a').attrs['href']
                            new_url = 'https://www.yelp.com' + new_url
                            time.sleep(2)
                            driver.get(new_url)
                            time.sleep(2)
                            extract_data_and_write_to_csv(driver, city)
                            driver.back()
                            time.sleep(2)
                        print(f"{city}: Scraped successfully")
                    except Exception as e:
                        continue
            print(f"{index}  ,{city}: Scraped successfully")
        except Exception as e:
            # Set up Slack client object
            SLACK_BOT_TOKEN = os.environ.get('SLACK_BOT_TOKEN')
            client = WebClient(token=SLACK_BOT_TOKEN)

            # Prepare notification message
            message = f"An error occurred: {e}\nError message from Yelp.com code, page number: {index}, city: {city}"

            # Send notification to Slack channel using the client object
            SLACK_CHANNEL = '#test'
            try:
                response = client.chat_postMessage(channel=SLACK_CHANNEL, text=message)
            except SlackApiError as e:
                logging.error(f"Error sending Slack message: {e}")
            print()
    driver.quit()


with open('Uscities.csv', 'r') as file:
    reader = csv.reader(file)
    list_reader = list(reader)


def run_scraper(last_file_index):
    folder_path = "C:/Users/scrap/PycharmProjects/yelb/yelp_motorcycle_rental-2"
    files = []
    for file in os.listdir(folder_path):
        if os.path.isfile(os.path.join(folder_path, file)):
            files.append(file)
    if files:
        last_file = max(files).split('.')[0]
        if last_file == last_file_index:
            return
        start_index = None
        search_file_path = "C:/Users/scrap/PycharmProjects/yelb/Uscities.csv"
        with open(search_file_path, "r") as f:
            lines = f.readlines()
            for i, line in enumerate(lines):
                if last_file in line:
                    start_index = i + 1
                    break
            else:
                print(f"Could not find {last_file} in the file.")
                return
        for i, link in enumerate(lsit_reader[start_index:], start_index):
            scrape_city(link[0], i)


run_scraper(None)

# folder_path = "C:/Users/scrap/PycharmProjects/yelb/yelp_motorcycle_rental-2"
# files = []
# for file in os.listdir(folder_path):
#     if os.path.isfile(os.path.join(folder_path, file)):
#         files.append(file)
# if files:
#     last_file = max(files).split('.')[0]
#
#     search_file_path = "C:/Users/scrap/PycharmProjects/yelb/Uscities.csv"
#     with open(search_file_path, "r") as f:
#         lines = f.readlines()
#         for i, line in enumerate(lines):
#             if last_file in line:
#                 print(f"The index of the line containing {last_file} is: {i}")
#                 break
#         else:
#             print(f"Could not find {last_file} in the file.")
