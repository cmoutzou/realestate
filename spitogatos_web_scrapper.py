from datetime import datetime
import unicodedata
import os
import random
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import csv
import psycopg2
import pandas as pd
import re
import numpy as np
import json
from collections import defaultdict
import time
import warnings

# Hide warnings of pandas
warnings.filterwarnings("ignore", category=FutureWarning)

#python -m venv venv
#source venv/bin/activate
#psql -U moutz -d realestate


# Connct to DB
def connect_to_db():
    conn = psycopg2.connect(
        dbname="realestate",
        user="moutz",
        password="root",
        host="localhost",
        port="5432"
    )
    return conn

#Create table fot specific area on DB
def create_table_from_df(conn, df, table_name="spitogatos_glyfada_lst"):
    with conn.cursor() as cursor:
        # Drop the table if it exists
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Dynamically create a new table schema based on DataFrame columns
        column_definitions = []
        for column in df.columns:
            if column == 'details':
                column_definitions.append(f"{column} TEXT")
            elif df[column].dtype == 'int64':
                column_definitions.append(f"{column} INTEGER")
            elif df[column].dtype == 'float64':
                column_definitions.append(f"{column} FLOAT")
            else:
                column_definitions.append(f"{column} TEXT")

        # Create table with dynamic columns
        create_table_query = f"CREATE TABLE {table_name} ({', '.join(column_definitions)})"
        cursor.execute(create_table_query)
        print(f"Table {table_name} created with columns: {', '.join(df.columns)}")

    conn.commit()

# Insert DataFrame to DB
def insert_data_from_df(conn, df):
    # First, create table based on df structure
    create_table_from_df(conn, df)

    # Prepare data for insertion
    with conn.cursor() as cursor:
        # Dynamically prepare insert statement based on columns
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO spitogatos_glyfada_lst ({columns}) VALUES ({placeholders})"
        data_to_insert = []
        for _, row in df.iterrows():
            # Convert the 'details' dictionary to a string format if necessary
            if isinstance(row.get('details'), dict):
                row['details'] = '; '.join([f"{key}: {value}" for key, value in row['details'].items()])

            # Convert other fields to the required formats
            row['info'] = json.dumps(row['info'], ensure_ascii=False) if isinstance(row['info'], dict) else '{}'
            row['images'] = ', '.join(row['images']) if isinstance(row['images'], list) else ''

            # Append the row data as a tuple for insertion
            data_to_insert.append(tuple(row[col] for col in df.columns))

        # Log the data being inserted for debugging
        print("Inserting data into PostgreSQL:")
        #for data in data_to_insert:
            #print(data)

        cursor.executemany(insert_query, data_to_insert)  # Batch insert
    conn.commit()


# Selects a random User-Agent for web request variation.
def get_random_user_agent():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0.2 Safari/605.1.15"
        # Add more user agents as needed
    ]
    return random.choice(user_agents)

# Fetch data from url and save it to DataFrame
def fetch_web_data():
    url_small = 'https://www.spitogatos.gr/pwliseis-katoikies/lesvos-mitilini/timi_apo-420000/timi_eos-420000'
    url_big = "https://www.spitogatos.gr/pwliseis-katoikies/glyfada/timi_apo-400000/timi_eos-405000/"
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(f"user-agent={get_random_user_agent()}")

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)

    listings = []
    page_number = 1  # Start from page 1

    try:
        start_time = time.time()
        while True:
            try:
                print(f"Processing page {page_number}...")
                driver.get(f"{url_big}/order_price_asc/selida_{page_number}/")
                # Increase wait time
                WebDriverWait(driver, 50).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "content__wrap"))
                )
                print("Page loaded successfully.")

                # Get the page source after waiting for content
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                articles = soup.find_all('article', class_='ordered-element')

                # Exit if no articles are found (end of pagination)
                if not articles:
                    print("No more articles found. Exiting pagination.")
                    break

                # Total Number of serach results    
                total_results_elem = soup.find('h2', attrs={'data-v-370a313c': True})
                if total_results_elem:
                    total_results_text = total_results_elem.get_text(strip=True)
                    total_results = int(re.search(r'(\d+)', total_results_text).group(1))  # Extracts the total count
                else:
                    total_results = 0  # Default value if not found

                # Loop through each article and extract the relevant data
                article_page_counter = 0
                print(f'Starting process.. Total Number of Articles: {total_results} page:{page_number}')
                for article in articles:
                    try:
                        article_page_counter += 1
                        print(f'Processing article {article_page_counter}/{len(articles)} from page {page_number}')
                        # Extract the link and title
                        link_tag = article.find('a', class_='tile__link')
                        link = link_tag['href'] if link_tag else None
                        full_link = f"https://www.spitogatos.gr{link}" if link else "Link not found"

                        # Extract title, location, description, and price
                        title_elem = article.find('h3', class_='tile__title')
                        location_elem = article.find('h3', class_='tile__location')
                        description_elem = article.find('p', class_='tile__description')
                        price_elem = article.find('p', class_='price__text')
                        
                        title_text = title_elem.text.strip() if title_elem else "Title not found"
                        location_text = location_elem.text.strip() if location_elem else "Location not found"
                        description_text = description_elem.text.strip() if description_elem else "Description not found"
                        price_text = price_elem.text.strip() if price_elem else "Price not found"
                        p_type=title_text.split(',')[0]
                        # Extract info from the <ul class="title__info">
                        info_list = article.find('ul', class_='title__info')
                        info_items = {}
                        if info_list:
                            for li in info_list.find_all('li'):
                                item_title = li.find('strong').text.strip() if li.find('strong') else "No title"
                                item_text = li.text.replace(item_title, '').strip()  # Get the text after the title
                                info_items[item_title] = item_text

                        images_soup = article.select('div.carousel img')
                        images=[]
                        # Extract and print each image URL
                        image_urls = [img['src'] for img in images_soup if img['src'].startswith('https://')]
                        for url in image_urls:
                            images.append(url)
                        
                        # Create a placeholder for property details
                        listing = {
                            'title': title_text,
                            'type':p_type,
                            'location': location_text,
                            'description': description_text,
                            'link': full_link,
                            'info': info_items,
                            'images': images,
                            'price': price_text,
                            'details': {}  # Placeholder for property details
                        }
                        listings.append(listing)

                        # Visit each property link to get detailed info
                        driver.get(full_link)
                        time.sleep(random.uniform(1, 3))  # Wait for page to load

                        # Get the page source after loading the property details
                        property_page_source = driver.page_source
                        property_soup = BeautifulSoup(property_page_source, 'html.parser')

                        # Extract property details
                        details_list = property_soup.find('dl', class_='property__details details')
                        rooms_icon = property_soup.find('li', id='roomsIcon')
                        rooms_number = rooms_icon.find_all('span')[1].text.strip()
                        listing['rooms_number'] = rooms_number

                        if details_list:
                            listing['details'] = {}
                            # Extract <dt> and <dd> pairs
                            detail_items = details_list.find_all(['dt', 'dd'])
                            for i in range(0, len(detail_items), 2):
                                if i + 1 < len(detail_items):
                                    key = detail_items[i].text.strip()
                                    value = detail_items[i + 1].text.strip()
                                    listing['details'][key] = value  # Store details in the listing
                        else:
                            print(f"No details found for {listing['link']}.")
                    except Exception as e:
                        print(f"An error occurred: {e}")
                        continue
            except Exception as e:
                print(f"An error occurred: {e}")
                continue    
            # Check for the next page link
            next_page_link = soup.find('a', class_='page-link', string='›')

            # Check if next page link is present and valid
            if next_page_link and 'href' in next_page_link.attrs:
                next_page_href = next_page_link['href']
                if next_page_href:
                    print("Going to the next page...")
                    page_number += 1
                else:
                    print("No next page available. Exiting pagination.")
                    break
            else:
                print("No more pages available. Exiting pagination.")
                break

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Calculate elapsed time
        end_time = time.time() 
        elapsed_time = end_time - start_time  
        print(f"Data fetched in {elapsed_time:.2f} seconds") 
        driver.quit()

    return pd.DataFrame(listings)

def clean_column_name(name):
    greek_to_english_map = {
    'Α': 'A', 'Β': 'B', 'Γ': 'G', 'Δ': 'D', 'Ε': 'E', 'Ζ': 'Z', 'Η': 'H', 'Θ': 'Th', 
    'Ι': 'I', 'Κ': 'K', 'Λ': 'L', 'Μ': 'M', 'Ν': 'N', 'Ξ': 'X', 'Ο': 'O', 'Π': 'P', 
    'Ρ': 'R', 'Σ': 'S', 'Τ': 'T', 'Υ': 'Y', 'Φ': 'F', 'Χ': 'Ch', 'Ψ': 'Ps', 'Ω': 'O',
    'α': 'a', 'β': 'b', 'γ': 'g', 'δ': 'd', 'ε': 'e', 'ζ': 'z', 'η': 'h', 'θ': 'th',
    'ι': 'i', 'κ': 'k', 'λ': 'l', 'μ': 'm', 'ν': 'n', 'ξ': 'x', 'ο': 'o', 'π': 'p',
    'ρ': 'r', 'σ': 's', 'τ': 't', 'υ': 'y', 'φ': 'f', 'χ': 'ch', 'ψ': 'ps', 'ω': 'o',
    'ό':'o','Ό':'O','ί':'i','Ί':'Ι','ς':'s','ή':'h','ά':'a','έ':'e','Έ':'E'
}
    # Replace Greek characters with English equivalents
    name = ''.join(greek_to_english_map.get(char, char) for char in name)
    # Normalize to remove accents
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    # Replace spaces with underscores
    name = re.sub(r'\s+', '_', name)
    # Remove all non-alphanumeric and non-underscore characters
    name = re.sub(r'[^a-zA-Z0-9_]', '', name).lower()
    return name

# ETL processing
def clean_data(df):
    # Helper function to clean text
    def clean_text(text):
        if isinstance(text, str):
            return text.strip().replace('\n', ' ').replace('\r', '').replace('  ', ' ')
        return text

    # Clean each column's text content
    for column in df.columns:
        df[column] = df[column].apply(clean_text)
    
    # Drop rows with missing essential information
    df = df.dropna(subset=['title', 'link'])

    # Replace NaN values in other columns with "Not available"
    df.fillna("Not available", inplace=True)

    # Clean 'title'
    df['title'] = df['title'].str.replace(r'[-+\s]', '', regex=True)

    # Clean and convert 'price' to numeric
    df['price'] = df['price'].replace(r'[^0-9]', '', regex=True).replace('', np.nan).replace('Χρηματοδότησέ το','')
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    # Clean up the 'details' column
    df['details'] = df['details'].replace('-', '').replace('+', '').replace('Χρηματοδότησέ το','').replace('τ.μ.','').replace('€','')

    # Convert 'info' to JSON strings if it's a dictionary
    df['info'] = df['info'].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else x)

    # Join 'images' as a single comma-separated string if it's a list
    #df['images'] = df['images'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

    # Standardize whitespace in 'description' and 'location'
    df['description'] = df['description'].str.replace('\n', '').str.strip()
    df['location'] = df['location'].str.strip()

    # Dictionary to store new columns for details processing
    new_columns = defaultdict(list)
    
    # Process each row in the DataFrame to split 'details' into key-value pairs
    for i, row in df.iterrows():
        details_dict = row['details']        
        # Ensure it's a dictionary before processing
        if isinstance(details_dict, dict):
            for key, value in details_dict.items():
                clean_key = clean_column_name(key).strip()
                clean_value = re.sub(r'\s+', ' ', value.strip())  # Remove unwanted whitespace
                clean_value = clean_value.replace('-', '').replace('+', '').replace('Χρηματοδότησέ το','').replace('τ.μ.','').replace('€','')
                # Add each key-value pair as a new column
                df.loc[i, clean_key] = clean_value
                # '{clean_key}' with value '{clean_value}' for row index {i}")

    # Add the new columns to the DataFrame with default values where necessary
    for column_name, values in new_columns.items():
        if len(values) < len(df):
            values.extend([None] * (len(df) - len(values)))  # Pad the list with None for missing values
        df[column_name] = values
    print(f'Data Cleaning Completed.')
    return df


# Save data to csv
def save_to_csv(data, table_name="spitogtos_glyfada_lst"):
    cleaned_data = clean_data(data)  # Get cleaned data

    # Define headers, including details if they exist
    headers = ['title', 'location', 'description', 'link', 'price'] + list(cleaned_data.columns)

    # File path for saving
    file_path = os.path.expanduser(f"~/Desktop/drive/python/projects/realestate/{table_name}.csv")

    # Write to CSV
    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()

        for _, record in cleaned_data.iterrows():
            row = {key: record.get(key, '') for key in headers[:-1]}  # Exclude 'details'
            row['details'] = str(record['details']).replace('\n', ' ').strip() if isinstance(record['details'], dict) else ""
            writer.writerow(row)


def basic_statistics(df):
    # Average Property Price
    average_price = df['price'].mean()

    # Average Price per Square Meter
    df['timh_ana_tm'] = df['timh_ana_tm'].str.replace('€', '').str.replace('.', '').astype(float)
    average_price_per_sqm = df['timh_ana_tm'].mean()

    # Number of Listings
    number_of_listings = df.shape[0]

    # Property Types
    property_type_counts = df['type'].value_counts()

    # Average Size
    df['embadon'] = df['embadon'].astype(float)
    average_size = df['embadon'].mean()

    # Number of Rooms
    df['rooms_number'] = pd.to_numeric(df['rooms_number'], errors='coerce')
    average_rooms = df['rooms_number'].mean()

    # 7. Year of Construction
    # Here, you might want to filter out unrealistic years if needed
    df['etos_kataskeyhs'] = pd.to_numeric(df['etos_kataskeyhs'], errors='coerce')
    average_year_built = df['etos_kataskeyhs'].mean()

    # 8. Heating Systems
    heating_system_counts = df['ssthma_thermanshs'].value_counts()

    # 9. Availability dhmosieysh_aggelias 
    df['dhmosieysh_aggelias'] = pd.to_datetime(df['dhmosieysh_aggelias'], format='%d/%m/%Y', errors='coerce')
    df['days_on_market'] = (datetime.now() - df['dhmosieysh_aggelias']).dt.days
    average_days_on_market = df['days_on_market'].mean()

    # Print Results
    print(f"Average Property Price: €{average_price:,.2f}")
    print(f"Average Price per Square Meter: €{average_price_per_sqm:,.2f}")
    print(f"Number of Listings: {number_of_listings}")
    print("Property Types:\n", property_type_counts)
    print(f"Average Size (sqm): {average_size:.2f}")
    print(f"Average Number of Rooms: {average_rooms:.2f}")
    print(f"Average Year Built: {average_year_built:.0f}")
    print("Heating Systems:\n", heating_system_counts)
    print(f"Average Days on Market: {average_days_on_market:.2f}")

def main():
    conn = connect_to_db()
    data = fetch_web_data()
    cleaned_data = clean_data(data)
    #print(cleaned_data.head())
    insert_data_from_df(conn, cleaned_data)
    conn.close()
    print("Data has been cleaned and inserted successfully.")
    basic_statistics(cleaned_data)
    save_to_csv(cleaned_data)  # Save cleaned data instead of raw data

if __name__ == "__main__":
    main()