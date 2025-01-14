# realestate Web Scraper

This project is a web scraper designed to extract real estate listings from [Spitogatos.gr](https://www.spitogatos.gr) and store the data in a PostgreSQL database. It uses Selenium and BeautifulSoup for web scraping and psycopg2 for database interactions.

## Features
- Scrapes property details including title, location, description, price, and images.
- Handles pagination and extracts detailed property information from individual listings.
- Stores the extracted data in a PostgreSQL database with a dynamically generated schema.

## Technologies Used
- Python
- Selenium
- BeautifulSoup
- PostgreSQL
- Pandas
- Psycopg2
- Chrome WebDriver

## Setup and Installation

### Prerequisites
- Python 3.x
- PostgreSQL
- Google Chrome

### Installation
1. Clone the repository:
    ```bash
    git clone https://github.com/YOUR_USERNAME/spitogatos_web_scraper.git
    cd spitogatos_web_scraper
    ```

2. Set up a virtual environment:
    ```bash
    python -m venv venv
    source venv/bin/activate
    ```

3. Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

4. Set up the PostgreSQL database and update the connection settings in the `connect_to_db` function.

### Running the Scraper
Run the scraper with:
```bash
python scraper.py
