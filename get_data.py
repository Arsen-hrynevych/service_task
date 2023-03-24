import psycopg2
import requests
from bs4 import BeautifulSoup
import lxml
import re

class WikiScraper:
    def __init__(self, dbname, user, password, host):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host

    def connect(self):
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host
        )
        self.cur = self.conn.cursor()

    def create_table(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS List 
            (Country VARCHAR(255), 
            Population INT);
        """)

    def close(self):
        self.cur.close()
        self.conn.close()

    def scrape(self, url):
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'lxml')

        country_tags = soup.find_all('td', {'scope': 'row'})
        population_tags = soup.find_all('td', {'style': 'text-align:right'})[1:] #skip first number "100%""

        for country, population in zip(country_tags, population_tags):
            country_name = country.find('a').text
            population_str = re.sub(r'[^\d,]', '', population.text)  # remove non-digit characters except commas
            population_number = int(population_str.replace(',', ''))

            query = f"INSERT INTO List (Country, Population) VALUES ('{country_name}', '{population_number}')"
            self.cur.execute(query)

        self.conn.commit()

if __name__ == '__main__':
    scraper = WikiScraper(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="db"
    )
    scraper.connect()
    scraper.create_table()
    scraper.scrape("https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population")
    scraper.close()
