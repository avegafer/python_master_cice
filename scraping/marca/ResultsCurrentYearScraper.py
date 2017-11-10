from scraping.core.scrape_request import Sender
from scraping.core.stdout_logger import Logger
from scraping.core.prefixed_mongo_wrapper import PrefixedMongoWrapper
from scraping.marca.MatchDetailsScraper import MatchDetailsScraper

from bs4 import BeautifulSoup

class ResultsCurrentYearScraper:

    def __init__(self):
        self.sender = Sender()
        self.logger = Logger(2)
        self.url = 'http://www.marca.com/futbol/primera-division/calendario.html'
        self.raw_content = ''
        self.writer = PrefixedMongoWrapper('marca')
        self.collection_name = 'current_season_results'

    def _getPage(self):

        self.raw_content = self.sender.get(self.url, {})

        if self.raw_content == '':
            self.logger.error(500, 'Empty page')
            exit()

    def scrape(self):
        self.logger.debug('Downloading marca web data')

        self._getPage()
        self.writer.drop_collection(self.collection_name)

        html = BeautifulSoup(self.raw_content, 'html.parser')

        for day_table in html.find_all('li', {'id': 'contenedorCalendarioInt'}):
            day_info = self.extract_day(day_table)
            self.logger.debug('Processing "' + day_info['num_day'] + ', ' + day_info['date'] + '"')
            results = self.process_results(day_table)

            dictionary_to_insert = {'day': day_info, 'results': results}
            self.writer.write_dictionary(self.collection_name, dictionary_to_insert)

        self.logger.debug('Done')

    def extract_day(self, day_table):
        header = day_table.find('span')
        num_day = header.find('h2').getText()
        date = header.contents[2].strip()

        return {'num_day': num_day, 'date': date}

    def process_results(self, day_table):
        results = []
        for row in day_table.find('ul', {'class': 'partidos-jornada'}).find_all('a'):
            counter = 0;
            result = {}
            colmap = {0: 'home', 1: 'away', 2: 'result'}
            for cell in row.find_all('span'):
                result[colmap[counter % 3]] = cell.getText()
                counter = counter + 1
            if row.has_attr('href'):
                result['url_details'] = row['href']
                lineups = self._get_lineups(result['url_details'])
                result['home_lineup'] = lineups['home']
                result['away_lineup'] = lineups['away']

            results.append(result)

        self.logger.debug('Inserted ' + str(len(results)) + ' items')
        return results

    def _get_lineups(self, url):
        scraper = MatchDetailsScraper(url)
        return scraper.extract_lineups()


