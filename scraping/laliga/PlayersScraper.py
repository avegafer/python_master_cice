from scraping.core.scrape_request import Sender
from scraping.core.stdout_logger import Logger
from scraping.core.prefixed_mongo_wrapper import PrefixedMongoWrapper
from bs4 import BeautifulSoup

class PlayersScraper:

    def __init__(self):

        self.logger = Logger(2)
        self.mongo_wrapper = PrefixedMongoWrapper('laliga_web')
        self.base_url = 'http://www.laliga.es/estadisticas-historicas/plantillas/'


    def get_teams(self, season):
        sender = Sender()

        response = sender.get(self.base_url + season, {})
        result = []
        if response != '':
            html = BeautifulSoup(response, 'html.parser')

            for item in html.find('select', {'id': 'select_equipos_estadisticas_historicas'}).find_all('option'):

                item_content = item.getText().strip()
                if item_content != 'Equipo':
                    result.append(item_content)

        return result