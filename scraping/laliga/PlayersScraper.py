from scraping.core.scrape_request import Sender
from scraping.core.stdout_logger import Logger
from scraping.core.prefixed_mongo_wrapper import PrefixedMongoWrapper
from bs4 import BeautifulSoup

class PlayersScraper:

    def __init__(self):

        self.logger = Logger(2)
        self.mongo_wrapper = PrefixedMongoWrapper('laliga_web')
        self.base_url = 'http://www.laliga.es/estadisticas-historicas/plantillas/'
        self.sender = Sender()
        self.sender.set_debug_level(2)
        self.collection = 'players'


    def get_teams(self, season):
        response = self.sender.get(self.base_url + season, {})
        result = []
        if response != '':
            html = BeautifulSoup(response, 'html.parser')

            for item in html.find('select', {'id': 'select_equipos_estadisticas_historicas'}).find_all('option'):

                team = item['value']
                if team != '':
                    result.append(team)

        return result

    def scrape_season(self, season):

        teams = self.get_teams(season)

        for team in teams:
            self.scrape_team(team, season)

    def scrape_team(self, team, season):
        self.logger.debug('Processing ' + team + ' ' + season)
        response = self.sender.get(self.base_url + season + '/' + team, {})

        if response != '':
            html = BeautifulSoup(response, 'lxml')

            container = html.find('div', {'class': 'container main table clearfix'})
            for row in container.find('tbody').find_all('tr'):
                dict = {
                    'player': row.find('td').getText(),
                    'team': team,
                    'season': season
                }


                self.mongo_wrapper.write_dictionary(self.collection, dict)
