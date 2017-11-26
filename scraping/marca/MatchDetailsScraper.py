from scraping.core.scrape_request import Sender
from scraping.core.stdout_logger import Logger

from bs4 import BeautifulSoup


class MatchDetailsScraper:
    def __init__(self, url):
        self.url = url
        self.logger = Logger(2)

    def extract_lineups(self):

        sender = Sender()
        sender.set_debug_level(2)
        response = sender.get(self.url, {})
        self.logger.debug('Processing lineups of ' + self.url)

        html = BeautifulSoup(response, 'html.parser')
        result = {
            'home': '',
            'away': ''

        }

        teams_container_main = html.find('section', {'class': 'laliga-fantasy columna2'})
        if not teams_container_main is None:
            teams_container = teams_container_main.find_all('section')[:2]
            home_container = teams_container[0]
            away_container = teams_container[1]

            result = {
                'home': self._extract_players(home_container),
                'away': self._extract_players(away_container)
            }
        else:
            self.logger.error(500, 'No lineup found in ' + self.url)

        return result

    def _extract_players(self, team_container):
        result = []
        for player in team_container.find_all('li'):
            result.append(player.getText().strip())

        #self.logger.debug('Retrieving ' + str(result))
        if len(result) != 11:
            self.logger.error(100, 'Team with ' + str(len(result)) + ' players')
        return result
