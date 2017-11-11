from scraping.core.prefixed_mongo_wrapper import PrefixedMongoWrapper
from scraping.core.stdout_logger import Logger
from scraping.aggregations.TeamsNormalizer import TeamsNormalizer
from scraping.laliga.LineUpManager import LineUpManager

import scraping.laliga.utils
import pymongo


class ResultsMerger:

    def __init__(self):

        self.logger = Logger(2)
        self.mapper = TeamsNormalizer()

        self.template = {'season': 'primera/2017-18',
                         'day_num': '',
                         'home': '',
                         'away': '',
                         'score_home': '',
                         'score_away': ''}

    def merge(self):

        self.logger.debug('Merging...')

        results = []
        results += self._get_archive_results()
        results += self._get_current_results()

        self.logger.debug('Processed ' + str(len(results)) + ' matches')

        self.logger.debug('Done')
        return results

    def save(self, result):
        self.logger.debug('Saving')
        wrapper = PrefixedMongoWrapper('etl_results')
        wrapper.drop_collection('all')
        wrapper.write_dictionaries_list('all', result)
        self.logger.debug('Done')



    def _get_archive_results(self):
        self.logger.debug('Getting archive results ... this will take time ...')
        wrapper = scraping.laliga.utils.create_mongo_writer()
        archive = wrapper.get_collection('primera_results').find().sort([('day_num', pymongo.ASCENDING)])
        result = []

        lineup_manager = LineUpManager()

        #el historico
        for archive_match in archive:

            entry = self.template.copy()
            entry['season'] = archive_match['season']
            entry['day_num'] = int(archive_match['day_num'])
            entry['home'] = archive_match['home']
            entry['away'] = archive_match['away']
            entry['score_home'] = int(archive_match['score_home'])
            entry['score_away'] = int(archive_match['score_away'])
            entry['lineup_home'] = lineup_manager.create_by_match_id(archive_match['match_id'], archive_match['home'])
            entry['lineup_away'] = lineup_manager.create_by_match_id(archive_match['match_id'], archive_match['away'])

            self.logger.debug('Processing ' + str(entry))
            result.append(entry)

        return result

    def _extract_scores(self, text):

        result = {}
        splitted = text.split('-')

        if len(splitted) == 2:
            if (len(splitted[0]) <= 2) and (len(splitted[1]) <= 2):
                result['score_home'] = int(splitted[0])
                result['score_away'] = int(splitted[1])

        return result

    def _get_current_results(self):
        self.logger.debug('Getting current season results')

        result = []
        wrapper = PrefixedMongoWrapper('marca')

        #los actuales bajados por la web de marca
        for day in wrapper.get_collection('current_season_results').find():
            for match in day['results']:

                match['result'] = self._marca_process_result(match['result'])

                entry = self.template.copy()
                entry['day_num'] = int(day['day']['num_day'].replace('Jornada', '').strip())
                entry['home'] = self.mapper.find_team_id('marca', match['home'])
                entry['away'] = self.mapper.find_team_id('marca', match['away'])

                scores = self._extract_scores(match['result'])

                if len(scores) == 2:
                    entry['score_home'] = scores['score_home']
                    entry['score_away'] = scores['score_away']
                else:
                    entry['score_home'] = match['result']
                    entry['score_away'] = match['result']


                result.append(entry)
        return result

    def _marca_process_result(self, text):
        '''

        En la web de marca en las celdas con los resultados pueden haber tres tipos de info, por ejemplo:
        1-1
        Sab-  13:00
        Sin confirmar

        controlamos simplemente por longitud del text

        :param text:
        :return:
        '''
        result = ''
        if len(text) < 8:
            result = text

        return result







