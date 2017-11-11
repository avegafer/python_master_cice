from scraping.core.prefixed_mongo_wrapper import PrefixedMongoWrapper
from scraping.core.stdout_logger import Logger
import pandas as pd
from difflib import SequenceMatcher
import os
import re

class PlayerNormalizer:
    def __init__(self):

        self.logger = Logger(2)
        self.default_csv_filename = './players_mapping.csv'

    def find_player_id(self, source, player):

        self.data = self._get_raw_data()
        results_indexes = self.data['master'].index[self.data[source] == player]
        if len(results_indexes) > 1:
            self.logger.error(300, 'More than a candidate')


        for result in results_indexes:
            return result

        self.logger.error(100, 'Cannot find map for ' + source + ': ' + player)
        return ''

    def _get_raw_data(self):
        if not os.path.isfile(self.default_csv_filename):
            data = self.normalize()
            self.save_csv(data)


        return pd.read_csv(self.default_csv_filename)


    def _get_master_list(self):
        '''
        Siendo que lo que interesa es sacar los jugadores de este año, construimos el master sacando
        los jugadores del año pasado en primera y segunda
        :return:
        '''
        self.logger.debug('Generating master')


        mongo_wrapper = PrefixedMongoWrapper('laliga_web')
        result = mongo_wrapper.get_collection('players').distinct('player')

        self.logger.debug('Done')
        return result

    def _get_marca_list(self):
        result = []

        mongo_wrapper = PrefixedMongoWrapper('marca_current_season')
        for day in mongo_wrapper.get_collection('results').find({"results.home_lineup": {"$exists": True}}):
            for match in day['results']:
                result += match['home_lineup']
                result += match['away_lineup']


        return list(set(result))

    def normalize(self):
        self.master = self._get_master_list()

        self.logger.debug('Normalizing data...')

        return self._normalize_one('marca', self._get_marca_list())

    def save_csv(self, result):
        self.logger.debug('Creating ' + self.default_csv_filename)
        csv_filename = self.default_csv_filename

        repo = pd.DataFrame(result)
        repo.index += 1
        repo.to_csv(csv_filename)


    def _normalize_one(self, source, players):

        result = {
            'master': [],
            source: [],
        }

        num_matched = 0
        for master_player in self.master:
            best_similarity = 0
            second_best_similarity = 0
            matched = ''
            for player in players:

                matcher = SequenceMatcher(None, master_player.lower(), player.lower())
                similarity = matcher.ratio()
                if (similarity > best_similarity) and (similarity > 0.75) and (second_best_similarity < 0.60) :
                    second_best_similarity = best_similarity
                    best_similarity = similarity
                    matched = player

            if matched != '':
                self.logger.debug('Matched ' + matched + ' with ' + master_player + ' ' + str(best_similarity))
                num_matched += 1

            result['master'].append(master_player)
            result[source].append(matched)

        self.logger.debug(str(len(players)) + ' players, ' + str(num_matched) + ' matched')

        for source_player in players:
            if source_player not in result[source]:
                self.logger.log(50, 'Unmatched ' + source_player)

        return result


