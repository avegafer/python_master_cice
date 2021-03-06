from scraping.core.prefixed_mongo_wrapper import PrefixedMongoWrapper
from scraping.core.stdout_logger import Logger
import pandas as pd
from difflib import SequenceMatcher
import os


class PlayerNormalizer:
    def __init__(self):

        self.logger = Logger(2)
        self.default_csv_filename = './players_mapping.csv'
        self.loaded = False

    def find_player_id(self, source, player):

        self._init_data()
        results_indexes = self.data['master'].index[self.data[source] == player]
        if len(results_indexes) > 1:
            self.logger.error(300, 'More than a candidate ('+str(len(results_indexes))+'): ' + player)


        for result in results_indexes:
            return result

        self.logger.error(100, 'Cannot find map for ' + source + ': ' + player)
        return ''

    def _init_data(self):
        if self.loaded == False:
            self.logger.debug('Loading map file')
            if not os.path.isfile(self.default_csv_filename):
                self.init_map_file()

            self.data = pd.read_csv(self.default_csv_filename)
            self.loaded = True


    def init_map_file(self):
        self.logger.debug('Generating master')


        mongo_wrapper = PrefixedMongoWrapper('laliga_web')
        # datos sacados del apartado "plantillas"
        #result = mongo_wrapper.get_collection('players').find({'season': 'primera/2016-17'}).distinct('player')
        #result += mongo_wrapper.get_collection('players').find({'season': 'segunda/2016-17'}).distinct('player')

        # faltan los de la temporada 1928-29 integramos con los resultados de primera y segunda
        result = mongo_wrapper.get_collection('players').distinct('player')
        result += mongo_wrapper.get_collection('primera_popups_matches_stats').distinct('player')
        result += mongo_wrapper.get_collection('segunda_popups_matches_stats').distinct('player')

        self.logger.debug('Done')
        data = {'master': list(set(result))}
        self.save_csv(data)


    def _get_marca_list(self):
        result = []

        mongo_wrapper = PrefixedMongoWrapper('marca_current_season')
        for day in mongo_wrapper.get_collection('results').find({"results.home_lineup": {"$exists": True}}):
            for match in day['results']:
                result += match['home_lineup']
                result += match['away_lineup']


        return list(set(result))



    def normalize(self):
        self._init_data()
        self.logger.debug('Normalizing data...')

        return self._normalize_one('marca', self._get_marca_list())

    def save_csv(self, result):
        self.logger.debug('Creating ' + self.default_csv_filename)
        csv_filename = self.default_csv_filename

        repo = pd.DataFrame(result)
        repo.index += 1
        repo.to_csv(csv_filename)

    def get_valid_players(self):
        mongo_wrapper = PrefixedMongoWrapper('laliga_web')

        result = mongo_wrapper.get_collection('primera_popups_matches_stats').distinct('player')
        result += mongo_wrapper.get_collection('players').find({'season': 'primera/2016-17'}).distinct('player')
        result += mongo_wrapper.get_collection('players').find({'season': 'segunda/2016-17'}).distinct('player')

        return list(set(result))


    def _normalize_one(self, source, players):

        result = {
            'master': [],
            source: [],
        }

        num_matched = 0
        valid_players = self.get_valid_players()
        #print(valid_players)
        #exit()

        already_got = []

        for master_player in self.data['master']:

            best_similarity = 0
            second_best_similarity = 0
            matched = ''

            if master_player in valid_players:

                for player in players:

                    matcher = SequenceMatcher(None, self.preprocess_name(master_player), self.preprocess_name(player))
                    similarity = matcher.ratio()
                    if (similarity > best_similarity) and \
                            (similarity > 0.95) and \
                            (second_best_similarity < 0.60) and \
                            (player not in already_got):

                        second_best_similarity = best_similarity
                        best_similarity = similarity
                        matched = player

                if matched != '':
                    self.logger.debug('Matched "' + matched + '" with "' + master_player + '" ' + str(best_similarity))
                    already_got.append(matched)
                    num_matched += 1

            result['master'].append(master_player)
            result[source].append(matched)



        self.logger.debug(str(len(players)) + ' players, ' + str(num_matched) + ' matched')
        return result


    def preprocess_name(self, name):
        result = name.lower()
        result = result.replace(',', '')
        return result
