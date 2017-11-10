from scraping.core.prefixed_mongo_wrapper import PrefixedMongoWrapper
from scraping.core.stdout_logger import Logger
import pandas as pd


class PlayerNormalizer:
    def __init__(self):

        self.logger = Logger(2)
        self.default_csv_filename = './players_mapping.csv'

    def _get_master_list(self):
        result = []

        mongo_wrapper = PrefixedMongoWrapper('laliga_web_primera')
        for match in mongo_wrapper.get_collection('results').find({'season': {"$in": ['primera/2015-16', 'primera/2016-17']}}):
            for stat in mongo_wrapper.get_collection('popups_matches_stats').find({'match_id': match['match_id']}):
                result.append(stat['player'])
            result = list(set(result))
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
        result = {
            'master': self.master,
            'marca': self._normalize_one(self._get_marca_list()),
        }

        return result

    def save_csv(self, result):
        self.logger.debug('Creating ' + self.default_csv_filename)
        csv_filename = self.default_csv_filename

        repo = pd.DataFrame(result)
        repo.to_csv(csv_filename)


    def _normalize_one(self, players):


        from difflib import SequenceMatcher
        result = []


        for master_player in self.master:
            best_similarity = 0
            matched = ''
            for player in players:

                matcher = SequenceMatcher(None, master_player.lower(), player.lower())
                similarity = matcher.ratio()
                if (similarity > best_similarity) and (similarity > 0.8):
                    best_similarity = similarity
                    matched = player

            if matched != '':
                self.logger.debug('Matched ' + matched + ' with ' + master_player + ' ' + str(best_similarity))
            result.append(matched)
        return result


