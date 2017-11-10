from scraping.core.prefixed_mongo_wrapper import PrefixedMongoWrapper
from scraping.core.stdout_logger import Logger

class LineUpManager:

    def __init__(self):
        self.logger = Logger()

    def create_by_match_id(self, match_id, team):
        mongo_wrapper = PrefixedMongoWrapper('laliga_web_primera')

        stats = mongo_wrapper.get_collection('popups_matches_stats').find({'match_id': match_id, 'team': team, 'main_lineup': True})

        result = []
        for stat in stats:
            result.append(stat['player'])

        if len(result) != 11:
            self.logger.error(100, 'Lineup of ' + str(len(result)) + ' players')

        return result