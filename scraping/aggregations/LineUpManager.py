from scraping.core.prefixed_mongo_wrapper import PrefixedMongoWrapper
from scraping.core.stdout_logger import Logger
from scraping.aggregations.PlayerNormalizer import PlayerNormalizer

class LineUpManager:

    def __init__(self):
        self.logger = Logger()
        self.normalizer = PlayerNormalizer()

    def create_by_match_id(self, match_id, team):
        mongo_wrapper = PrefixedMongoWrapper('laliga_web_primera')

        stats = mongo_wrapper.get_collection('popups_matches_stats').find({'match_id': match_id, 'team': team, 'main_lineup': True})

        result = []
        for stat in stats:
            player_id = self.normalizer.find_player_id('master', stat['player'])
            if (player_id != ''):
                result.append(str(player_id))
            else:
                self.logger.log(400, 'Unmatched player ' + stat['player'])

        if len(result) != 11:
            self.logger.error(100, 'Lineup of ' + str(len(result)) + ' players')


        # ordena alfabeticamente.
        result.sort()
        return '_'.join(result)