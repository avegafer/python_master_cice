from scraping.aggregations.MatchesFactsAggregator import MatchesFactsAggregator
from scraping.laliga import utils

csv_filename = ''
if csv_filename == '':
    print('Setea la ruta completa del fichero CSV')
    exit()

aggregator = MatchesFactsAggregator(2)
seasons = utils.build_seasons('primera', 1928)
seasons.append('primera/2017-18')

for season in seasons:
    aggregator.process_matches_played(season)
aggregator.write_data_csv(csv_filename + '_past.csv')

#aggregator = MatchesFactsAggregator(2)
#aggregator.process_matches_to_play('primera/2017-18')
#aggregator.write_data_csv(csv_filename + '_future.csv')
