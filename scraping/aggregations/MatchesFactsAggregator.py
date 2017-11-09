from scraping.core.prefixed_mongo_wrapper import PrefixedMongoWrapper
from scraping.core.stdout_logger import Logger

import pymongo

class MatchesFactsAggregator:
    '''
    Genera la tabla de facts partidos en csv considerando los partidos terminados

    En self.counters se van actualizando los valores incrementales, tipo los goles marcados a la fecha.

    Para modificar los ficheros que genera hay que definir el template de los registros que se van
    generando en self.counter_template, y poner conforme los valores en _update_counters
    '''
    def __init__(self, log_detail_level = 0):
        self.prefix = 'etl'
        self.mongo_wrapper = PrefixedMongoWrapper(self.prefix)
        self.collection = 'results_all'
        self.results = []

        self.counters = {}
        self.tournament_positions = {}
        self.logger = Logger(log_detail_level)

        #Diccionario con los datos recientes. Ver self._update_counters
        self.teams_recent_history = {}
        self.counter_template = {
            'played_home': 0,
            'played_away': 0,

            'score_competition': 0,

            'goals_scored': 0,
            'goals_conceded': 0,

            'num_days_without_goals': 0,
            'num_days_without_victory': 0,
            'ranking': 0

        }


    def _update_counters(self, match):

        match_winner = self._winner(match)

        if match_winner != '':

            # Goles hechos por cada equipo
            self._add_to_counter(match['home'], 'goals_scored', match['score_home'])
            self._add_to_counter(match['away'], 'goals_scored', match['score_away'])

            # Partidos jugados
            self._add_to_counter(match['home'], 'played_home', 1)
            self._add_to_counter(match['away'], 'played_away', 1)

            self._add_to_counter(match['home'], 'goals_conceded', match['score_away'])
            self._add_to_counter(match['away'], 'goals_conceded', match['score_home'])

            # añado al historiar los goles hechos
            self.teams_recent_history[match['home']]['goals'].append(int(match['score_home']))
            self.teams_recent_history[match['away']]['goals'].append(int(match['score_away']))

            # Suma de los goles hechos en los últimos 5 días
            self._set_counter(match['home'], 'ranking', sum(self.teams_recent_history[match['home']]['goals'][-5:]))
            self._set_counter(match['away'], 'ranking', sum(self.teams_recent_history[match['away']]['goals'][-5:]))

            # Puntos
            key_map = {'home': 3, 'away': 0, 'none': 1}
            self._add_to_counter(match['home'], 'score_competition', key_map[match_winner])

            key_map = {'home': 0, 'away': 3, 'none': 1}
            self._add_to_counter(match['away'], 'score_competition', key_map[match_winner])

            # Días sin ganar
            if match_winner == 'home':
                self._set_counter(match['home'], 'num_days_without_victory', 0)
                self._add_to_counter(match['away'], 'num_days_without_victory', 1)

            if match_winner == 'away':
                self._set_counter(match['away'], 'num_days_without_victory', 0)
                self._add_to_counter(match['home'], 'num_days_without_victory', 1)

            if match_winner == 'none':
                self._add_to_counter(match['home'], 'num_days_without_victory', 1)
                self._add_to_counter(match['away'], 'num_days_without_victory', 1)

            # Días sin marcar
            if int(match['score_home']) > 0:
                self._set_counter(match['home'], 'num_days_without_goals', 0)
            else:
                self._add_to_counter(match['home'], 'num_days_without_goals', 1)

            if int(match['score_away']) > 0:
                self._set_counter(match['away'], 'num_days_without_goals', 0)
            else:
                self._add_to_counter(match['away'], 'num_days_without_goals', 1)



    def _generate_tournament_positions(self):
        tournament_scores = {}

        raw_data = self.counters

        for team in raw_data.keys():

            score_competition = raw_data[team]['score_competition']

            if score_competition not in tournament_scores.keys():
                tournament_scores[score_competition] = []

            tournament_scores[score_competition].append(team)

        tournament_positions = {}
        current_position = 1
        for team_score in reversed(sorted(tournament_scores)):

            if current_position not in tournament_positions:
                tournament_positions[current_position] = []

            tournament_positions[current_position] += tournament_scores[team_score]
            current_position += 1

        result = {}
        for current_position in tournament_positions.keys():
            for team in tournament_positions[current_position]:
                result[team] = current_position

        self.tournament_positions = result

    def process_matches_played(self, season):
        self._init_counters(season)
        for match in self._collection().find({'season': season}).sort([('day_num', pymongo.ASCENDING)]):
            entry = self._process_match(match)
            if entry['winner'] != '':
                self._add_to_results(entry)

    def process_matches_to_play(self, season):
        self._init_counters(season)
        for match in self._collection().find({'season': season}).sort([('day_num', pymongo.ASCENDING)]):
            entry = self._process_match(match)
            if entry['winner'] == '':
                self._add_to_results(entry)


    def write_data_mongo(self):
        '''
        Escribe en mongo los resultados del proceso
        :return:
        '''
        self.mongo_wrapper.drop_collection('aggregated_results')
        self.mongo_wrapper.write_dictionaries_list('aggregated_results', self.results)

    def write_data_csv(self, filename):
        '''
        Exporta a csv los resultados del proceso
        :param filename:
        :return:
        '''
        import pandas as pd

        data = {}
        for column in self.results[0].keys():
            data[column] = []

        for result in self.results:
            for attribute_name in result.keys():
                data[attribute_name].append(result[attribute_name])

        repo = pd.DataFrame(data)
        repo.to_csv(filename)

    def _process_match(self, match):

        self.logger.debug('processing ' + str(match))

        home_stats = self.counters[match['home']]
        away_stats = self.counters[match['away']]

        entry = {}
        self._generate_tournament_positions()

        entry['score_competition_diff'] = self.counters[match['home']]['score_competition'] - self.counters[match['away']]['score_competition']
        entry['tournament_position_home'] = self.tournament_positions[match['home']]
        entry['tournament_position_away'] = self.tournament_positions[match['away']]

        entry['season'] = match['season']
        entry['day_num'] = match['day_num']

        entry['team_home'] = match['home']
        entry['team_away'] = match['away']

        entry['score_home'] = match['score_home']
        entry['score_away'] = match['score_away']

        #entry['ranking_home'] = 2 * entry['ranking_home']
        #entry['ranking_away'] = 2 * entry['ranking_away']
        entry['winner'] = self._winner(match)


        for key in home_stats.keys():
            entry[key + '_home'] = home_stats[key]

        for key in away_stats.keys():
            entry[key + '_away'] = away_stats[key]

        self._update_counters(match)

        return entry

    def _winner(self, match):
        if match['score_home'] == '':
            return ''
        if int(match['score_home']) > int(match['score_away']):
            return 'home'
        if int(match['score_home']) < int(match['score_away']):
            return 'away'
        if int(match['score_home']) == int(match['score_away']):
            return 'none'

    def _add_to_results(self, entry):
        self.results.append(entry)

    def _add_to_counter(self, team, key, qty):
        self.counters[team][key] += int(qty)

    def _set_counter(self, team, key, value):
        self.counters[team][key] = value

    def _init_counters(self, season):
        self.counters = {}
        self.teams_recent_history = {}

        for team in self._collection().find({'season': season}).distinct("home"):
            self.counters[team] = self.counter_template.copy()
            self.teams_recent_history[team] = {'goals': []}


    def _collection(self):
        return self.mongo_wrapper.get_collection(self.collection)
