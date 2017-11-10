from bs4 import BeautifulSoup

from scraping.core import scrape_request



class PopUpScraper:
    def __init__(self, match_id,  writer):
        self.match_id = match_id
        self.sender = scrape_request.Sender()
        self.sender.set_debug_level(0)
        self.sender.set_delay(1)
        self.writer = writer

    def scrape_popup(self, params):

        sender = self.sender

        response = sender.post('http://www.laliga.es/includes/ajax/abrir_partido_historico.php', params)

        if response:
            self.process_popup(response)

    def extract_header(self, html):
        title_container = html.find('div', {'id': 'contenedor_titulo'})
        cell_contents = []
        for cell in title_container.find_all('div'):
            cell_contents.append(cell.getText())

        return {
            'league_numday': cell_contents[0],
            'stadium': cell_contents[1],
            'day': cell_contents[2]
        }

    def process_popup(self, popup_content):
        html = BeautifulSoup(popup_content, 'html.parser')

        header = self.extract_header(html)

        result_info_container = html.find('div', {'id': 'contenedor_resultado'})
        result_info = self.popup_get_result_info(result_info_container)


        scores_info_container = html.find('div', {'id': 'contenedor_goles'})
        scores_info = self.popup_get_scorers(scores_info_container, result_info['team1'], result_info['team2'])

        players_data_container = html.find('div', {'id': 'contenedor_jugadores'})
        results = []

        main_counter = 0
        for team_details_container in players_data_container.find_all('div'):

            if (main_counter % 2) == 0:
                team = result_info['team1']
            else:
                team = result_info['team2']

            row_counter = 1
            for row in team_details_container.find_all('tr')[:]:

                player = ''
                stat = ''
                counter = 0

                if (row_counter <= 11):
                    main_lineup = True
                else:
                    main_lineup = False

                for cell in row.find_all('td')[:]:

                    if (counter % 2) == 0:
                        player = cell.getText()
                    else:
                        stat = cell.getText()

                    if player and stat:
                        results.append({'match_id': self.match_id, 'player': player, 'stats': stat, 'team': team, 'main_lineup': main_lineup})

                    counter = counter + 1
                row_counter = row_counter + 1
            main_counter = main_counter + 1


        self.writer.write_dictionaries_list('popups_matches_stats', results)
        self.writer.write_dictionaries_list('popups_matches_scores', scores_info)

        match_data = {
            'match_id': self.match_id,
            'coach_home': result_info['coach1'],
            'coach_away': result_info['coach2'],
            'stadium': header['stadium']
        }

        self.writer.write_dictionary('matches_data', match_data)

    def popup_get_result_info(self, result_info_container):
        data = []
        for item in result_info_container.find_all('span'):
            data.append(item.getText())

        return {
            'team1': data[0]
            , 'score1': data[1]
            , 'coach1': data[2]
            , 'team2': data[3]
            , 'score2': data[4]
            , 'coach2': data[5]
        }

    def popup_get_scorers(self, scores_info_container, team_1_name, team_2_name):
        is_first = True

        scorers_1 = []
        scorers_2 = []

        for table in scores_info_container.find_all('table')[:]:
            if is_first:
                scorers_1 = self.popup_get_one_team_scorers(table, team_1_name)
                is_first = False
            else:
                scorers_2 = self.popup_get_one_team_scorers(table, team_2_name)

        return scorers_1 + scorers_2

    def popup_get_one_team_scorers(self, table, team_name):

        result = []

        for row in table.find_all('tr'):
            counter = 0
            entry = {'match_id': self.match_id, 'team_name': team_name}

            for cell in row.find_all('td'):
                if (counter % 2) == 0:
                    entry['player'] = cell.getText()
                else:
                    entry['content'] = cell.getText()

                counter = counter + 1

            result.append(entry)

        return result
