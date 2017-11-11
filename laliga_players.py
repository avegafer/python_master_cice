from scraping.laliga import utils
from scraping.laliga.PlayersScraper import PlayersScraper
from multiprocessing import Pool



# las temporadas a procesar
seasons = utils.build_seasons('primera', 1928) + utils.build_seasons('segunda', 2016)


def scrape_season(season):

    scraper = PlayersScraper()
    scraper.scrape_season(season)


print('BEGIN PROCESS')

#writer = utils.create_mongo_writer()
#writer.reset()

if __name__ == '__main__':


    p = Pool(5)
    p.map(scrape_season, seasons)

print('END PROCESS')
