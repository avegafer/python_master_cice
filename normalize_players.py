from scraping.aggregations.PlayerNormalizer import PlayerNormalizer

normalizer = PlayerNormalizer()
result = normalizer.normalize()
normalizer.save_csv(result)
