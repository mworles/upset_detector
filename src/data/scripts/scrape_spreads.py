import scrapers
import Transfer

rows = scrapers.spreads.run()
Transfer.insert('spreads', rows, at_once=False)
