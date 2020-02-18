from data import Scrapers
import time

links = Scrapers.box_links('2019/11/05')

for l in links[0:1]:
    print l
    time.sleep(1)
    result = Scrapers.game_box(l)

for r in result:
    print r
