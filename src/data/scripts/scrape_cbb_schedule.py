import time
from src.data.scrapers import cbb
from src.data.transfer import DBAssist


dba = DBAssist()
dba.create_from_schema('cbb_schedule')
dba.create_from_schema('cbb_schedule_error')

for season in range(1993, 2003):
    links = cbb.srcbb_schedule_links(season)

    for url in links[0:5]:
        time.sleep(1)
        sched = cbb.get_team_schedule(url, season)

        if len(sched) > 2:
            dba.insert_rows('cbb_schedule', sched)
        else:
            dba.insert_rows('cbb_schedule_error', sched)

dba.close()
