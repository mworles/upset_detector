from bs4 import BeautifulSoup
import requests
import datetime
import Transfer
import Scrapers



url = 'http://kenpom.com/index.php'
rows = Scrapers.team_ratings(url)

dbt = Transfer.DBTable('ratings', rows)
dbt.setup_table()

dba = Transfer.DBAssist()
dba.connect('../../aws.config')
dba.create_table(dbt)
#dba.insert_rows(dbt)

dba.close()
