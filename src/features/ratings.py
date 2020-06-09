import re
import pandas as pd
import numpy as np
from src.data import clean

def compile():
    ratings_files = clean.s3_folder_files('team_ratings')

    df_list = []
    for file in ratings_files:
        df = clean.s3_data(file)
        df.columns = map(str.lower, df.columns)
        if 'season' in df.columns:
            pass
        else:
            season = re.findall('\d+', file)[0]
            df['season'] = int('20{}'.format(season))
        df_list.append(df)

    ratings = pd.concat(df_list)
    ratings['em'] = np.where(ratings['em'].isnull(), ratings['adjem'],
                             ratings['em'])
    ratings['rankem'] = np.where(ratings['rankem'].isnull(), ratings['rankadjem'],
                                 ratings['rankem'])
    ratings = ratings.drop(columns=['adjem', 'rankadjem'])
    
    return ratings
