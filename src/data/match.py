""" match.

A module for matching alternate versions of team school names to one distinct
numeric identifer for each team. Numeric identifers are used to merge data
from disparate sources across the project. 

"""
import pandas as pd
import numpy as np
import clean
from src.data.transfer import DBAssist
from src.constants import DATA_DIR
from src.constants import SOURCE_ID_YEARS
import re

class TeamSource():
    """
    A container for unique sources of team name data.

    Attributes
    ----------
    label: str
        Label to assign the source in the team name key table.
    data: pandas DataFrame
        Source of data providing the team names.
    team_columns: list of str
        Names of all columns containing team names.
    unique: list of str
        All unique team names with nulls removed.
    clean: list of str
        Team names reformatted for optimal matching with master id file.
    team_id : list of numeric
        Unique numeric team identifers.

    """
    def __init__(self, label, data, team_columns):
        """Initialize TeamSource instance."""
        self.label = label
        self.data = data
        self.team_columns = team_columns
        # want single unique list with nulls removed before searching for id
        self.unique = self.unique_teams()
        # clean created separately in case of additional pre-processing
        self.clean = None
        self.team_id = None

    def unique_teams(self):
        """Return list of teams with nulls and duplicates removed."""
        raw = list(self.data[self.team_columns[0]])

        if len(self.team_columns) == 2:
            raw = raw + list(self.data[self.team_columns[1]])

        unique_teams = list(set(raw))
        nulls = [None, '']
        valid_teams = [t for t in list(set(raw)) if t not in nulls]
        # remove nans by removing any floats
        valid_teams = [x for x in valid_teams if not isinstance(x, float)]

        return valid_teams

    def clean_teams(self, teams=None):
        """
        Return instance with teams assigned.
        
        Parameters
        ----------
        teams : list of str, optional
            Option to input teams in case of additional preprocessing after
            creating instance.
        
        Returns
        -------
        self : instance of TeamSource
            Return self with clean attribute set.

        """
        if teams is None:
            teams = self.unique
        
        
        self.clean = map(lambda name: self.format_school(name), teams)

        return self

    def format_school(self, name_raw):
        """
        Return school name formatted to match the version used in the id key.

        Parameters
        ----------
        name_raw : str
            The original team name from the source data

        Returns
        -------
        name_clean : str
            Team name reformatted to merge with team numeric id key.

        """
        # clean string
        name_clean = str.lower(name_raw)
        name_clean = re.sub('[().&*\']', '', name_clean)
        name_clean = name_clean.rstrip()
        # replace spaces with hyphens to match format used in team id file
        name_clean = name_clean.replace('  ', '-')
        name_clean = name_clean.replace(' ', '-')    
        
        return name_clean

    def find_ids(self, id_data):
        """
        Return school name formatted to match the version used in the id key.

        Parameters
        ----------
        id_data : pandas DataFrame
            Master id data containing 'name_spelling', 'team_id', and
            'lastd1season' columns.

        Returns
        -------
        self : TeamSource instance
            Return self with team_id attribute set.

        """
        min_year = SOURCE_ID_YEARS[self.label]
        id_map = self.create_id_map(id_data, min_year=min_year)
        self.team_id = map(lambda team: self.find_id(team, id_map), self.clean)
        return self

    def create_id_map(self, df, min_year):
        """
        Return dict mapping unique team spellings to team numeric identifer
        from master id data.

        Parameters
        ----------
        df : pandas DataFrame
            Master id data containing 'name_spelling', 'team_id', and
            'lastd1season' columns.
        min_year : int
            The lowest year available in the source data. Used to narrow the
            range of teams to find numeric identifers.

        Returns
        -------
        id_map : dict
            Dict mapping unique team name versions to numeric identifiers.

        """
        df = df[df['lastd1season'] >= min_year]
        df = df[['name_spelling', 'team_id']].set_index('name_spelling')
        id_map = df['team_id'].to_dict()
        
        return id_map

    def find_id(self, team, id_map):
        """
        Return team numeric identifier from exact or fuzzy string match.

        Parameters
        ----------
        team : str
            Team name from data source reformatted for optimal matching.
        id_map : dict
            Dict mapping unique team name versions to numeric identifiers.

        Returns
        -------
        team_id : int or None
            Team numeric identifer found through exact match or fuzzy string
            match. None if no exact match or fuzzy match meeting a numeric
            similarity cutoff can be found.

        """
        if team in id_map.keys():
            team_id = id_map[team]
        else:
            fuzz_match = clean.fuzzy_match(team, id_map.keys(), cutoff=85)
            if fuzz_match is None:
                team_id = None
            else:
                team_id = id_map[fuzz_match]
    
        return team_id

    def make_key(self):
        """
        Create a pandas dataframe key from TeamSource instance. Columns are
        the source label with original unique team names and 'team_id' with 
        numeric team identifiers.
        
        """
        key_data = {self.label: self.unique, 'team_id': self.team_id}
        df = pd.DataFrame(key_data)
        df = df[df['team_id'].notnull()]
        self.key = df
        return self


def run(data_dir=DATA_DIR):
    dba = DBAssist()
    names = dba.return_data('team_spellings')
    teams = dba.return_data('teams')
    dba.close()

    id = pd.merge(names, teams, how='inner', left_on='team_id',
                  right_on='team_id')
    
    id['name_spelling'] = id['name_spelling'].str.replace(' ', '-')
    id['name_spelling'] = id['name_spelling'].str.replace('.', '')
    id = id.drop_duplicates()
    id = id.loc[:, ['team_id', 'team_name', 'name_spelling', 'lastd1season']]

    sources = make_sources(data_dir)
    sources = [source.find_ids(id) for source in sources]
    sources = [source.make_key() for source in sources]

    key = master_key(sources)
    
    return key


def make_sources(data_dir=DATA_DIR):
    """
    Returns list of TeamSource instances with clean attributes set for
    finding team numeric identifers.

    Parameters
    ----------
    data_dir: string
        Path to directory containing flat data files.

    Returns
    -------
    sources: list of TeamSource instances

    """ 
    dba = DBAssist()

    # sports reference season school stats .csv files
    raw = clean.combine_files(DATA_DIR + 'external/school_stats/')
    source_cbb = TeamSource('team_ss', raw, ['School'])

    # kenpom team ratings .csv files
    raw = clean.combine_files(DATA_DIR + 'external/kp/')
    source_kp = TeamSource('team_kp', raw, ['TeamName'])

    # predictiontracker .csv files
    raw = clean.combine_files(DATA_DIR + 'external/pt/')
    source_pt = TeamSource('team_pt', raw, ['home', 'road'])

    # table from oddsportal scraper 
    raw = dba.return_data('oddsportal')
    source_op = TeamSource('team_oddsport', raw, ['team_1', 'team_2'])

    # table from tcpalm box score scraper
    raw = dba.return_data('game_scores')
    source_tcp = TeamSource('team_tcp', raw, ['home_team', 'away_team'])

    # table from scraped vegasinsider odds
    raw = dba.return_data('odds')
    source_vi_odds = TeamSource('team_vi_odds', raw, ['team_1', 'team_2'])

    # table from scraped vegasinsider spreads
    raw = dba.return_data('spreads')
    source_vi_spreads = TeamSource('team_vi_spreads', raw, ['team_1', 'team_2'])

    sources = [source_cbb, source_kp, source_pt, source_op, source_tcp,
               source_vi_odds, source_vi_spreads]

    sources = [source.clean_teams() for source in sources]

    # clean and match sportsbookreviews teams
    raw = pd.read_csv(DATA_DIR + 'external/sbro/spreads_sbro.csv')
    source_sbro = TeamSource('team_sbro', raw, ['home', 'away'])
    # create spaced names from camelcase strings
    teams_spaced = map(clean_camel, source_sbro.unique)
    source_sbro = source_sbro.clean_teams(teams_spaced)

    sources.append(source_sbro)
    
    dba.close()
    
    return sources


def clean_camel(team):
    """
    Return camelcase team name parsed using regex.

    Parameters
    ----------
    team : str
        The original team name from the source data.

    Returns
    -------
    result : str
        Team name with spaces inserted according to camelcase.

    """
    # all caps names are commonly used acronyms 
    if team.isupper():
        result = team

    else:
        # elements to find are 1+ consecutive upper not followed by lower
        # or single upper followed by 1 or more lower
        regex = '[A-Z&]+(?![a-z])|[A-Z][a-z\']+'
        regex_result = re.findall(regex, team)
        
        # if len=0 string is all lower, keep raw string
        if len(regex_result) == 0:
            result = team
        else:
            result = " ".join(regex_result)

    return result


def master_key(sources):
    """
    Return a master team key dataframe from combining all team name sources.

    Parameters
    ----------
    sources : list of TeamSource instances
        Team sources that have team_id attributes from finding numeric
        identifiers to match the source team names.

    Returns
    -------
    key : pandas DataFrame
        Contains a master team numeric identifer and name, and a column
        containing the corresponding team name from each source.

    """
    # read in master id file
    dba = DBAssist()
    key = dba.return_data('teams')
    dba.close()

    key = key[['team_id', 'team_name']]

    # create universal key
    for source in sources:
        key = pd.merge(key, source.key, on='team_id', how='left')
    key = key.fillna('')
    key = key.drop_duplicates()
    key = key.sort_values('team_id')
    
    return key


def id_from_name(df, key_col, name_col, drop=True, how='inner'):
    """From input data containing team name column specified in 'name_col', 
    returns dataframe containing team numeric identifiers.

    Arguments
    ----------
    datdir: string
        Relative path to data directory.
    df: pandas dataframe
        Data input to add team numeric identifier as a column.
    key_col: string
        The name of column in id key file to match team name.
    name_col: string
        The name of team name column in the input df.
    """ 
    # read in the id key data
    dba = DBAssist()
    id = dba.return_data('team_key')
    dba.close()

    # from id key data, only need numeric identifer and key_col to merge on
    id = id[['team_id', key_col]]
    id['team_id'] = id['team_id'].astype(int)
    id_name = name_col + '_id'
    id = id.rename(columns={'team_id': id_name})
    
    # remove duplicates
    id = id[~id.duplicated()]
    # join data the id key file using specified inputs
    mrg = pd.merge(df, id, left_on=name_col, right_on=key_col, how=how)

    # list of cols to drop, key_col is redundant with name_col
    drop_cols = [key_col]
    # add name_col to drop list, if desired
    if drop == True:
        drop_cols.append(name_col)
    # remove columns from dataframe
    mrg = mrg.drop(drop_cols, axis=1)

    return mrg
