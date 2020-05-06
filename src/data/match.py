""" match.

A module for matching alternate versions of team school names to one distinct
numeric identifer for each team. Numeric identifers are used to merge data
from different external sources across the project. 

Classes
-------
TeamSource
    A container for a unique source of team name data. Attributes contain
    source's data, methods transform and create related objects.

Functions
---------
run
    Return df with numeric ids and team names from each unique source.
make_sources
    Return list of TeamSource instances prepared for finding numeric ids.
clean_camelcase
    Return camelcase team name with spacing between name stems.
master_key
    Return a team key df combining all team name sources.
id_from_name
    
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
    A container for a unique source of team name data.

    Attributes
    ----------
    label: str
        Label to assign the source in the team id key table.
    data: pandas DataFrame
        Source of data providing the team names.
    team_columns: list of str
        Names of all columns containing team names.
    unique: list of str
        List of unique teams names.
    clean: list of str
        Team names reformatted for optimal matching with master id file.
    team_id: list of numeric
        Unique numeric team identifers assigned to each team.
    key: pandas DataFrame
        Dataframe containing original unique team names and team id.
    """
    def __init__(self, label, data, team_columns):
        """Initialize TeamSource instance."""
        self.label = label
        self.data = data
        self.team_columns = team_columns
        # want single unique list with nulls removed before searching for id
        self.unique = self.unique_teams()
        # clean and team_id attributes set using methods below
        self.clean = None
        self.team_id = None
        self.key = None

    def unique_teams(self):
        """Return list of teams with nulls and duplicates removed."""
        raw = list(self.data[self.team_columns[0]])

        # if data contains team names in multiple columns
        if len(self.team_columns) > 1:
            raw += list(self.data[self.team_columns[1]])
        
        
        unique_teams = list(set(raw))
        
        # remove Nones, empty strings, and nans
        nulls = [None, '']
        valid_teams = [t for t in list(set(raw)) if t not in nulls]
        valid_teams = [x for x in valid_teams if not isinstance(x, float)]

        return valid_teams

    def clean_teams(self, teams=None):
        """
        Return instance with clean attribute containing reformatted team names
        for optimal matching with team id key.
        
        Parameters
        ----------
        teams: list of str, optional
            Teams can be input if additional processing needed on unique teams. 
        
        Returns
        -------
        self: instance of TeamSource
            Return self with clean attribute set.

        """
        if teams is None:
            teams = self.unique
        
        self.clean = map(lambda name: self.format_school(name), teams)

        return self

    def format_school(self, name_raw):
        """
        Return school name formatted for optimal matching with versions of
        team names used in the team id key.

        Parameters
        ----------
        name_raw: str
            The original team name from the source data

        Returns
        -------
        name_clean: str
            Team name reformatted to merge with team numeric id key.

        """
        # to match format used in team id file
        name_clean = str.lower(name_raw)
        name_clean = re.sub('[().&*\']', '', name_clean)
        name_clean = name_clean.rstrip()
        name_clean = name_clean.replace('  ', '-')
        name_clean = name_clean.replace(' ', '-')    
        
        return name_clean

    def find_ids(self, id_data, cutoff=85):
        """
        Return instance with team_id attribute containing the corresponding
        numeric team id for source teams.

        Parameters
        ----------
        id_data: pandas DataFrame
            Master id data containing 'name_spelling', 'team_id', and
            'lastd1season' columns.

        Returns
        -------
        self: TeamSource instance
            Return self with team_id attribute set.

        """
        min_year = SOURCE_ID_YEARS[self.label]
        id_map = self.create_id_map(id_data, min_year=min_year)
        self.team_id = map(lambda x: self.find_id(x, id_map, cutoff=cutoff),
                           self.clean)
        return self

    def create_id_map(self, df, min_year):
        """
        Return dict mapping unique team name versions from master key to 
        numeric identifers.

        Parameters
        ----------
        df: pandas DataFrame
            Master id data containing 'name_spelling', 'team_id', and
            'lastd1season' columns.
        min_year: int
            The lowest year available in the source data. Used to narrow the
            range of teams to find numeric identifers.

        Returns
        -------
        id_map: dict
            Dict mapping unique team name versions to numeric identifiers.

        """
        df = df[df['lastd1season'] >= min_year]
        df = df[['name_spelling', 'team_id']].set_index('name_spelling')
        id_map = df['team_id'].to_dict()
        
        return id_map

    def find_id(self, team, id_map, cutoff=85):
        """
        Return team numeric identifier from exact or fuzzy string match.

        Parameters
        ----------
        team: str
            Team name from data source reformatted for optimal matching.
        id_map: dict
            Dict mapping unique team name versions to numeric identifiers.

        Returns
        -------
        team_id: int or None
            Team numeric identifer found through exact match or fuzzy string
            match. If no exact match or fuzzy match does not meet numeric
            similarity cutoff, return None.

        """
        if team in id_map.keys():
            team_id = id_map[team]
        else:
            fuzz_match = clean.fuzzy_match(team, id_map.keys(), cutoff=cutoff)
            if fuzz_match is None:
                team_id = None
            else:
                team_id = id_map[fuzz_match]
    
        return team_id

    def make_key(self):
        """
        Create a pandas dataframe key from TeamSource instance. Columns 
        contain original unique team names (named after the source label)
        and numeric team identifiers.

        """
        key_data = {self.label: self.unique, 'team_id': self.team_id}
        df = pd.DataFrame(key_data)
        df = df[df['team_id'].notnull()]
        self.key = df
        return self


def run(data_dir=DATA_DIR):
    """
    Top-level function to create a key with numeric team ids and team names
    from each unique team name source.
    
    Parameters
    ----------
    data_dir: str
        Path to directory containing flat data files.

    Returns
    -------
    key: pandas DataFrame
        Contains a team numeric identifer and name, and a column
        containing the corresponding team name for each team name source.

    """
    dba = DBAssist()
    names = dba.return_data('team_spellings')
    teams = dba.return_data('teams')
    dba.close()

    id = pd.merge(names, teams, how='inner', left_on='team_id',
                  right_on='team_id')

    # process for optimal matching with other team sources
    id['name_spelling'] = id['name_spelling'].str.replace(' ', '-')
    id['name_spelling'] = id['name_spelling'].str.replace('.', '')
    id = id.drop_duplicates()
    # keep only columns needed for matching
    id = id.loc[:, ['team_id', 'team_name', 'name_spelling', 'lastd1season']]
    
    # initialize TeamSource instance for each of the sources
    sources = make_sources(data_dir)
    # for each source find the numeric id for each team
    sources = [source.find_ids(id) for source in sources]
    # make a df key for each source containing team names and numeric ids
    sources = [source.make_key() for source in sources]
    # combine all source keys into one 
    key = master_key(sources)
    
    return key


def make_sources(data_dir=DATA_DIR):
    """
    Returns list of TeamSource instances prepared for finding team numeric
    identifers for each team in the source.

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

    # combine sources to list and assign processed names to clean attribute
    sources = [source_cbb, source_kp, source_pt, source_op, source_tcp,
               source_vi_odds, source_vi_spreads]
    sources = [source.clean_teams() for source in sources]

    # sportsbookreviews .csv files
    raw = pd.read_csv(DATA_DIR + 'external/sbro/spreads_sbro.csv')
    source_sbro = TeamSource('team_sbro', raw, ['home', 'away'])
    # additional processing step to handle camelcase strings
    teams_spaced = map(clean_camelcase, source_sbro.unique)
    source_sbro = source_sbro.clean_teams(teams_spaced)

    sources.append(source_sbro)
    
    dba.close()
    
    return sources


def clean_camelcase(team):
    """
    Return camelcase team name with spacing between name stems.

    Parameters
    ----------
    team: str
        The original team name from the source data.

    Returns
    -------
    result: str
        Team name with spaces inserted according to camelcase.

    """
    # all caps names are commonly used acronyms, keep original string
    if team.isupper():
        result = team

    else:
        # elements to find are 1+ consecutive upper not followed by lower
        # or single upper followed by 1 or more lower
        regex = '[A-Z&]+(?![a-z])|[A-Z][a-z\']+'
        regex_result = re.findall(regex, team)

        # if len=0 string is all lower, keep raw string
        # can still attempt fuzzy match
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
    sources: list of TeamSource instances
        Team sources with team_id attributes assigned from team names.

    Returns
    -------
    key: pandas DataFrame
        Contains a team numeric identifer and name, and a column
        containing the corresponding team name for each team name source.

    """
    # read in master id file
    dba = DBAssist()
    key = dba.return_data('teams')
    dba.close()

    key = key[['team_id', 'team_name']]

    # create merged key containing all unique sources
    for source in sources:
        key = pd.merge(key, source.key, on='team_id', how='left')
    
    # assign empty strings to keep string data type consistent
    key = key.fillna('')
    key = key.drop_duplicates()
    key = key.sort_values('team_id')
    
    return key


def ids_from_names(names, key_col):
    """From input data containing team name column specified in 'name_col', 
    returns dataframe containing team numeric identifiers.

    Arguments
    ----------
    names: list of str
        Team names from an external data source.
    key_col: string
        Name of the source label used as column in team key table.

    Returns
    -------
    team_ids: list of int
        Elements are team id obtained from team key or None for each team name.
 
    """ 
    # read in the id key data
    dba = DBAssist()
    df = dba.return_data('team_key')
    dba.close()

    # from id key data, only need numeric identifer and key_col to merge on
    df['team_id'] = df['team_id'].astype(int)
    id = df[['team_id', key_col]].drop_duplicates().copy()
    id = id.set_index(key_col)
    id_map = id['team_id'].to_dict()

    team_ids = map(lambda team: id_from_name(team, id_map), names)
    
    return team_ids

def id_from_name(name, id_map):
    """Return team id mapped to name or None if not found."""
    try:
        team_id = id_map[name]
    except KeyError:
        team_id = None
    
    return team_id
