""" match.

A module for matching alternate string versions of team school names to a 
single distinct numeric identifer used for each team. 

Classes
-------
TeamSource
    A container for a unique source of team name data. 

Functions
---------
run
    Return df with numeric ids and team names from each unique source.
    
"""
import re
import pandas as pd
from src.data import clean
from transfer import DBAssist
from src.constants import SOURCE_ID_YEARS

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
        self.team_id = map(lambda x: self.match_team(x, id_map, cutoff=cutoff),
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

    def match_team(self, team, id_map, cutoff=85):
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


def run():
    """
    Top-level function to create a key with numeric team ids and team names
    from each unique team name source.

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
    sources = make_sources()
    # for each source find the numeric id for each team
    sources = [source.find_ids(id) for source in sources]
    # make a df key for each source containing team names and numeric ids
    sources = [source.make_key() for source in sources]
    # combine all source keys into one 
    key = master_key(sources)
    
    return key


def make_sources():
    """
    Returns list of TeamSource instances prepared for finding team numeric
    identifers for each team in the source.

    Returns
    -------
    sources: list of TeamSource instances

    """ 
    dba = DBAssist()

    # kenpom team ratings
    raw = clean.s3_folder_data('team_ratings')
    source_kp = TeamSource('team_kp', raw, ['TeamName'])

    # sports reference season school stats .csv files
    raw = clean.s3_folder_data('cbb_summary')
    source_cbb = TeamSource('team_ss', raw, ['School'])

    # combine sources to list and assign processed names to clean attribute
    sources = [source_kp, source_cbb]
    sources = [source.clean_teams() for source in sources]

    dba.close()

    return sources


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
