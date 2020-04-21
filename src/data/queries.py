# get ratings for teams in game_info
ratings_t1 =    """SELECT *
                FROM ratings_at_day as r
                INNER JOIN game_info AS g
                ON r.team_id = g.t1_team_id AND r.date = g.date;
                """

ratings_t2 =    """SELECT *
                FROM ratings_at_day as r
                INNER JOIN game_info AS g
                ON r.team_id = g.t2_team_id AND r.date = g.date;
                """

info_spreads =  """SELECT s.game_id, v.t1_spread, v.over_under,
                from game_info AS g
                RIGHT JOIN spreads_clean AS s
                USING game_id
                """
