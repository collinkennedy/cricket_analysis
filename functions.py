import pandas as pd
import numpy as np
import json
import sys
import os
import mysql.connector


###############################################
def parse_team_player_info(json_data: dict):
    """
    takes json data as

    Parameters:
    --------------
    - json_data (dict) : json object as a dictionary


    Returns:
    -------------
    - player_df (pandas DataFrame): 
    
    
    """
    player_columns = [
        "team",
        "player_name",
        "registryID"
    ]

    temp_people = json_data['info']['registry']['people']
    temp_teams = json_data['info']['teams']
    
    player_df = pd.DataFrame(columns = player_columns)
    for team in temp_teams:
        #for each team, get the players on that team, for each player, get their registry id as well

        for player in json_data['info']['players'][team]: 

            registryId = temp_people[player]

            # print("team: {}, player: {}, registryId: {} ".format(team,player, registryId))
            
            temp_df = pd.DataFrame({"team": [team], "player_name": [player], "registryID": [registryId]})

            player_df = pd.concat([player_df,temp_df], axis = 0)
        
    team_df = player_df[['team']].drop_duplicates() #drop duplicates
    player_df = player_df[['player_name','registryID']]
    return team_df, player_df






def write_players_to_cricket_db(player_df: pd.DataFrame, cnx: mysql.connector.connection_cext.CMySQLConnection):
    """Writes player data to players table in cricket_db"""
    table_name = 'players'
    columns = ', '.join(player_df.columns)
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s'] * len(player_df.columns))})"

    # Execute the INSERT statement for each row
    cursor = cnx.cursor()
    for row in player_df.itertuples(index=False):
        try:
            cursor.execute(sql, row)
            cnx.commit()
            # print(f"1 row inserted successfully into {table_name} table!")
        except mysql.connector.IntegrityError:
            # print("Duplicate! Skipping...")
            pass

    cursor.close()




########################################################
def write_teams_to_cricket_db(team_df: pd.DataFrame, cnx: mysql.connector.connection_cext.CMySQLConnection):
    """Write unique teams to teams table in cricket_db"""
    table_name = 'teams'
    columns = ', '.join(team_df.columns)
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s'] * len(team_df.columns))})"

    # Execute the INSERT statement for each row
    cursor = cnx.cursor()
    for row in team_df.itertuples(index=False):
        try:
            cursor.execute(sql, row)
            cnx.commit()
            # print(f"1 row inserted successfully into {table_name} table!")
        except mysql.connector.IntegrityError:
            # print("Duplicate! Skipping...")
            pass

    cursor.close()



################################################
def parse_match_info(json_data: dict, cnx: mysql.connector.connection_cext.CMySQLConnection):

    """ parse that match info from the json and return a named dataframe """

    #balls per over
    balls_per_over = json_data['info']['balls_per_over']

    #city
    try:
        city = json_data['info']['city']
    except KeyError:
        city = "no city field"

    #date start
    date_start = json_data['info']['dates'][0]

    #date end
    date_end = json_data['info']['dates'][-1] #latest date

    #match name
    try:
        match_name = json_data['info']['event']['name']
    except:
        match_name = None

    #match number
    try:
        match_number = json_data['info']['event']['match_number']
    except KeyError:
        match_number = None

    #gender
    gender = json_data['info']['gender']

    #match type
    match_type = json_data['info']['match_type']

    #match_type_number
    match_type_number = json_data['info']['match_type_number']

    #offical_data
    try:
        official_data = json_data['info']['officials'] #just store as json for now
    except KeyError:
        official_data = None

    #winner
    try:
        winner = json_data['info']['outcome']['winner']
    except:
        winner = json_data['info']['outcome']['result'] #this will happen in the case of a tie, for example


    #decision_by
    try:
        decision_by = list(json_data['info']['outcome']['by'].keys())[0]
    except:
        decision_by = json_data['info']['outcome']['result']

    #overs
    overs = json_data['info']['overs']

    #player_of_match
    try:
        player_of_match = json_data['info']['player_of_match'][0]
    except KeyError:
        player_of_match = "no PoM field"

    #team1
    team1 = list(json_data['info']['players'].keys())[0]

    print("team1", team1)

    #team1_players
    team1_players = json_data['info']['players'][team1]

    #team2
    team2 = list(json_data['info']['players'].keys())[1]
    print("team2: ", team2)


    #team2_players
    team2_players = json_data['info']['players'][team2]

    #season
    season = json_data['info']['season']

    #team_type
    team_type = json_data['info']['team_type']

    #toss_decision
    toss_decision = json_data['info']['toss']['decision']

    #toss_winner
    toss_winner = json_data['info']['toss']['winner']

    #venue
    venue = json_data['info']['venue']

    

    #get the team1 and team2 id— team data is always written first so this should always exist before writing the match info data
    cursor = cnx.cursor()
    cursor.execute("SELECT team_id FROM cricket_db.teams WHERE team = '{}'".format(team1))
    result = cursor.fetchall()  # Fetch any remaining results
    cursor.close()
    print(result)
    team1_id = result[0][0]

    cursor = cnx.cursor()
    cursor.execute("SELECT team_id FROM cricket_db.teams WHERE team = '{}'".format(team2))
    result = cursor.fetchall()  # Fetch any remaining results
    cursor.close()
    team2_id = result[0][0]


    # match_info_df = pd.DataFrame(columns = match_info_columns)

    match_info_df = pd.DataFrame({
        "balls_per_over": [balls_per_over],
        "city": [city],
        "date_start": [date_start],
        "date_end" : [date_end],
        "match_name": [match_name],
        "match_number" : [match_number],
        "gender" : [gender],
        "match_type": [match_type],
        "match_type_number" : [match_type_number],
        "official_data" :[json.dumps(official_data)],
        "winner": [winner],
        "decision_by" : [decision_by],
        "overs": [overs],
        "player_of_match" : [player_of_match],
        "team1" : [team1],
        "team1_id": [team1_id],
        "team1_players": [json.dumps(team1_players)],
        "team2": [team2],
        "team2_id": [team2_id],
        "team2_players": [json.dumps(team2_players)],
        "season": [season],
        "team_type" : [team_type],
        "toss_decision" : [toss_decision],
        "toss_winner": [toss_winner],
        "venue": [venue]

    })
    return match_info_df



######################################################
def write_match_info(match_info_df: pd.DataFrame, cnx: mysql.connector.connection_cext.CMySQLConnection):
    match_info_tuples = match_info_df.values.tolist()

    #write to database
    table_name = 'match_info'
    columns = ', '.join(match_info_df.columns)
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s'] * len(match_info_df.columns))})"

    # Execute the INSERT statement for each row
    cursor = cnx.cursor()
    for row in match_info_tuples:
        try:
            cursor.execute(sql, row)
            cnx.commit()
            # print(f"1 row inserted successfully into {table_name} table!")
        except mysql.connector.IntegrityError:
            # print("Duplicate! Skipping...")
            pass

    cursor.close()
    

def parse_innings_info(json_data: dict, match_info_df: pd.DataFrame, cnx: mysql.connector.connection_cext.CMySQLConnection):

    # get distinct match_id from the match_info table
    cursor = cnx.cursor()
    query = """
    SELECT MAX(match_id) -- this will be themost recent match_id
    FROM match_info 
    """

    cursor.execute(query)
    match_id = cursor.fetchall()[0][0]
    print("cursor.fetchall():", match_id)
     #this is the match_id that needs to be written to the innings table
    cursor.close()

    #get the team ids by querying the teams table
    team_lst = []
    over_lst = []
    batter_lst = []
    bowler_lst = []
    non_striker_lst = []
    batter_runs_lst = []
    extra_runs_lst = [] 
    extras_lst = []
    wicket_lst = []
    wicket_info_lst = []

    #add match_id and match_number to the dataframe at the end



    #loop through all the overs for both teams #TODO 3 for loops is disgusting
    for i in range(len(json_data['innings'])): #len is pretty much always 2
        #i will correspond to the team: 0 is team 1, 1 is team 2
        for over_idx, over in enumerate(json_data['innings'][i]['overs']):

            for delivery in over['deliveries']:

                team_lst.append(json_data['innings'][i]['team']) 
            
                over_lst.append(over['over']) #the over
                
                batter_lst.append(delivery['batter']) #get batter

                bowler_lst.append(delivery['bowler']) #get bowler

                # extras_lst.append(delivery['extras']) #the extras list
                
                non_striker_lst.append(delivery['non_striker']) #get non_striker

                
                batter_runs_lst.append(delivery['runs']['batter'])
                extra_runs_lst.append(delivery['runs']['extras'])

                try:
                    wicket_lst.append(json.dumps(delivery['wickets'][0]))
                except:
                    wicket_lst.append(None)
            
      
    innings_df = pd.DataFrame({
    "team": team_lst,
    "over_num": over_lst,
    "batter": batter_lst,
    "bowler": bowler_lst,
    "non_striker": non_striker_lst,
    "batter_runs": batter_runs_lst,
    "extra_runs": extra_runs_lst,
    "wickets": wicket_lst
    # "wicket_info": wicket_info_lst
    })
    innings_df['match_id'] = match_id


    return innings_df


########################################################
def write_innings_info(innings_info_df: pd.DataFrame, cnx: mysql.connector.connection_cext.CMySQLConnection):
    innings_info_tuples = innings_info_df.values.tolist()

    #write to database
    table_name = 'innings_info'
    columns = ', '.join(innings_info_df.columns)
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s'] * len(innings_info_df.columns))})"

    # Execute the INSERT statement for each row
    cursor = cnx.cursor()
    for row in innings_info_tuples:
        try:
            cursor.execute(sql, row)
            # print(f"1 row inserted successfully into {table_name} table!")
        except mysql.connector.IntegrityError:
            # print("Duplicate Data! Skipping...")
            pass

    cnx.commit()
    cursor.close()

def clear_contents(cnx: mysql.connector.connection_cext.CMySQLConnection ):

    tables = ['players','teams','match_info','innings_info']
    for table in tables:
        cursor = cnx.cursor()

        query = """ 
        SET FOREIGN_KEY_CHECKS = 0; """

        cursor.execute(query)

        print("Foreign key checks disabled")

        # Truncate the players table
        query = """ 
        TRUNCATE TABLE {}""".format(table)

        cursor.execute(query)

        print(cursor.rowcount, "rows deleted")

        # Enable foreign key checks again
        query = """ 
        SET FOREIGN_KEY_CHECKS = 1; """

        cursor.execute(query)

        print("Foreign key checks enabled")
