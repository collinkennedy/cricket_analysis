
#establish database connection 
import mysql.connector
import pandas as pd
import numpy as np
import requests
import sys
import os
import json
from concurrent.futures import ProcessPoolExecutor
import logging
from functions import *


#create logging object
logging.basicConfig(
    stream = sys.stderr,
    format = '%(levelname)s: %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Set the logging level
logger.setLevel(logging.INFO)

logger.info("Successfully imported necessary libraries!")

#connect to MySQL database cricket_db
cnx = mysql.connector.connect(user='webmaster', password='Zelus2cool123',
                              database='cricket_db')

#set folder path
folder_path = "/Users/collinkennedy/Dropbox/R_dropbox/Zelus_Analytics/odis_json"

#put all the file paths in a list
file_paths = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path) if filename.endswith('.json')]


logger.info("setting directory path to: {}".format(folder_path))
for idx,file in enumerate(file_paths):

    logger.info("file no. {},  parsing/writing file {}...".format(idx, file))
    with open(file, 'r') as file:
        json_data = json.load(file)

    #get team and player information from the json
    team_df, player_df = parse_team_player_info(json_data = json_data)

    #write info to cricket_db
    write_players_to_cricket_db(player_df = player_df, cnx=cnx)
    write_teams_to_cricket_db(team_df=team_df, cnx = cnx)

    #get match information
    match_info_df = parse_match_info(json_data=json_data, cnx=cnx)

    #write match info to database
    write_match_info(match_info_df=match_info_df, cnx = cnx )

    #get innings information
    innings_df = parse_innings_info(json_data = json_data, match_info_df=match_info_df, cnx = cnx)

    #write to cricket_db
    write_innings_info(innings_info_df = innings_df, cnx=cnx)

logger.info("Successfully Completed Task")