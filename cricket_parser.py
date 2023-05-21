
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
import shutil
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

#scrape the data and store it in odis_json folder:
# Get the file from the URL
response = requests.get("https://cricsheet.org/downloads/odis_json.zip")


current_directory = os.getcwd() #the directory where the script is being run. store the downloaded json here for now

# Check if the folder exists
if os.path.exists("odis_json"):
    # Delete the folder and all of its contents
    shutil.rmtree("odis_json")

# Create a folder to store the files
os.mkdir("odis_json")

# Write the file to the folder
with open("odis_json/odis_json.zip", "wb") as f:
    f.write(response.content)

# Unzip the file
os.system("unzip odis_json/odis_json.zip -d odis_json")

#connect to MySQL database cricket_db
cnx = mysql.connector.connect(user='webmaster', password='Zelus2cool123',
                              database='cricket_db')

#clear contents
clear_contents(cnx = cnx)

#set folder path
folder_path = current_directory + "/odis_json"
logging.info("Folder Path: {}".format(folder_path))

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


logger.info("Running Query to answer Q2a: ")
cursor = cnx.cursor()

query1 = """ WITH temp AS(
(SELECT
	season,
	gender,
	team1 as team,
	team1_id as team_id,
	winner
FROM match_info mi)
UNION ALL
(SELECT 
	season,
	gender,
	team2 as team,
	team2_id as team_id,
	winner 
FROM match_info))


SELECT
	season,
	gender,
	team,
	SUM(CASE WHEN team = winner THEN 1 ELSE 0 END) as num_wins,
	COUNT(*) as total_games_played_excluding,
	SUM(CASE WHEN team = winner THEN 1 ELSE 0 END)*1.0/COUNT(*)*1.0 as win_percentage
FROM temp
GROUP BY 1,2,3
ORDER BY season, gender, team"""

cursor.execute(query1)
result1 = cursor.fetchall()



logger.info("Q2a Answer (as a pandas dataframe): ")
#insert dataframe here
df = pd.DataFrame(result1)
df.columns = ['season', 'gender', 'team', 'num_wins', 'total_games_played_excluding', 'win_percentage']
print(df)

logger.info("Running Query for Q2b...")
cursor = cnx.cursor()

query2 = """ WITH temp AS(
(SELECT
	season,
	gender,
	team1 as team,
	team1_id as team_id,
	winner
FROM match_info mi)
UNION ALL
(SELECT 
	season,
	gender,
	team2 as team,
	team2_id as team_id,
	winner 
FROM match_info)),

win_pct AS(
SELECT
	season,
	gender,
	team,
	SUM(CASE WHEN team = winner THEN 1 ELSE 0 END)*1.0/COUNT(*)*1.0 as win_percentage,
	ROW_NUMBER() OVER(PARTITION BY gender ORDER BY SUM(CASE WHEN team = winner THEN 1 ELSE 0 END)*1.0/COUNT(*)*1.0 DESC) as ranking
FROM temp
WHERE season = '2019' -- not sure how seasons work in cricket, i have 2019 alone and 2018/19 and 2019/20...
GROUP BY 1,2,3) 

SELECT 
	season,
	gender,
	team as team_with_best_win_pctg
FROM win_pct
WHERE ranking = 1 """

cursor.execute(query2)
result2 = cursor.fetchall()

logger.info("Answer to Q2b (as a pandas dataframe): ")

#insert dataframe here
df2 = pd.DataFrame(result2)
columns = ['year','gender','team']
df2.columns = columns
print(df2)


logger.info("Running Query for Q2c...")

query3 = """ WITH temp AS(
SELECT 
	season,
	batter,
	COUNT(innings_id) as times_bowled_to, -- each innings_id should represent a pitch/delivery
	SUM(batter_runs) as runs_from_batting, -- extra runs are a separate column/FIELD
	(SUM(batter_runs)*1.0/COUNT(innings_id)*1.0) as strike_rate,
	ROW_NUMBER() OVER(ORDER BY SUM(batter_runs)*1.0/COUNT(innings_id)*1.0 DESC ) as ranking
FROM innings_info 
LEFT JOIN match_info ON innings_info.match_id = match_info.match_id 
WHERE season = '2019'
GROUP BY season, batter)

SELECT 
	batter
FROM temp
WHERE ranking = 1"""

cursor.execute(query3)
result3 = cursor.fetchall()


logger.info("Answer to Q2c (as a pandas dataframe): ")
df3 = pd.DataFrame(result3)
columns = ['batter_with_highest_strikerate_2019']
df3.columns = columns
print(df3)

