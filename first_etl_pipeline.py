import sqlite3
import pandas as pd 
import requests
import json
from datetime import datetime
import datetime
import sqlalchemy
from sqlalchemy.orm import sessionmaker

DATABASE_LOCATION = "sqlite:///first_etl_pipeline.sqlite"

#creating a function to validate/transform the data

def check_if_valid_data(df: pd.DataFrame) -> bool:
    #is the data frame empty?
    if df.empty:
        print("No data downloaded, finishing exectuion!")
        return False
    
    #put a constraint for the primary key so there are not duplicates (team_name).
    if pd.Series(df['team_name']).is_unique:
        pass
    else:
        raise Exception("Team name check is violated!")
    
    #check for any null values
    if df.isnull().values.any():
        raise Exception("Null values found")
    
    return True
    
#Extracting the information from the api
if __name__ == "__main__":
    
    url = "https://api-football-v1.p.rapidapi.com/v3/standings"
    querystring = {"season":"2020","league":"39"}
    
    headers = {
        'x-rapidapi-key': "XXXXXXXXXX",
        'x-rapidapi-host': "api-football-v1.p.rapidapi.com"
    }
    
    response = requests.request("GET", url, headers=headers, params=querystring)

    data = response.text
    data = response.json()


    team_name = []
    points_total = []
    league = []
    wins = []
    draws = []
    loses = []

    for item in data['response']:
        for items in item['league']['standings']:
            for names in items:
                team_name.append(names['team']['name'])
                points_total.append(names['points'])
                league.append(names['group'])
                wins.append(names['all']['win'])
                draws.append(names['all']['draw'])
                loses.append(names['all']['lose'])
            
    league_dict = {
        "team_name" : team_name,
        "points_total" : points_total,
        "league_play" : league,
        "wins" : wins, 
        "draws" : draws,
        "loses" : loses
    }

    league_df = pd.DataFrame(league_dict, columns= ("team_name", "points_total", "league_play", "wins", "draws", "loses"))

    if check_if_valid_data(league_df):
        print("Data is valid, proceed to load stage!")

    
    #Load

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('first_etl_pipeline.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS first_etl_pipeline(
        team_name VARCHAR(200),
        points_total VARCHAR(200),
        league_play VARCHAR(200),
        wins VARCHAR(200),
        draws VARCHAR(200),
        loses VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (team_name) 
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully!")

    try:
        league_df.to_sql("first_etl_pipeline", engine, index=False, if_exists='append')
    except:
        print("Data already exists in database")
    
    conn.close()
    print("Close database success!")