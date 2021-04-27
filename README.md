A simple beginner ETL Pipeline made in python:

This is my first project I did on my own using python. I wanted to extract data from from a sports api to give me the current standings in the Premire League and 
store it to a database. I used the request library to make a call to the api to get the current standings. I then converted the information into a JSON format 
and put in some validation steps to make sure I was getting clean data. In the next step, I created a loop that would pull out some basic information 
(wins, losses, draws, league, points total) and append the information to empty lists. I then converted those lists into a dictionary that I converted 
into a Pandas dataframe. Last, I connected a sqlite database to put the Pandas dataframe into the sqlite database.
