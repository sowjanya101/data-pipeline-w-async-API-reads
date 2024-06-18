****# pre-requisites
- python version of python 3.11 or above 
- spark installation, and environmental path variables set 
- "make" is installed on windows. I used chocolatey to intall make for windows

# Folder structure 
main_folder/
    star_wars/ -> package 
        main.py -> main function 
        requirements.txt 
    Makefile

# steps to execute the code 
- open cmd prompt and navigate to main_folder folder 
- run below steps to install requirements and spark-submit the code 
  1. make install 
  2. make run 

# code walkthrough
1. Make API calls to the SWAPI asynchronously 
2. Get the total number of records available for people and planets resource. This is to set up pagination asynchronously
3. Based on the counts, fetch people and planets data asynchronously
4. Grab the required fields and create spark dataframes out of it
5. Join the people and planet data on URL and get the count of people per planet. (I have grabbed all planets wise poeple count, but join cond can be modified accordingly if there are any changes to requirements)
6. write the output to a json file in "main_folder" directory - "people_count_by_planet.json"