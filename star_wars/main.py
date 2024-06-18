import json
import asyncio
import aiohttp
from math import ceil
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import count


base_url = 'https://swapi.dev/api'

async def fetch_data(task_type, session, url, key):
    print('started getting ', task_type)
    async with session.get(url) as response:
        response.raise_for_status()
        result = await response.json()
        print('ended getting ', task_type)
        return result[f'{key}']


async def get_data_from_SWAPI():
    people_list = []
    planet_list = []

    async with aiohttp.ClientSession() as session:

        # get the count of items by making an API call to the SWAPI resource
        people_count_task = asyncio.create_task(fetch_data('people count',session, f'{base_url}/people/', 'count'))
        planet_count_task = asyncio.create_task(fetch_data('planet count',session, f'{base_url}/planets/', 'count'))

        people_count = await people_count_task
        planet_count = await planet_count_task

        print('people_count : ', people_count)
        print('planet_count : ', planet_count)

        # asynchronously fetch all the pages from poeple and planet resource
        people_tasks = [fetch_data('people data', session, f'{base_url}/people/?page={page}', 'results') for page in range(1, ceil(people_count/10)+1)]
        planet_tasks = [fetch_data('planet data', session, f'{base_url}/planets/?page={page}', 'results') for page in range(1, ceil(planet_count/10)+1)]

        tasks1 = asyncio.gather(*people_tasks)
        tasks2 = asyncio.gather(*planet_tasks)
        people_list.append(await tasks1)
        planet_list.append(await tasks2)

        person_data = []
        planet_data = []

        for page in people_list[0]:
            for person in page:
                person_data.append((person['name'], person['homeworld']) )

        for page in planet_list[0]:
            for planet in page:
                planet_data.append((planet['name'], planet['url']) )

        return person_data, planet_data


async def main():

    # fetch people and planets data from SWAPI
    people_list, planet_list = await get_data_from_SWAPI()

    # create a spark session
    spark = SparkSession.builder.appName('star_wars').master('local[*]').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # create dataframes for people and planets
    df_people = spark.createDataFrame(people_list, ['person_name', 'planet_url'])
    df_planets = spark.createDataFrame(planet_list, ['planet_name', 'planet_url'])

    # join two dfs and find the people count on each planet
    join_cond = df_planets.planet_url == df_people.planet_url
    df_count = (df_planets.join(df_people, join_cond, 'left')
        .select(df_planets.planet_name, df_people.person_name)
        .dropDuplicates()
        .groupBy('planet_name').agg(count('person_name').alias('people_count'))
    )

    # write to target
    with open('people_count_by_planet.json', 'w+') as f:
        f.write(json.dumps(df_count.rdd.collectAsMap()))


# Run the main function using 'asyncio.run()' if the script is executed
if __name__ == "__main__":
    try:
        asyncio.run(main())
        exit(0)
    except Exception as e: 
        print('Exception raised: ', e)
        raise e
