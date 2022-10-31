import asyncio
import sys
import json
import asyncio

from civi import app

if __name__ == '__main__':
    # entry point for the programm

    config_path = sys.argv[1]
    print(f'reading config from: {config_path}')
    config_file = open(config_path)

    print('parsing config...')
    # ok for some reason json.load(config_file) does not work and I cant figure out why..
    config_data = json.loads(config_file.read()) #it dosnt make any sense at all but ok
    # we want to fail if the keys do not exist so we do no checking
    DB_PATH = config_data['db-path']
    TOKEN = config_data['auth-token']

    print(f'using database: {DB_PATH}, and github auth-token: {TOKEN}')
    print('start main...')

    asyncio.run(app.main(DB_PATH, TOKEN))
