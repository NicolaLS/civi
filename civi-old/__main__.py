import asyncio
import sys
import json
import asyncio

from civi import syncer


def log(msg):
    print(msg)
    sys.stdout.flush()

if __name__ == '__main__':
    # entry point for the programm

    config_path = sys.argv[1]
    log(f'reading config from: {config_path}')
    config_file = open(config_path)

    log('parsing config...')
    # ok for some reason json.load(config_file) does not work and I cant figure out why..
    config_data = json.loads(config_file.read()) #it dosnt make any sense at all but ok
    # we want to fail if the keys do not exist so we do no checking
    DB_PATH = config_data['db-path']
    TOKEN = config_data['auth-token']

    log(f'using database: {DB_PATH}, and github auth-token: {TOKEN}')
    log('start main...')

    asyncio.run(syncer.main(DB_PATH, TOKEN))
