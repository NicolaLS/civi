import asyncio
from codecs import ignore_errors
import json
import aiohttp
import aiosqlite
from aiosqlite import IntegrityError
import sys

from civi import vi

# dont do this
REPO_URL = None

async def log(msg):
    print(msg)
    sys.stdout.flush()

async def init_db(db):
    # create sync state table
    await db.execute("""CREATE TABLE IF NOT EXISTS state (
                id TEXT PRIMARY KEY,
                page INTEGER,
                ignore_elements INTEGER
                )
               """)
    await db.commit()

    #create runs table
    await db.execute("""
              CREATE TABLE IF NOT EXISTS runs (
              id INTEGER PRIMARY KEY,
              workflow_id integer,
              run_number INTEGER,
              created_at NUMERICAL,
              status TEXT,
              raw BLOB
              )
              """)
    await db.commit()

    #create jobs table
    await db.execute("""
              CREATE TABLE IF NOT EXISTS jobs (
              id INTEGER PRIMARY KEY,
              run_id INTEGER REFERENCES runs,
              name TEXT,
              conclusion TEXT,
              started_at NUMERICAL,
              completed_at NUMERICAL,
              raw BLOB
              )
              """)

    await db.commit()

async def sync(tag, db, session, endpoint):
   # fetch the state in case we are resuming (if we have lets say 50% synced, then stop&resume it would think it reached the end due to duplicates)
   page, ignore_elements = await get_state(tag, db)
   total_count = None
   # we already fetched this page so we want to fetch the next
   page = page + 1
   await log(f'{tag}: syncing from {page}')

   while True:
    new_total_count, elements = await fetch_elements(tag, session, endpoint, page)
    await log(f'{tag}: fetched {len(elements)} elements')

    if total_count is None:
        total_count = new_total_count
    elif total_count < new_total_count:
        # there were elements added in between
        delta = new_total_count - total_count
        ignore_elements = ignore_elements + delta
        await update_state(tag, db, page, ignore_elements) # in jobs this should never happen anyway

    # ids of inserted elements
    inserted = await insert_elements(tag, db, elements)
    await log(f'{tag}: wrote {len(inserted)} to db')
    if not tag.startswith('jobs'):
        await update_state(tag, db, page, ignore_elements)

    if not tag.startswith('jobs'):
        # if we just fetched the runs we want to fetch all the jobs for each run
        syncers = [ sync(f'jobs-{i}', db, session, f'/actions/runs/{i}/jobs') for i in inserted]
        await asyncio.gather(*syncers)
    else:
        # a run will not change so its jobs will not change too
        # this means we dont need to safe where we are or reset the page to sync from the
        # start again we just stop here (we also don't care for duplicates)
        break

    max_ignore = min(100, ignore_elements)
    await log(f'{tag} ignoring {max_ignore}')
    if len(inserted) < len(elements) - max_ignore or len(elements) == 0:
        # we didn't write all elements this means we reached to already synced state
        ignore_elements = ignore_elements - max_ignore
        await log(ignore_elements)
        page = 0 #reset page
        await update_state(tag, db, page, ignore_elements) # ignore should be 0
        break

    ignore_elements = ignore_elements - max_ignore
    await update_state(tag, db, page, ignore_elements)
    page = page + 1


async def get_state(tag, db):
    c = await db.execute("""SELECT page, ignore_elements FROM state
    WHERE id=?
    """, (tag,))

    result = await c.fetchall() # dont use fetchall?

    if not result:
        return (0, 0)
    else:
        return result[0]

async def update_state(tag, db, page, ignore):
    await db.execute(""" INSERT OR REPLACE INTO state
    VALUES(?, ?, ?)
    """, (tag, page, ignore))
    await db.commit()

async def insert_elements(tag, db, elements):
    query = """INSERT INTO runs
    VALUES(:id, :workflow_id, :run_number, :created_at, :status, :raw)
    """ if tag.startswith('runs') else """INSERT INTO jobs
    VALUES(:id, :run_id, :name, :conclusion, :started_at, :completed_at, :raw)
    """

    ids = []
    for e in elements:
        try:
            await db.execute(query, e)
            await db.commit()
            ids.append(e['id'])
        except IntegrityError as ie:
            id = e['id']
            await log(f'{tag}: {ie} reached duplicates for id: {id}')
            continue # should break here but just to debug continue
        except Exception as e:
            await log(f'{tag}: OTHER DB ERROR {e}')
    return ids


async def fetch_elements(tag, session, endpoint,  page):
    resp = None

    while True:
        try:
            resp = await session.get(REPO_URL + endpoint + '?per_page=100&page=' + str(page))
        except Exception as e:
            await log(f'{tag} request failed got error: {e}')
            await asyncio.sleep(60)
        remaining = resp.headers.get('x-ratelimit-remaining')
        await log(f'{tag} remaining requests: {remaining}')
        if remaining == None:
            await log(f'{tag}: probably secondary rate-limit due to async (job) requests. waiting 180sec')
            await log(await resp.text())
            await asyncio.sleep(121)
        elif int(remaining) == 0:
            await log(f'{tag}: rate-limit exceeded. waiting 1 hour')
            await asyncio.sleep(3600)
        elif resp.status == 200:
            break

    data = await resp.json()
    key = 'workflow_runs' if tag.startswith('runs') else 'jobs'
    elements = []
    for e in data[key]:
        if key == 'workflow_runs':
            vals = { k: e[k] for k in ('id', 'workflow_id', 'run_number', 'created_at', 'status') }
            vals['raw'] = json.dumps(e, indent=4)
            elements.append(vals)
        else:
            vals = { k: e[k] for k in ('id', 'run_id', 'name', 'conclusion', 'started_at', 'completed_at') }
            vals['raw'] = json.dumps(e, indent=4)
            elements.append(vals)
    
    total_count = data['total_count']
    
    return (int(total_count), elements)

async def main(DB_PATH, TOKEN):
    #TODO as cmd arg
    #TODO with connect as db..
    db = await aiosqlite.connect(DB_PATH)
    await init_db(db)    

    HEADERS = { 'Accept': 'application/json', 'Authorization': 'Bearer ' + TOKEN, 'User-Agent': 'CI-VI / 1.0 Github CI Visualization'}
    async with aiohttp.ClientSession('https://api.github.com', headers=HEADERS) as session:
        global REPO_URL
        REPO_URL = '/repos/fedimint/fedimint'
        while True:
            await sync('runs-0', db, session, '/actions/runs')
            await db.close()
            vi.visualize(DB_PATH)
            db = await aiosqlite.connect(DB_PATH)
            await log('finished, waiting one hour..')
            await asyncio.sleep(3600)