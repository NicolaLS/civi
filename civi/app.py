import asyncio
import json
import aiohttp
import aiosqlite
from aiosqlite import IntegrityError

# dont do this
REPO_URL = None

async def init_db(db):
    # create sync state table
    await db.execute("""CREATE TABLE IF NOT EXISTS state (
                id TEXT PRIMARY KEY,
                page INTEGER,
                total_pages INTEGER
                )
               """);
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
   page, total_pages = await get_state(tag, db)
   # we already fetched this page so we want to fetch the next
   page = page + 1
   print(f'{tag}: syncing from {page}')

   while True:
    new_last, elements = await fetch_elements(tag, session, endpoint, page)
    print(f'{tag}: fetched {len(elements)} elements')

    if total_pages is None:
        # there was no state initialized in the db
        total_pages = new_last
    elif total_pages < new_last:
        # d new pages were added in between requests !
        # this means we did not fetch page i but i-d
        # we need to skip this (or else we will stop due to duplicates)
        # and shift our 'cursor' by the delta (page)
        delta = new_last - total_pages
        print(f'{tag}: {delta} new pages were added in between requests')
        page = page + delta
        total_pages = new_last
        # notice we don't update the state in the DB yet.
        # this is because if we fail here we want to come to the same
        # conclusion and try to skip & shift again
        continue
    
    # ids of inserted elements
    inserted = await insert_elements(tag, db, elements)
    await update_state(tag, db, page, total_pages)

    if not tag.startswith('jobs'):
        # if we just fetched the runs we want to fetch all the jobs for each run
        syncers = [ sync(f'jobs-{i}', db, session, f'/actions/runs/{i}/jobs') for i in inserted]
        await asyncio.gather(*syncers)
    else:
        # a run will not change so its jobs will not change too
        # this means we dont need to safe where we are or reset the page to sync from the
        # start again we just stop here (we also don't care for duplicates)
        break

    if len(inserted) < len(elements) or page == total_pages:
        # we didn't write all elements this means we reached to already synced state
        # or we fetched the last page
        page = 0 #reset page
        await update_state(tag, db, page, total_pages)
        break

    page = page + 1


async def get_state(tag, db):
    c = await db.execute("""SELECT page, total_pages FROM state
    WHERE id=?
    """, (tag,))

    result = await c.fetchall() # dont use fetchall?

    if not result:
        return (0, None)
    else:
        return result[0]

async def update_state(tag, db, page, total_pages):
    await db.execute(""" INSERT OR REPLACE INTO state
    VALUES(?, ?, ?)
    """, (tag, page, total_pages))
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
            print(f'{tag}: reached duplicates')
            break
    return ids


async def fetch_elements(tag, session, endpoint,  page):
    resp = None

    while True:
        resp = await session.get(REPO_URL + endpoint + '?per_page=100&page=' + str(page))
        remaining = int(resp.headers.get('x-ratelimit-remaining'))
        if remaining == 0:
            print(f'{tag}: rate-limit exceeded. waiting 1 hour')
            await asyncio.sleep(3600)
        elif resp.status == 403:
            print(f'{tag}: got status 403. probably secondary rate-limit due to async (job) requests. waiting 180sec')
            print(await resp.text())
            await asyncio.sleep(180)
        else:
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
    
    last_page = page
    if 'last' in resp.links:
        last_page = resp.links.get('last').get('url').query['page']
    
    return (int(last_page), elements)

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
            print('finished syncing, waiting one hour..')
            await asyncio.sleep(3600)