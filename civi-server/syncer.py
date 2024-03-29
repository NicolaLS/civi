import asyncio
from codecs import ignore_errors
import json
import aiohttp
import aiosqlite
from aiosqlite import IntegrityError
import sys

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

    # create workflows table
    # await db.execute("""
    # CREATE TABLE IF NOT EXISTS workflows (
    #             id INTEGER PRIMARY KEY,
    #             node_id TEXT,
    #             name TEXT,
    #             path TEXT,
    #             state TEXT,
    #             created_at NUMERICAL,
    #             updated_at NUMERICAL,
    #             );
    # """)

    # await db.commit()

    #create runs table
    await db.execute("""
    CREATE TABLE IF NOT EXISTS runs (
              id INTEGER PRIMARY KEY,
              name TEXT,
              workflow_id INTEGER,
              node_id TEXT,
              head_branch TEXT,
              head_sha TEXT,
              path TEXT,
              display_title TEXT,
              run_number INTEGER,
              event TEXT,
              status TEXT,
              conclusion TEXT,
              check_suite_id TEXT,
              check_suite_node_id TEXT,
              created_at NUMERICAL,
              updated_at NUMERICAL,
              actor TEXT
,
              run_attempt INTEGER,
              run_started_at NUMERICAL,
              triggering_actor TEXT
              )
              """)
    await db.commit()

    #create jobs table
    await db.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            run_id INTEGER REFERENCES runs,
            run_attempt INTEGER,
            node_id TEXT,
            head_sha TEXT,
            status TEXT,
            conclusion TEXT,
            started_at NUMERICAL,
            completed_at NUMERICAL,
            name TEXT,
            runner_id INTEGER,
            runner_name TEXT,
            runner_group_id INTEGER,
            runner_group_name TEXT,
            steps BLOB
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
    VALUES(:id, :name, :workflow_id, :node_id, :head_branch, :head_sha, :path, :display_title, :run_number, :event, :status, :conclusion, :check_suite_id, :check_suite_node_id, :created_at, :updated_at, :actor, :run_attempt, :run_started_at, :triggering_actor)
    """ if tag.startswith('runs') else """INSERT INTO jobs
    VALUES(:id, :run_id, :run_attempt, :node_id, :head_sha, :status, :conclusion, :started_at, :completed_at, :name, :runner_id, :runner_name, :runner_group_id, :runner_group_name, :steps)
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
            vals = { k: e[k] for k in ('id', 'name', 'workflow_id', 'node_id',
            'head_branch', 'head_sha', 'path', 'display_title',
             'run_number', 'event', 'status', 'conclusion',
              'check_suite_id', 'check_suite_node_id', 'created_at', 'updated_at',
               'run_attempt', 'run_started_at') }

            actor = e.get('actor')
            vals['actor'] = str(actor.get('login')) if actor else str(None)
            triggering_actor = e.get('triggering_actor')
            vals['triggering_actor'] = str(triggering_actor.get('login')) if triggering_actor else str(None) 
            elements.append(vals)
        else:
            vals = { k: e[k] for k in ('id', 'run_id', 'run_attempt', 'node_id', 'head_sha', 'status', 'name', 'conclusion', 'started_at', 'completed_at', 'runner_id', 'runner_name', 'runner_group_id', 'runner_group_name') }
            vals['steps'] = json.dumps(e.get('steps'), indent=2)
            elements.append(vals)
    
    total_count = data['total_count']
    
    return (int(total_count), elements)

async def run_syncer(db_path, token):
    #TODO as cmd arg
    #TODO with connect as db..
    db = await aiosqlite.connect(db_path)
    await init_db(db)    

    HEADERS = { 'Accept': 'application/json', 'Authorization': 'Bearer ' + token, 'User-Agent': 'CI-VI / 1.0 Github CI Visualization'}
    async with aiohttp.ClientSession('https://api.github.com', headers=HEADERS) as session:
        global REPO_URL
        REPO_URL = '/repos/fedimint/fedimint'
        await sync('runs-0', db, session, '/actions/runs')
        await db.close()

