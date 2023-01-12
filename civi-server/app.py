from flask import Flask
from flask import request
from syncer import run_syncer
from datetime import datetime
import asyncio
import sqlite3
import pandas as pd
import threading
import time
import json

app = Flask(__name__)

# This is a bad design but I just want to finish this quickly
# for now: The Syncer should be a state-machine and then we
# could run a dispatcher loop which either handles requests or
# drives the Syncer (because we use sqlite we can only have
# one connection) or even better use a Posgres database so we
# could just read the DB if we need to while syncing in another
# thread or proccess.
# I don't want to do this now though so we just put a pandas
# dataframe in a lock which will get updated after every sync
# cycle.

# probably naive way of doing cfg
cfg_file = open('config.json')
cfg = json.loads(cfg_file.read())

db_path = cfg['db-path']
gh_token = cfg['auth-token']

def syncer_worker(lock):
  global df # bad ik

  while True:
    # run the syncer
    asyncio.run(run_syncer(db_path, gh_token))
    print('updating dataframe...')
    # reload the dataframe
    # if we reload EVERYTHING anyway why dont we do it on request ?
    # keeping the df in memory is obvl. bad but we cant just lock the DB
    # while the syncer is working..the server would be unresponsive for
    # most of the time
    with lock:
      df = pd.read_sql("""SELECT jobs.id, jobs.run_id, jobs.run_attempt, jobs.started_at, jobs.completed_at, 
      jobs.name, runs.workflow_id FROM jobs INNER JOIN runs ON jobs.run_id=runs.id WHERE
      jobs.status='completed' AND jobs.conclusion IN ('success', 'failure')""", conn)
      df['started_at']= pd.to_datetime(df['started_at'])
      df['completed_at']= pd.to_datetime(df['completed_at'])
      df['durration'] = df['completed_at'] - df['started_at']

    print('finished, waiting one hour..')
    time.sleep(3600)

# connect to DB we can set check_same_thread=False because we use a lock
conn = sqlite3.connect(db_path, check_same_thread=False)
lock = threading.Lock()
# we don't need to use the lock yet, since the syncer_worker hasn't started
df = pd.read_sql("""SELECT jobs.id, jobs.run_id, jobs.run_attempt, jobs.started_at, jobs.completed_at, 
jobs.name, runs.workflow_id FROM jobs INNER JOIN runs ON jobs.run_id=runs.id WHERE
jobs.status='completed' AND jobs.conclusion IN ('success', 'failure')""", conn)
df['started_at']= pd.to_datetime(df['started_at'])
df['completed_at']= pd.to_datetime(df['completed_at'])
df['durration'] = df['completed_at'] - df['started_at']

syncer = threading.Thread(target=syncer_worker, args=(lock, ))
syncer.start()

@app.route("/")
def running_ci():
  return "Running ci"

# returns all unique workflow IDs
@app.route("/workflows")
def workflows():
  with lock:
    workflow_ids = df['workflow_id'].unique().tolist()
    return workflow_ids

# returns all unique job names in a workflow
@app.route("/workflows/<int:workflow_id>/jobs")
def jobs(workflow_id):
  with lock:
    job_names = df['name'].loc[df['workflow_id'] == workflow_id].unique().tolist()
    return job_names

# returns job data
@app.route("/workflow/<wf_id>/job/<string:job_name>")
def job(wf_id, job_name):
  with lock:
    # TODO: refactor to get all and use *params in proccess_data
    rolling = {
      "method": request.args.get('rolling_method'),
      "window": request.args.get('rolling_window')
    }
    method = request.args.get('method')
    interval = request.args.get('interval')
    data = proccess_data(wf_id, job_name, method, interval, rolling)
    return data

def proccess_data(wf_id, name, method='mean', interval='d', rolling=None):
  data_df = df[(df['name'] == name) & (df['workflow_id'] == wf_id)]
  if method == 'mean':
    data_df = df.loc[df['name'] == name].resample(interval, on='started_at')['durration'].mean().dropna(how='all')
  elif method == 'median':
    data_df = df.loc[df['name'] == name].resample(interval, on='started_at')['durration'].median().dropna(how='all')
  else:
    return "" #FIXME: return error
  
  data_df = data_df.dt.total_seconds().div(60).astype(float)
  if rolling:
    if rolling.get('method') == 'mean':
      data_df = data_df.rolling(int(rolling['window'])).mean().dropna(how='all')
    elif rolling.get('method') == 'median':
      data_df = data_df.rolling(int(rolling['window'])).median().dropna(how='all')

  # TODO: make duration also as variable (secs, mins)
  data = {
    "started_at": data_df.index.tolist(),
    "durration_min": data_df.tolist(),
  }
  return data