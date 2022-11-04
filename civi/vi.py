import os
import sqlite3
import json
from datetime import datetime
import random
import matplotlib.pyplot as plt
import statistics
import matplotlib.dates as mdates

def get_workflow_ids():
    cursor.execute("""SELECT DISTINCT workflow_id FROM runs""")
    workflow_ids = [ id[0] for id in cursor.fetchall() ]
    return workflow_ids

# returns run ids for the specified workflow
def per_wf_data(id):
    cursor.execute("""SELECT id, run_number, created_at 
                   FROM runs WHERE workflow_id=? AND status=? ORDER BY created_at """, (id, 'completed'))

    # (id, run_number, created_at)
    runs = cursor.fetchall()
    
    # because we ordered the results the last one will be the most recent run
    most_recent_run = runs[-1][0]
    
    # we only care for jobs that still exist
    cursor.execute("""SELECT name FROM jobs WHERE run_id=?""", (most_recent_run, ))
    relevant_job_names = [n[0] for n in cursor.fetchall() ]
    
    data = {}
    for run in runs:
        run_number = run[1]
        run_date = run[2]
        
        placeholder= '?' # For SQLite. See DBAPI paramstyle.
        placeholders= ', '.join(placeholder for unused in relevant_job_names)
        query = 'SELECT id, run_id, name, conclusion, started_at, completed_at FROM jobs WHERE run_id=? AND name IN (%s)' % placeholders
        cursor.execute(query, (run[0], *relevant_job_names))
        
        # (id, run_id, name, conclusion, started_at, completed_at, raw)
        run_jobs = cursor.fetchall()
        for job in run_jobs:
            
            job_name = job[2]
            if job_name not in data:
                # we have to initialize the first entry
                data[job_name] = []
            
            # we want to safe the duration in seconds the job took to complete
            started_at = datetime.strptime(job[4], '%Y-%m-%dT%H:%M:%SZ')
            completed_at = datetime.strptime(job[5], '%Y-%m-%dT%H:%M:%SZ')
            durration = (completed_at - started_at).total_seconds()
            
            data[job_name].append((durration, run_number, run_date)) #<--- coninute here
    return data
    
def make_data_nicer(data):
    # right now we will have multiple runs per day
    # we will take the median of all the runs in a day
    nicer_data = {}
    
    #remember data = (duration, run_number, run_date) 0 1 2
    for job in data:
        if job not in nicer_data:
            nicer_data[job] = []
        
        single_day_durations = []
        # just take the first day as 'previous'
        previous_run_date = datetime.strptime(data[job][0][2].split('T', 1)[0], '%Y-%m-%d')
        
        for single_run in data[job]:
            single_run_duration = single_run[0]
            # if we just cut of the hours/minutes/seconds we can see if day = day
            single_run_date = datetime.strptime(single_run[2].split('T', 1)[0], '%Y-%m-%d')
            
            # if the day of this single run and the previous single run are the same
            # we append it to the single_day_durations and take the median for that day
            # in the first iteration prev_day == single_run_day but thats ok
            if single_run_date == previous_run_date:
                single_day_durations.append(single_run_duration)
            else:
                # so we are at a new day B we want to take the median of (a1, a2, a3) and write it to A
                nicer_data[job].append((statistics.median(single_day_durations), previous_run_date))
                single_day_durations = [ single_run_duration ]
            
            previous_run_date = single_run_date
        # dont't forget to use the last day..we always insert the data for the prev day when the day changes
        nicer_data[job].append((statistics.median(single_day_durations), previous_run_date))
        
    return nicer_data

def plot_and_export(data, name):

    fig, ax = plt.subplots(figsize=(10, 5))
    
    for job in data:
        job_data = { 'x': [x[0]/60 for x in data[job] ], 'y': [y[1] for y in data[job] ] }
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.plot('y', 'x', data=job_data, label=job)
        ax.tick_params(axis='x', labelrotation=90)

    ax.set_xlabel('runs')  # Add an x-label to the axes.
    ax.set_ylabel('min')  # Add a y-label to the axes.
    ax.set_title(name)  # Add a title to the axes.
    ax.legend();  # Add a legend.
    
    plt.savefig(f'./plots/{name}')

def visualize(db_path):
    #create or open database
    db = sqlite3.connect(db_path)
    #get a cursoir for ops on the db
    global cursor # god im lazy
    cursor = db.cursor()
    # get all workflow ids
    workflow_ids = get_workflow_ids()
    
    # do the things
    for id in workflow_ids:
        # 1. proccess the data
        data = per_wf_data(id)
        nicer_data = make_data_nicer(data) #this is art
        # 2. plot the data and export plots as image
        plot_and_export(nicer_data, id) #TODO use workflow name
    
    db.close()
