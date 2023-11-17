import requests as r
from flask import Flask, render_template, url_for, request, redirect, jsonify
import utils
import json
import ast
import re
import threading
import copy
import os
import sqlite3

app = Flask(__name__)

# Home page where the user can request the wanted JSON file
@app.route('/', methods=['GET', 'POST'])
def home():
    # Redirecting if form was submited
    if request.method == 'POST':
        args_dict = {key: value for key, value in zip(request.form.keys(), request.form.values())}
        
        # Migrate all records
        if len(request.form.keys()) < 2:
            return redirect(url_for('.uploading_json', messages=args_dict))
        
        # Migrate record or query of records
        else:
            return redirect(url_for('.json_generator', messages=args_dict))
    
    return render_template('index.html')

# Metadata parser for all videos in selected category
json_records = None
@app.route('/json_generator', methods=['GET', 'POST'])
def json_generator():
    # Restarting progress list
    global progress_list
    progress_list = []

    global json_records

    # Redirecting to upload page
    if request.method == 'POST':
        return redirect(url_for('.uploading_json'))

    # Recovering form data
    args_dict = request.args['messages'].replace("'", '"')
    args_dict = json.loads(args_dict)

    # Deciding between single record request or multiple record request
    contents = []
    # Single record
    if len(args_dict['recid']) != 0:
        contents = utils.fetch_data(recid=args_dict['recid'])

    # Multiple records
    else:
        query = args_dict['qry']

        # Sanitazing and processing query
        # Search query
        if re.match("^[a-zA-Z ]*$", query):
            query = query.replace(" ", "+") + "+"
            contents = utils.fetch_data(query=query)
            
        # Multiple records
        elif re.match("^[0-9]+([,;][0-9]+)*$", query):    
            for recid in query.split(","):
                contents = contents + utils.fetch_data(recid=recid)

        # Malformed/malicious input
        else:
            contents = []

    # Generating blobs and jsons
    blob_list = utils.creating_blobs(contents)
    json_records, _ = utils.creating_jsons(blob_list)

    return render_template('json_generator.html', blob_list=blob_list, json_records=json_records)

# Creating and updating progress bar
progress_list = []
thread = None
@app.route('/api/upload_progress')
def upload_progress():
    global progress_list
    progress_list_copy = copy.deepcopy(progress_list)

    return jsonify(progress_list_copy)

# Writing state file to save migration state
@app.route('/api/write_db', methods=['POST'])
def write_db():
    
    state_string = request.get_json()['state_string']
    state_list = state_string.split('\n')[:-1]

    connection = sqlite3.connect('moving_images_data/migration_database.db')
    cur = connection.cursor()

    for state in state_list:
        state = state.split(';')
        cur.execute("INSERT OR REPLACE INTO migration_state (recid_video, uploaded, migration_message) VALUES (?, ?, ?)",
                    (state[0] + '_' + state[2], ast.literal_eval(state[1]), state[3]))
    
    connection.commit()

    return jsonify({'message': 'Migration state saved'}), 200

# Uploading JSON to CDS-Videos
records_len = 0
@app.route('/uploading_json', methods=['GET', 'POST'])
def uploading_json():
    global json_records
    global progress_list
    global records_len

    #json_records = ast.literal_eval(request.args['json_records'])
    #json_records.append([])

    '''
    # Deciding whether we are dealing with one record or multiple records
    multiple = False
    if len(json_records) > 1:
        multiple = True
    '''

    if request.method == 'GET':

        # Migrate all records
        if len(request.args.keys()) > 0:
            json_records = []
            progress_list = []

            # Getting record successes and fails
            connection = sqlite3.connect('moving_images_data/migration_database.db')
            cur = connection.cursor()
            cur.execute("SELECT recid_video FROM migration_state WHERE uploaded = 1")
            recid_successes = {row[0]: True for row in cur.fetchall()}

            # Parsing JSON file with all records
            with open("moving_images_data/persistent_data/moving_images_json_all", "r") as f:
                json_parser = f.read().split('{\n  "recid"')
                for json_record in json_parser[1:]:
                    json_record = '{\n  "recid"' + json_record
                    json_record = json.loads(json_record)

                    try:
                        # Computing index of video used
                        has_file = False
                        index_video = 0
                        for file in json_record['_files']:
                            if file['tags']['content_type'] in ['mp4', 'mkv', 'mov']:
                                has_file = True
                                break
                            index_video += 1

                        if has_file and recid_successes[str(json_record['recid']) + '_' + json_record['_files'][index_video]['key']] is True:
                            continue
                        else:
                            json_records.append(json_record)

                    except Exception as e:
                        #print(e)
                        json_records.append(json_record)
            

        records_len = 0
        for json_record in json_records:
            if type(json_record) is list:
                records_len += len(json_record)
            else:
                records_len += 1

    # Creating threads to allow building progress bar and passing jsons in chunks
    global thread
    json_records_chunk = json_records[:10] if len(json_records) >= 10 else json_records
    json_records = json_records[10:] if len(json_records) >= 10 else []
    thread = threading.Thread(target=utils.upload_json, args=(json_records_chunk, progress_list))
    thread.start()

    return render_template('uploading_json.html', progress_list=progress_list, records_len=records_len)



def main():
    # Running app
    app.run(port=5555, debug=True)

if __name__ == '__main__':
    main()