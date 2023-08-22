import requests as req
import math
import json

from cds_dojson.marc21.utils import create_record
from cds_dojson.marc21.models.videos.video import model as video_model
from cds_dojson import utils

# Computing tag frequencies and repeating tags
def compute_repeating_tags(tags_list):
    repeating_tags_list = []
    for tags in tags_list:
        
        tag_frequency_dict = {}
        for tag in tags:
            
            try:
                tag_frequency_dict[tag] += 1
            
            except:
                tag_frequency_dict[tag] = 1

        repeating_tags = {tag: tag_frequency_dict[tag] for tag in tag_frequency_dict.keys() if tag_frequency_dict[tag] > 1}
        repeating_tags_list.append(repeating_tags)
    
    return repeating_tags_list

# Extracting existing tags on parsed blob
def extract_tags(blob):
    tags = []
    for tag in blob.keys():

        # Tags with indices or codes
        if len(tag) > 3:

            # Multiple instances of the same tag
            if type(blob[tag]) is tuple:
                for subtag in blob[tag]:
                    for key in subtag.keys():
                        tags.append(tag + key)
            
            # Multiple codes for a tag
            else:
                for subtag in blob[tag].keys():
                    tags.append(tag + subtag)

        # Tags with no indices and no code
        else:
            tags.append(tag)

    return tags

# Fetching data, removing XML header and saving it to a binary file
def fetch_data(recid=None, query=None):
    contents = []

    # For single record usage
    if recid is not None:
        len_removable_ini = len('<?xml version="1.0" encoding="UTF-8"?>') + 1
        
        r = req.get('https://cds.cern.ch/record/' + str(recid) + '/export/xm?ln=en')
        
        contents.append(r.content[len_removable_ini:])
    
    # For multiple record usage
    else:

        r = req.get('https://cds.cern.ch/search?ln=en&cc=CERN+Moving+Images&sc=1&p=' + query + '963__a%3A%22PUBLIC%22+and+500__a%3A%22The+video+was+digitized+from+its+original+recording+as+part+of+the+CERN+Digital+Memory+project%22+and+not+980__c%3A%22MIGRATED%22+and+not+980__a%3A%22ANNOUNCEMENT%22&action_search=Search&c=&sf=&so=d&rm=&rg=10&sc=1&of=xm&jrec=1')
        
        # Extracting number of records found and preparing useful paginating/parsing variables
        total_number_files = int(str(r.content).split('Search-Engine-Total-Number-Of-Results: ')[1].split(' ')[0])
        fetch_max_size = 200
        jrec = 1

        len_removable_ini = len('<?xml version="1.0" encoding="UTF-8"?>') + 1 + len('<!--  Search-Engine-Total-Number-Of-Results: ' + str(total_number_files) + ' -->') + len('<collection xmlns="http://www.loc.gov/MARC21/slim">')
        len_removable_end = len('</collection>')
        
        for index in range(math.ceil(total_number_files/fetch_max_size)):
            r = req.get('https://cds.cern.ch/search?ln=en&cc=CERN+Moving+Images&sc=1&p=' + query + '963__a%3A%22PUBLIC%22+and+500__a%3A%22The+video+was+digitized+from+its+original+recording+as+part+of+the+CERN+Digital+Memory+project%22+and+not+980__c%3A%22MIGRATED%22+and+not+980__a%3A%22ANNOUNCEMENT%22&action_search=Search&c=&sf=&so=d&rm=&rg=10&sc=1&of=xm&jrec=' + str(jrec))
                
            # Preprocessing data to create a collection for every record
            content = str(r.content[len_removable_ini:-len_removable_end])
            content = content.replace('<record>', '<collection xmlns="http://www.loc.gov/MARC21/slim">\n<record>')
            content = content.replace('</record>', '</record>\n</collection>')
            content = content.replace('\\n', '\n')
            content = content.replace("\\\'s", "\'s")[2:-2]

            marcxml_list = [bytes(record + '</collection>', 'utf-8') for record in content.split('</collection>\n')]
            contents = contents + marcxml_list

            jrec += fetch_max_size

    return contents

# Creating blob from XML
def creating_blobs(marcxml_list):
    blob_list = []
    for marcxml in marcxml_list:
        blob_list.append(create_record(marcxml))
    
    return blob_list

# Creating JSONs from blobs and optionally computing missing tags
def creating_jsons(blob_list, missing_tags=False):
    skipping_tags = ['088__z']
    json_records = []
    missing_tags = {}
    for blob in blob_list:

        #import ipdb
        #ipdb.set_trace()

        # Special tags to skip
        skip = False
        if type(blob) is list:
            for sub_blob in blob:
                for tag in skipping_tags:
                    if tag[:-1] in sub_blob.keys():
                        if type(sub_blob[tag[:-1]]) is tuple:
                            for sub_field in sub_blob[tag[:-1]]:
                                if tag[-1] in sub_field.keys():
                                    skip = True
                        else:
                            if tag[-1] in sub_blob[tag[:-1]].keys():
                                skip = True
        else:
            for tag in skipping_tags:
                if tag[:-1] in blob.keys():
                    if type(blob[tag[:-1]]) is tuple:
                        for sub_blob in blob[tag[:-1]]:
                            if tag[-1] in sub_blob.keys():
                                skip = True
                    else:
                        if tag[-1] in blob[tag[:-1]].keys():
                            skip = True

        if skip:
            continue

        # Record with multiple videos
        if type(blob) is list:
            json_records.append([video_model.do(sub_blob) for sub_blob in blob])
            
        # Record with one video
        else:
            json_records.append(video_model.do(blob))

        if missing_tags:
            missing_tags[blob['001']] = sorted(utils.not_accessed_keys(blob))

    return (json_records, missing_tags)

# Make video post and video put requests
def video_post_put(res_project, headers, json_record, progress_list):
    video_req_fields = ['title', 'description', 'language', 'date', 'contributors']
    video_add_fields = ['keywords']

    # Variable to capture video failed fields
    failed_fields = []

    # Building video request headers and body
    headers['content-type'] = "application/vnd.video.partial+json"
    #body = {"$schema": "https://sandbox-videos.web.cern.ch/schemas/deposits/records/videos/video/video-v1.0.0.json"}
    body = {"$schema": "https://localhost:5000/schemas/deposits/records/videos/video/video-v1.0.0.json"}
    body["_project_id"] = res_project.json()['id']
    for field in video_req_fields:
        try:
            body[field] = json_record[field]
        except Exception as err:
            if field in ['title', 'date']:
                failed_fields.append(field)
            elif field == 'description':
                body[field] = 'No description'
            elif field == 'language':
                body[field] = 'No language information'
            else:
                body[field] = [{'name': 'unknown', 'role': 'Creator'}]

    for field in video_add_fields:
        try:
            body[field] = json_record[field]
        except:
            continue

     # Ending thread and providing video broken record information
    if len(failed_fields) > 0:
        progress_list.append((False, json_record['_files'][0]['key'], '(Video) No fields: {}'.format(failed_fields), json_record['recid']))
        return

    # Request to create video
    #res_video = req.post("https://sandbox-videos.web.cern.ch/api/deposits/video/", json=body, headers=headers)
    res_video = req.post("https://localhost:5000/api/deposits/video/", json=body, headers=headers, verify=False)
    #print(res_video.content, '\n')
    
    if res_video.status_code != 201:
        print(res_video.content)
        progress_list.append((False, json_record['_files'][0]['key'], '(Video) Bad response', json_record['recid']))

    else:
        file_path = None
        index_video = 0
        for file in json_record['_files']:
            if file['tags']['content_type'] in ['mp4', 'mkv', 'mov', 'avi']:
                if 'cern.ch/digital-memory' in file['filepath']:
                    file_path = file['filepath'] if req.get(file['filepath'], stream=True).status_code == 200 else None
                    
                else:
                    file_path = "https://mediaarchive.cern.ch/" + file['filepath'] if req.get("https://mediaarchive.cern.ch/" + file['filepath'], stream=True).status_code == 200 else None

                if file_path is not None:
                    break

            index_video += 1

        if file_path is not None:
            headers['content-type'] = 'application/json'
            body = {'uri': file_path, 'key': file_path.split('/')[-1], 'bucket_id': res_video.json()['metadata']['_buckets']['deposit'], 'deposit_id': res_video.json()['metadata']['_deposit']['id']}
            res_flow = req.post("https://localhost:5000/api/flows/", json=body, headers=headers, verify=False)
            #print(res_flow.content, '\n')

            progress_list.append((True, json_record['_files'][index_video]['key'], headers['Authorization'], res_video.json()['id'], json_record['recid']))

        else:
            progress_list.append((False, json_record['_files'][0]['key'], '(Video) Video file not compatible or video not found', json_record['recid']))

    
# Uploads JSON to CDS-Videos
def upload_json(json_records, progress_list):
    # Get access token
    with open('../access_token_local') as f:
        auth_token = f.read().split('\n')[0]

    # Project fields and video fields
    project_req_fields = ['title', 'description', 'category', 'type', 'contributors']
    project_add_fields = ['keywords', '_eos_library_path']

    # Creating projects and videos
    for json_record in json_records:

        headers = {'Authorization': f'Bearer {auth_token}'}
        body = {}
            
        # Building project request headers and body
        headers['content-type'] = 'application/vnd.project.partial+json'
        #body = {"$schema": "https://sandbox-videos.web.cern.ch/schemas/deposits/records/videos/project/project-v1.0.0.json"}
        body = {"$schema": "https://localhost:5000/schemas/deposits/records/videos/project/project-v1.0.0.json"}

        # Variable to capture project failed fields
        failed_fields = []

        # Multiple videos in one project
        if type(json_record) is list:
            for field in project_req_fields:
                try:
                    body[field] = json_record[0][field]
                except Exception as err:
                    if field == 'title':
                        failed_fields.append(field)
                    elif field == 'description':
                        body[field] = 'No description.'
                    elif field == 'category':
                        body[field] = 'CERN'
                    elif field == 'type':
                        body[field] = 'VIDEO'
                    else:
                        body[field] = [{'name': 'unknown', 'role': 'Creator'}]


            for field in project_add_fields:
                try:
                    body[field] = json_record[0][field]
                except:
                    continue
            
            # Ending thread and providing project broken record information for mulitple videos in one record
            if len(failed_fields) > 0:
                for sub_json_record in json_record:
                    progress_list.append((False, sub_json_record['_files'][0]['key'], '(Project) No fields: {}'.format(failed_fields), sub_json_record['recid']))
                continue

            # Request to create project
            #res_project = req.post("https://sandbox-videos.web.cern.ch/api/deposits/project/", json=body, headers=headers)
            res_project = req.post("https://localhost:5000/api/deposits/project/", json=body, headers=headers, verify=False)

            if res_project.status_code != 201:
                print(res_project.content)
                for sub_json_record in json_record:
                    progress_list.append((False, sub_json_record['_files'][0]['key'], '(Project) Bad response', sub_json_record['recid']))
                continue

            else:
                for sub_json_record in json_record:
                    video_post_put(res_project, headers, sub_json_record, progress_list)

        # Single video project
        else:
            for field in project_req_fields:
                try:
                    body[field] = json_record[field]
                except Exception as err:
                    if field == 'title':
                        failed_fields.append(field)
                    elif field == 'description':
                        body[field] = 'No description.'
                    elif field == 'category':
                        body[field] = 'CERN'
                    elif field == 'type':
                        body[field] = 'VIDEO'
                    else:
                        body[field] = [{'name': 'unknown', 'role': 'Creator'}]

            for field in project_add_fields:
                try:
                    body[field] = json_record[field]
                except:
                    continue

            # Ending thread and providing project broken record information for single video in one record
            if len(failed_fields) > 0:
                progress_list.append((False, json_record['_files'][0]['key'], '(Project) No fields: {}'.format(failed_fields), json_record['recid']))
                continue

            # Request to create project
            #res_project = req.post("https://sandbox-videos.web.cern.ch/api/deposits/project/", json=body, headers=headers)
            res_project = req.post("https://localhost:5000/api/deposits/project/", json=body, headers=headers, verify=False)

            if res_project.status_code != 201:
                print(res_project)
                progress_list.append((False, json_record['_files'][0]['key'], '(Project) Bad response', json_record['recid']))

            else:
                video_post_put(res_project, headers, json_record, progress_list)