########################################################################################################################################################################
# THIS AN EXPERIMENTAL PIECE OF CODE DEVELOPED TO IMPLEMENT A FIRST VERSION OF BASICALLY EVERY TOOL USED ON THE CDS-VIDEO-TRANSFER PROJECT. BE CAREFUL WITH ITS USAGE. #
########################################################################################################################################################################

import requests as req
import json
import dateutil.parser as parser
import re
import os
import math
import glob
from tqdm import tqdm

from cds_dojson.marc21.utils import create_record
from cds_dojson.marc21.models.videos.video import model as video_model
from cds_dojson.marc21.models.base import model
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
def fetch_data(url=None):
    
    # For single record usage
    if url is not None:
        len_removable_ini = len('<?xml version="1.0" encoding="UTF-8"?>') + 1
        r = req.get(url)

        recid = url.split('/')[4]

        with open('./moving_images_data/' + str(recid) + '.xml', 'wb') as f:
            f.write(r.content[len_removable_ini:])
    
    # For multiple record usage
    else:
        total_number_files = 1829
        fetch_max_size = 200
        jrec = 1
        for index in range(math.ceil(total_number_files/fetch_max_size)):
            len_removable_ini = len('<?xml version="1.0" encoding="UTF-8"?>') + 1 + len('<!--  Search-Engine-Total-Number-Of-Results: 1829  -->') + len('<collection xmlns="http://www.loc.gov/MARC21/slim">')
            len_removable_end = len('</collection>')

            #r = req.get('https://cds.cern.ch/search?ln=en&cc=CERN+Moving+Images&p=&action_search=Search&op1=a&m1=a&p1=&f1=&c=CERN+Moving+Images&c=&sf=&so=d&rm=&rg=200&sc=1&of=xm&jrec=' + str(jrec))
            
            r = req.get('https://cds.cern.ch/search?ln=en&cc=CERN+Moving+Images&sc=1&p=963__a%3A%22PUBLIC%22+and+500__a%3A%22The+video+was+digitized+from+its+original+recording+as+part+of+the+CERN+Digital+Memory+project%22+and+not+980__c%3A%22MIGRATED%22+and+not+980__a%3A%22ANNOUNCEMENT%22&action_search=Search&c=&sf=&so=d&rm=&rg=200&sc=1&of=xm&jrec=' + str(jrec))

            with open('moving_images_data/moving_images_' + str(int((jrec - 1)/fetch_max_size)) + '.xml', 'wb') as f:
                
                # Preprocessing data to create a collection for every record
                content = str(r.content[len_removable_ini:-len_removable_end])
                content = content.replace('<record>', '<collection xmlns="http://www.loc.gov/MARC21/slim">\n<record>')
                content = content.replace('</record>', '</record>\n</collection>')
                content = content.replace('\\n', '\n')
                content = content.replace("\\\'s", "\'s")
                content = bytes(content, 'utf-8')[2:-2]
                f.write(content)
            
            jrec += fetch_max_size

# Creating blob from XML
def creating_blobs(PATH):
    marcxml_list = []
    with open(PATH, 'rb') as f:
        marcxml_list = f.read()
        marcxml_list = [record + bytes('</collection>', 'utf-8') for record in marcxml_list.split(bytes('</collection>\n', 'utf-8'))]

    blob_list = []
    for marcxml in marcxml_list:
        record = create_record(marcxml)
        
        if type(record) is list:
            blob_list += record
        
        else:
            blob_list.append(record)
    
    return blob_list

def main():
    # Control variable for single record or multiple records
    single = bool(int(input('1 for single record and 0 for all Digital Memory Project files: ')))

    blob_list = []
    if not single:
        #for file in glob.glob('../moving_images_data/persistent_data/moving_images_*.xml'):
        #    os.remove(file)

        #if not os.path.exists('./moving_images_data/moving_images_0.xml'):
        fetch_data()

        for file in glob.glob('./moving_images_data/moving_images_*.xml'):
            blob_list += creating_blobs(file)

    else:
        recid = input('Please insert a record number: ')

        fetch_data('https://cds.cern.ch/record/' + recid + '/export/xm?ln=en')
        blob_list = creating_blobs('./moving_images_data/' + recid + '.xml')

    # Testing blobs and key
    #print('\n', blob_list)
    #print('\n', blob['0248_'].keys())
    #print('\n', blob.keys())

    # Extracting tags contained in the blobs
    #tags_list = []
    #for blob in blob_list:
    #    tags_list.append(extract_tags(blob))
    #print('\n', tags_list)

    # Computing tag frequencies and repeating tags
    #repeating_tags_list = compute_repeating_tags(tags_list)
    #print('\n', repeating_tags_list)

    # Handling files to store all information about the explored files
    if os.path.exists('./moving_images_data/moving_images_json'):
        os.remove('./moving_images_data/moving_images_json')
    
    if os.path.exists('./moving_images_data/missing_tags_examples'):
        os.remove('./moving_images_data/missing_tags_examples')

    f_json = open('./moving_images_data/moving_images_json', 'a')
    f_missing_tags_examples = open('./moving_images_data/missing_tags_examples', 'a')
    f_fails = open('./moving_images_data/moving_images_fails', 'w')
    
    # Generating cds-videos json file, computing missing tags with examples and recording fails
    not_used_tags = set()
    not_used_tags_unique_examples = {}
    for blob in tqdm(blob_list):

        migrated = False
        
        if type(blob['980__']) is tuple:
            for item in blob['980__']:
                try:
                    if item['c'] == 'MIGRATED':
                        migrated = True
                    else:
                        continue
                except:
                    continue
                    
        if not migrated:
            try:
                json_record = video_model.do(blob)
                json_record = json.dumps(json_record, indent=2)
                f_json.write(json_record)
                f_json.write('\n')
                #print('\n', json_record)

                not_accessed_keys = utils.not_accessed_keys(blob)

                for key in not_accessed_keys:
                    if type(blob[key[:-1]]) is tuple:
                        for codes in blob[key[:-1]]:
                            if key[-1] in codes.keys():
                                try:
                                    not_used_tags_unique_examples[key] = not_used_tags_unique_examples[key].union({codes[key[-1]]})
                                except:
                                    not_used_tags_unique_examples[key] = set({codes[key[-1]]})

                    else:
                        try:
                            not_used_tags_unique_examples[key] = not_used_tags_unique_examples[key].union({blob[key[:-1]][key[-1]]})
                        except:
                            not_used_tags_unique_examples[key] = set({blob[key[:-1]][key[-1]]})

                f_missing_tags = open('./moving_images_data/missing_tags', 'w')
                f_missing_tags_values = open('./moving_images_data/missing_tags_values', 'w')

                #f_missing_tags_values.truncate(0)
                for key, value in not_used_tags_unique_examples.items():
                    f_missing_tags_values.write(str(key))
                    f_missing_tags_values.write(": ")
                    f_missing_tags_values.write(str(value))
                    f_missing_tags_values.write('\n')
                
                not_used_tags = not_used_tags.union(not_accessed_keys)
                #f_missing_tags.truncate(0)
                f_missing_tags.write(str(sorted(not_used_tags)))
                f_missing_tags.write('\n')
                #print('\n', sorted(utils.not_accessed_keys(blob)))

                f_missing_tags.close()
                f_missing_tags_values.close()

                f_missing_tags_examples.write(blob['001'] + ' ' + str(sorted(not_accessed_keys)))
                f_missing_tags_examples.write('\n')
            
            except Exception as err:
                #print(err)
                f_fails.write(blob['001'] + ' ' + str(err) + '\n')

    f_json.close()
    f_missing_tags_examples.close()
    f_fails.close()

    '''
    with open('../access_token_local') as f:
        auth_token = f.read()[:-1]
    headers = {'Authorization': f'Bearer {auth_token}', 'content-type': 'application/vnd.project.partial+json'}
    body = {"$schema": "https://localhost:5000/schemas/deposits/records/videos/project/project-v1.0.0.json"}
    #body = {"$schema": "https://sandbox-videos.web.cern.ch/schemas/deposits/records/videos/project/project-v1.0.0.json"}
    test_project = req.post("https://localhost:5000/api/deposits/project/", json=body, headers=headers, verify=False)
    #test_project = req.post("https://sandbox-videos.web.cern.ch/api/deposits/project/", json=body, headers=headers)
    #print(test_project.content, '\n')

    headers['content-type'] = "application/vnd.video.partial+json"
    body["$schema"] = "http://sandbox-videos.web.cern.ch/schemas/deposits/records/videos/video/video-v1.0.0.json"
    body["_project_id"] = json.loads(str(test_project.content)[2:-1])['id']
    body["title"] = {"title": blob_list[0]['245__']['a'].replace(' ', '_')}
    test_video = req.post("https://localhost:5000/api/deposits/video/", json=body, headers=headers, verify=False)
    #test_video = req.post("https://sandbox-videos.web.cern.ch/api/deposits/video/", json=body, headers=headers)
    #print(test_video.content, '\n')

    #headers['content-type'] = 'video/mp4'
    #video_binary = req.get("https://mediaarchive.cern.ch/MediaArchive/Video/Public/Movies/CERN/1993/CERN-MOVIE-1993-006/CERN-MOVIE-1993-006-001/#CERN-MOVIE-1993-006-001-1136-kbps-640x480-audio-64-kbps-stereo.mp4")
    #test_upload = req.put(json.loads(str(test_video.content)[2:-1])['links']['bucket'] + '/' + body['title']['title'] + ".mp4", data=video_binary.#content, headers=headers)
    #print(test_upload.content, '\n')    

    headers['content-type'] = 'application/json'
    body = {'uri': "https://mediaarchive.cern.ch/MediaArchive/Video/Public/Movies/CERN/1993/CERN-MOVIE-1993-006/CERN-MOVIE-1993-006-001/CERN-MOVIE-1993-006-001-1136-kbps-640x480-audio-64-kbps-stereo.mp4", 'key': body['title']['title'] + ".mp4", 'bucket_id': test_video.json()['metadata']['_buckets']['deposit'], 'deposit_id': test_video.json()['metadata']['_deposit']['id']}
    test_flow = req.post("https://localhost:5000/api/flows/", json=body, headers=headers, verify=False)
    #print(test_flow.content, '\n')
    '''
    

if __name__ == '__main__':
    main()

'''
PROJECT
fetch("https://sandbox-videos.web.cern.ch/api/deposits/project/", {
  "headers": {
    "authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2OTAyOTg3MzAsInN1YiI6IjE3IiwianRpIjoiYTMzYmUxZTItNDExOS00OTU2LWE5YWEtODQxYmI0NDdmZGQxIn0._7qOceBnhZp94YZZ7ngKIcGfz4fIPLR17r35iYRTRAg",
    "content-type": "application/vnd.project.partial+json",
  },
  "body": "{\"$schema\":\"https://sandbox-videos.web.cern.ch/schemas/deposits/records/videos/project/project-v1.0.0.json\"}",
  "method": "POST"
});

VIDEO
fetch("https://sandbox-videos.web.cern.ch/api/deposits/video/", {
  "headers": {
    "authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2OTAyOTg3MzAsInN1YiI6IjE3IiwianRpIjoiYTMzYmUxZTItNDExOS00OTU2LWE5YWEtODQxYmI0NDdmZGQxIn0._7qOceBnhZp94YZZ7ngKIcGfz4fIPLR17r35iYRTRAg",
    "content-type": "application/vnd.video.partial+json",
  },
  "body": "{\"$schema\":\"https://sandbox-videos.web.cern.ch/schemas/deposits/records/videos/video/video-v1.0.0.json\",\"_project_id\":\"5e6c03f439d6468991f97571623c5260\",\"title\":{\"title\":\"test_video\"}}",
  "method": "POST"
});

VIDEO FILE
fetch("https://sandbox-videos.web.cern.ch/api/files/281dfbed-2a2e-425b-9195-959173fa9f64/test_video.mp4", {
  "headers": {
    "authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2OTAyOTg3MzAsInN1YiI6IjE3IiwianRpIjoiYTMzYmUxZTItNDExOS00OTU2LWE5YWEtODQxYmI0NDdmZGQxIn0._7qOceBnhZp94YZZ7ngKIcGfz4fIPLR17r35iYRTRAg",
    "content-type": "video/mp4",
  },
  "method": "PUT"
});

FLOW
fetch("https://sandbox-videos.web.cern.ch/api/flows/", {
  "headers": {
    "authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2OTAyOTg3MzAsInN1YiI6IjE3IiwianRpIjoiYTMzYmUxZTItNDExOS00OTU2LWE5YWEtODQxYmI0NDdmZGQxIn0._7qOceBnhZp94YZZ7ngKIcGfz4fIPLR17r35iYRTRAg",
    "content-type": "application/json",
  },
  "body": "{\"version_id\":\"55b65bc4-0580-4961-ad36-b26bffe76c0f\",\"key\":\"test_video.mp4\",\"bucket_id\":\"281dfbed-2a2e-425b-9195-959173fa9f64\",\"deposit_id\":\"9b2d22439fc14c68894d11d9be592d33\"}",
  "method": "POST"
});
'''