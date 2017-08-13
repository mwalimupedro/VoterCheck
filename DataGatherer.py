import logging

#import urllib.request
#from lxml import html
import requests
#import html.parser
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import json
import re

class DataGatherer(object):

    def __init__(self):
        # Configure Logging
        logging.basicConfig(level=logging.INFO)
        # logging.basicConfig(level=logging.WARNING)
        self.logger = logging.getLogger(__name__)
        # self.logger.setLevel(logging.INFO)
        self.logger.setLevel(logging.DEBUG)
        # self.logger.setLevel(logging.WARNING)

        self.forms_base_url = "https://forms.iebc.or.ke/"
        #self.storage_base_url = "https://forms.iebc.or.ke/storage/images/1_1_047_277_1381_004_01.jpeg"

        #self.main_county_list = []

    def get_all_children_per_parent(self, child_item, parent):
        #example_url = "https://forms.iebc.or.ke/160/wards?data=160"
        child_item_without_underscore = child_item.replace('_', '')
        wards_param = child_item_without_underscore+'?data='
        # Get list of constituencies from directory of Constituencies per County
        all_parent_id_list = self.get_all_parent_ids(parent)

        extension = '.json'
        for parent_id in all_parent_id_list:
            #if parent_id[0] == '445':
            if child_item == 'polling_station':
                key_param = parent_id[0]+ '-' + parent_id[2]
            else:
                key_param = parent_id[0]
            full_wards_url =  self.forms_base_url + '/'+ key_param + '/'+ wards_param + key_param
            self.logger.debug('WARDS from CONSTITUENCY url: {}'.format(full_wards_url))
            resp = requests.get(full_wards_url)

            chars = set("\\/")
            #if any(["\\",'/']) in parent_id[1]
            if any((c in chars) for c in parent_id[1]):
                #fixed_parent_name = parent_id[1].replace('/', '_')  #Fixing consituency names with '/' E.g Chuka/Igambang'ombe
                fixed_parent_name = re.sub('[\\\\/:*?"<>|]', '_', parent_id[1])
                file_name = key_param+ '_' + fixed_parent_name + '_' + child_item + extension
            else:
                file_name = key_param + '_' + parent_id[1] + '_' + child_item + extension
            # fixed_parent_name = parent_id[1].replace('\\', '_')  #
            # file_name = parent_id[0] + '_' + fixed_parent_name + '_' + child_item + extension
            temp_filepath = os.path.join('data', child_item , file_name)
            if resp.ok:
                if resp.headers.get('content-length'):
                    total_size = int(resp.headers.get('content-length'))
                    self.logger.info('File Length: {}'.format(total_size))
                else:
                    self.logger.debug('Headers: {}'.format(resp.headers))
                with open(temp_filepath, 'wb') as file_handle:
                    # if the file is large we do the writing in chunks
                    # tqdm is a library for a portable progressbar (Default unit is "it" --> number of iterations)
                    #for block in tqdm(resp.iter_content(1024), unit='B', total=total_size / 1024, unit_scale=True):
                    for block in tqdm(resp.iter_content(1024), unit='B', unit_scale=True):
                        file_handle.write(block)

            else:
                self.logger.error(
                    "Something went wrong with the file download: {} - {}".format(resp.status_code, resp.reason))

        return

    def get_all_parent_ids(self, parent):
        temp_file_path = os.path.join('data', parent)
        parent_files = os.listdir(temp_file_path)
        self.logger.debug('Number of files: {}'.format(len(parent_files)))

        parent_id_list = []
        for file_name in parent_files:
            grand_parent_id = file_name.split('_')[0]
            file_path = os.path.join(temp_file_path, file_name)
            with open(file_path, 'r') as json_file:
                json_items = json.load(json_file)
                self.logger.debug('Number of ITEMS: {}'.format(len(json_items[parent])))
                # self.logger.debug('Type of ITEMS: {}'.format(type(json_items['constituency'])))
                # self.logger.debug('ITEMS: {}'.format(json_items['constituency']))
                # self.logger.debug('First ITEM: {}'.format(json_items['constituency'].items()[0]))
                for parent_id, parent_name in json_items[parent].items():
                    self.logger.debug('{}: {}'.format(parent, parent_id))
                    # constit_id_list.append({constit_id : constit_name})
                    parent_id_list.append((parent_id, parent_name, grand_parent_id))

        self.logger.debug('First ITEM in {} list: {}'.format(parent.upper(), parent_id_list[0]))
        self.logger.debug('First ITEM in {} list: {}'.format(parent.upper(), parent_id_list[1]))
        self.logger.debug('First ITEM in {} list: {}'.format(parent.upper(), parent_id_list[40774]))
        self.logger.debug("Total Number of {}: {}".format(parent.upper(), len(parent_id_list)))
        # for
        return parent_id_list

    def get_full_image_id(self):
        # Example URL: https://forms.iebc.or.ke/download/28015-1_1_037_199_0995_064_01.jpeg
        # Example URL: https://forms.iebc.or.ke/download/40774-1_1_048_291_5001_003_01.jpeg -UG pstation01
        # Example URL: https://forms.iebc.or.ke/download/40775-1_1_048_291_5001_003_02.jpeg =UG pstation02
        # for county_id in range(1,48):
        county_list_path = os.path.join('data', 'constituency')
        self.logger.debug('County Path: {}'.format(county_list_path))
        county_list = os.listdir(county_list_path)
        self.logger.debug('Number of Counties: {}'.format(len(county_list)))

        all_poll_stations_in_order = []
        all_poll_station_image_ids = []
        poll_station_key = 0
        for index, county in enumerate(county_list):
            county_id = county.split('_')[0]
            self.logger.debug('County_ID: {}'.format(county_id))
            constituency_file_path = os.path.join(county_list_path, county)
            with open(constituency_file_path, 'r') as json_constituency_file:
                json_constituency = json.load(json_constituency_file)
                for constituency_id, constituency_name in json_constituency['constituency'].items():
                    fixed_constituency_name = re.sub('[\\\\/:*?"<>|]', '_', constituency_name)
                    self.logger.debug('Constituency ID: {} - Name: {}'.format(constituency_id, fixed_constituency_name))
                    ward_file_name = constituency_id + '_' + fixed_constituency_name + '_wards.json'
                    ward_file_path = os.path.join('data', 'wards', ward_file_name)
                    self.logger.debug('Ward File Path: {}'.format(ward_file_path))
                    with open(ward_file_path, 'r') as json_wards_file:
                        json_wards = json.load(json_wards_file)
                        for ward_id, ward_name in json_wards['wards'].items():
                            fixed_ward_name = re.sub('[\\\\/:*?"<>|]', '_', ward_name)
                            self.logger.debug('Ward ID: {} - Name: {}'.format(ward_id, fixed_ward_name))
                            poll_centre_file_name = ward_id + '_' + fixed_ward_name + '_polling_centre.json'
                            poll_centre_path = os.path.join('data', 'polling_centre', poll_centre_file_name)
                            self.logger.debug('Poll Centre Path: {}'.format(poll_centre_path))
                            with open(poll_centre_path, 'r') as json_poll_centre_file:
                                json_poll_centres = json.load(json_poll_centre_file)
                                for poll_centre_id, poll_centre_name in json_poll_centres['polling_centre'].items():
                                    fixed_poll_centre_name = re.sub('[\\\\/:*?"<>|]', '_', poll_centre_name)
                                    self.logger.debug('Polling Centre ID: {} - Name: {}'.format(poll_centre_id,
                                                                                                fixed_poll_centre_name))
                                    poll_station_file_name = poll_centre_id + '-' + ward_id + '_' + fixed_poll_centre_name + '_polling_station.json'
                                    poll_station_path = os.path.join('data', 'polling_station', poll_station_file_name)
                                    self.logger.debug('Poll Station Path: {}'.format(poll_station_path))
                                    with open(poll_station_path, 'r') as json_poll_station_file:
                                        json_poll_stations = json.load(json_poll_station_file)
                                        for poll_station_id, poll_station_name in json_poll_stations[
                                            'polling_station'].items():
                                            fixed_poll_station_name = re.sub('[\\\\/:*?"<>|]', '_', poll_station_name)
                                            self.logger.debug(
                                                'Polling Station ID: {} - Name: {}'.format(poll_station_id,
                                                                                           fixed_poll_station_name))
                                            poll_station_key += 1

                                            # image_id = str(county_id) + '_' + constituency_id + '_' + ward_id + '_' + poll_centre_id + '_' + poll_station_id
                                            image_id = '_'.join(
                                                [str(poll_station_key) + '-1_1', county_id, constituency_id.zfill(3),
                                                 ward_id.zfill(4),
                                                 poll_centre_id.zfill(3), poll_station_id])
                                            self.logger.debug('Image ID: {}'.format(image_id))
                                            all_poll_stations_in_order.append(
                                                {'key': poll_station_key, 'id': poll_station_id,
                                                 'name': fixed_poll_station_name,
                                                 'image_id': image_id})
                                            self.logger.debug(
                                                'Poll Station = Key: {} | ID: {} | Name: {} | Image_id: {}'.format(
                                                    poll_station_key, poll_station_id,
                                                    fixed_poll_station_name,
                                                    image_id))

                                            # Download Image
                                            self.download_image(image_id)

        return

    def download_image(self, image_id):
        # Example URL: https://forms.iebc.or.ke/download/28015-1_1_037_199_0995_064_01.jpeg
        # Example URL: https://forms.iebc.or.ke/download/40774-1_1_048_291_5001_003_01.jpeg -UG pstation01
        # Example URL: https://forms.iebc.or.ke/download/40775-1_1_048_291_5001_003_02.jpeg =UG pstation02
        # SUCCESS --> https://forms.iebc.or.ke/download/1-1_1_001_001_0001_001_01.jpeg
        # SUCCESS --> https://forms.iebc.or.ke/download/38037-1_1_047_277_1381_004_05.jpeg
        # SUCCESS --> https://forms.iebc.or.ke/download/38038-1_1_047_277_1381_004_06.jpeg
        # FAIL --> https://forms.iebc.or.ke/download/38038-1_1_047_277_1381_004_07.jpeg

        full_image_url = 'https://forms.iebc.or.ke/download/' + image_id + '.jpeg'
        self.logger.debug('Image URL: ()'.format(full_image_url))
        resp = requests.get(full_image_url)

        file_name = image_id + '.jpeg'
        temp_filepath = os.path.join('data', 'IMAGES', file_name)
        if resp.ok:
            if resp.headers.get('content-length'):
                total_size = int(resp.headers.get('content-length'))
                self.logger.info('File Length: {}'.format(total_size))

                with open(temp_filepath, 'wb') as file_handle:
                    # if the file is large we do the writing in chunks
                    # tqdm is a library for a portable progressbar (Default unit is "it" --> number of iterations)
                    for block in tqdm(resp.iter_content(1024), unit='B', total=total_size / 1024, unit_scale=True):
                        # for block in tqdm(resp.iter_content(1024), unit='B', unit_scale=True):
                        file_handle.write(block)
            else:
                self.logger.debug('Headers: {}'.format(resp.headers))
                self.logger.debug('Polling Station Missing Image: {}'.format(image_id))
                txt_file_path = temp_filepath + '.txt'
                open(txt_file_path, 'a').close()
                # with open(txt_file_path, 'a'):
                #     os.utime(txt_file_path, None)

        else:
            self.logger.error(
                "Something went wrong with the file download: {} - {}".format(resp.status_code, resp.reason))
