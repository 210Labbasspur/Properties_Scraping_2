#####################           MarshallWhite_Sold

import random
import mimetypes
import re
from botocore.exceptions import NoCredentialsError, ClientError
import requests, json, time, threading, queue, os
import boto3
import pymongo
from datetime import datetime
from botocore.exceptions import NoCredentialsError
import scrapy

class MarshallWhite_Sold(scrapy.Spider):
    name = "MarshallWhite_Sold"

    url = ("https://www.marshallwhite.com.au/___filter?q=%7B%22orderBy%22%3A%22_updatedAt_DESC%22%2C%22filter%22%3A%7B%22type%22%3A%22DatoCmsProperty%22%2C%22fields%22%3A%7B%22for%22%3A%7B%22eq%22%3A%22sold%22%7D%7D%7D%7D&"
           "page={}&perPage=100&orderByType=desc")
    prefix = "https://www.marshallwhite.com.au"
    headers = {
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    }

    count = 1
    db = 'MarshallWhite'
    collection = 'MarshallWhite_Sold'
    # bucket_prefix = f'P_{collection}'
    bucket_prefix = f'Images1'

    local_file_path = '/img'

    def __init__(self, *args, **kwargs):
        super(MarshallWhite_Sold, self).__init__(*args, **kwargs)
        self.links = []
        self.database_sale_matching_url = self.read_data_base()


    '''    SCRPAY SCRIPT START'S FROM HERE     '''
    def start_requests(self):
        page_no = 1
        offset = 0
        yield scrapy.Request(self.url.format(page_no), headers=self.headers, callback=self.parse, meta={'offset':offset,'page_no':page_no,})

    def parse(self, response):
        page_no = response.meta['page_no']
        offset = response.meta['offset']
        '''    FARHAN'S LOGIC    '''
        data = json.loads(response.text)
        all_links = data.get('nodes',[])
        links = []
        for each_url in all_links:
            each_url = f"https://www.marshallwhite.com.au{each_url.get('permalink')}"
            links.append(each_url)
            self.links.append(each_url)

        for each_db_detail_page_url in self.database_sale_matching_url:
            if each_db_detail_page_url in links:
                new_data = {
                    "Field102": "ACTIVE" if 'Sale' in self.name or 'Rent' in self.name else None,
                    "Field104": each_db_detail_page_url,
                }
                self.update_database(each_db_detail_page_url, new_data, 'true')
                index_to_remove = links.index(each_db_detail_page_url)
                links.pop(index_to_remove)

            elif each_db_detail_page_url not in self.links:
                new_data = {
                    "Field102": "REMOVED" if 'Sale' in self.name or 'Rent' in self.name else None,
                    "Field104": each_db_detail_page_url,
                }
                self.update_database(each_db_detail_page_url, new_data, 'false')


        for property in all_links:
            offset += 1
            each_url = f"https://www.marshallwhite.com.au{property.get('permalink')}"
            print(self.count,' # Property URL :', each_url)
            self.count += 1
            self.detail_parse(property)

        total_count = data.get('pageInfo',{}).get('totalCount')
        total_pages = data.get('pageInfo',{}).get('pageCount')
        print(f"Current-offset :", offset, f' || Total_count = {total_count} || Current-Page = {page_no} || Total-Pages = {total_pages}')
        if offset < total_count:
            page_no += 1
            yield scrapy.Request(self.url.format(page_no), headers=self.headers, callback=self.parse, meta={'offset': offset, 'page_no': page_no, })

    def detail_parse(self, property):
        detail_page_url = f"https://www.marshallwhite.com.au{property.get('permalink')}"
        database_sale_matching_url = self.read_data_base()
        if detail_page_url not in database_sale_matching_url:
            item = dict()
            item['Field1'] = ''
            item['Field2'] = int(2382)
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'Marshall White'

            streetNumber = property.get('addressComponents')[0].get('streetNumber','') if property.get('addressComponents') else ''
            street = property.get('addressComponents')[0].get('street','') if property.get('addressComponents') else ''
            city = property.get('addressComponents')[0].get('city','') if property.get('addressComponents') else ''
            postCode = property.get('addressComponents')[0].get('postCode','') if property.get('addressComponents') else ''
            property_address = f"{streetNumber} {street} {city} {postCode}"
            item['Field5'] = property_address  # full_adddress

            # item['Field6'] = response.xpath("//img[@alt='Bedroom']/following-sibling::text()[1]").get('').strip()     ## for bulship tags
            item['Field6'] = property.get('beds','')
            item['Field7'] = property.get('baths','')
            item['Field8'] = property.get('allCarSpaces','')

            price = property.get('price','')
            if re.search(r'\d', price):
                item['Field9'] = price.strip().replace('SOLD','').replace('$','').replace('Contact','').replace('Agent','')
            # item['Field11'] = property.get('soldDate','')
            item['Field11'] = property.get('remoteDiff', {}).get('sold_details_sold_date', '')
            item['Field12'] = property.get('remoteDiff',{}).get('description','')

            '''         Uploading Images on Wasabi S3 Bucket            '''
            prop_images = property.get('remoteDiff',{}).get('objects',{}).get('img',[])
            images = []
            for img in prop_images[:25]:        ## Scrape maximum 25 photos
                img = img.replace('background-image:url(','').replace(");","")
                images.append(f'{img}' if img else "")
            images_string = ', '.join(images)
            # item['Field13'] = images_string
            # print('Property images are :', images_string)
            random_number = random.randint(1, 10000000000)
            item['Field13'] = ", ".join(self.uploaded(images_string, random_number))

            item['Field14'] = f"https://www.marshallwhite.com.au{property.get('permalink')}"
            '''          AGENTS          '''

            agent1_name = property.get('remoteDiff',{}).get('agent_name_1','')
            if agent1_name:
                first_name1, last_name1 = agent1_name.split(maxsplit=1)
                item['Field15'] = first_name1.strip()  # agent_first_name_1
                item['Field16'] = last_name1.strip()  # agent_surname_name_1
                item['Field17'] = agent1_name.strip()  # agent_full_name_1
                item['Field18'] = property.get('remoteDiff',{}).get('agent_position_1','')
                item['Field19'] = property.get('remoteDiff',{}).get('agent_email_1','')
                item['Field20'] = property.get('remoteDiff',{}).get('agent_mobile_1','')
                item['Field21'] = property.get('remoteDiff',{}).get('agent_phone_1','')

            agent2_name = property.get('remoteDiff',{}).get('agent_name_2','')
            if agent2_name:
                item['Field23'] = agent2_name.strip()
                item['Field24'] = property.get('remoteDiff',{}).get('agent_position_2','')
                item['Field25'] = property.get('remoteDiff',{}).get('agent_email_2','')
                item['Field26'] = property.get('remoteDiff',{}).get('agent_mobile_2','')
                item['Field26A'] = property.get('remoteDiff',{}).get('agent_phone_2','')

            agent3_name = property.get('remoteDiff',{}).get('agent_name_3','')
            if agent3_name:
                item['Field28'] = agent3_name.strip()
                item['Field29'] = property.get('remoteDiff',{}).get('agent_position_3','')
                item['Field30'] = property.get('remoteDiff',{}).get('agent_email_3','')
                item['Field31'] = property.get('remoteDiff',{}).get('agent_mobile_3','')
                item['Field31A'] = property.get('remoteDiff',{}).get('agent_phone_3','')


            item['Field33'] = property.get('id','')
            item['Field35'] = property.get('remoteDiff', {}).get('property_type', '')

            land_area = property.get('remoteDiff', {}).get('listing_area',[])[0]['area'] if property.get('remoteDiff', {}).get('listing_area',[]) else ''
            if land_area:
                if 'Acres' in land_area:
                    pass
                else:
                    item['Field36'] = land_area.replace('Land','').replace('is','').replace('m²','').replace('sqm','').replace('Sqm','').replace(' ','')   # land_area
            # floor_area = response.xpath('//*[contains(text(),"Floor area")]/following-sibling::span[1]/text()').get('').strip()
            # if floor_area:
            #     if 'Acres' in floor_area:
            #         pass
            #     else:
            #         item['Field37'] = floor_area.replace('Sqm','').replace(' ','')   # land_area
            #
            feature_count = 58
            features = property.get('remoteDiff', {}).get('features',[])
            for feature in features:   ##  first 4 features are ID, bed, bath etc.
                if 'condition' in feature.lower():
                    item['Field52'] = feature
                else:
                    item[f'Field{feature_count}'] = feature
                    feature_count += 1

            ####        ACTIVE / REMOVED Logic implemented in "Field102"
            item['Field102'] = 'ACTIVE' if 'Sale' in self.name or 'Rent' in self.name else None
            item['Field104'] = f"https://www.marshallwhite.com.au{property.get('permalink')}"
            print(item)
            self.insert_database(item)
        else:
            print(f'Skipping Record {detail_page_url}, since it already exists in the Data Base  !!!!! ')


    def read_data_base(self):
    # def read_data_base(self, profileUrl):
        connection_string = 'mongodb://localhost:27017'
        conn = pymongo.MongoClient(connection_string)
        db = conn[self.db]
        collection = db[self.collection]

        sale_urls_list_of_DB = []
        all_matching_data = collection.find()
        for each_row in all_matching_data:
            # sale = each_row.get('Field104')
            sale = each_row.get('Field14')
            sale_urls_list_of_DB.append(sale)
        return sale_urls_list_of_DB


    def insert_database(self, new_data):
        connection_string = 'mongodb://localhost:27017'
        conn = pymongo.MongoClient(connection_string)
        db = conn[self.db]
        collection = db[self.collection]
        collection.insert_one(new_data)
        print("Data inserted successfully!")

    def update_database(self, profileUrl, new_data, area):
        connection_string = 'mongodb://localhost:27017'
        conn = pymongo.MongoClient(connection_string)
        db = conn[self.db]
        collection = db[self.collection]

        # search_query = {"Field104": profileUrl}
        search_query = {"Field14": profileUrl}

        update_query = {
            "$set": {
                # "Field3": new_data["Field3"],
                # "Field102": new_data["Field102"],
                "Field102": new_data.get("Field102", ""),  # This will return empty string if key doesn't exist
                "Field104": new_data["Field104"]
            }
        }
        collection.update_one(search_query, update_query, upsert=True)
        print(f"Data updated successfully! + {area} : ",search_query)


    '''    HANDLING IMAGES - WASABI     '''
    def download_image(self, img_url, file_path, name):
        try:
            response = requests.request(method='GET', url=img_url)
            if response.status_code == 200:
                # sanitized_name = self.sanitize_filename(name)
                with open(f'{file_path}/{name}', 'wb') as file:
                    file.write(response.content)
                print(f"{name} Image downloaded successfully.")
            else:
                print("Failed to download the image. Status code:", response.status_code)
        except requests.exceptions.RequestException as e:
            print("An error occurred while downloading:", e)
        except Exception as e:
            print(e)

    def create_new_bucket(self, bucket_prefix, bucket_number, s3):
        new_bucket_name = f'{bucket_prefix}_{bucket_number}'
        s3.create_bucket(Bucket=new_bucket_name)
        return new_bucket_name

    def uploaded(self, list_of_img, names):
        list_images = [url.strip() for url in list_of_img.split(',')]
        wasabi_access_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        wasabi_secret_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        s3 = boto3.client(
            's3',
            aws_access_key_id=wasabi_access_key,
            aws_secret_access_key=wasabi_secret_key,
            endpoint_url="https://s3.ap-southeast-1.wasabisys.com",
        )
        # bucket_prefix = 'BLS'
        bucket_prefix = self.bucket_prefix
        bucket_number = 1
        current_bucket_name = f'{bucket_prefix}_{bucket_number}'

        # existing_buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
        existing_buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
        if current_bucket_name in existing_buckets:
            if current_bucket_name != self.create_new_bucket(bucket_prefix, bucket_number, s3):
                print("Bucket already exists. Create Buket and Run...")
                return

        current_bucket_name = self.create_new_bucket(bucket_prefix, bucket_number, s3)
        print(f"using bucket: {current_bucket_name}")
        try:
            object_count = len(s3.list_objects(Bucket=current_bucket_name).get('Contents', []))
        except s3.exceptions.NoSuchBucket:
            current_bucket_name = self.create_new_bucket(bucket_prefix, bucket_number, s3)
            object_count = 0
        wasabi_url = []

        # create image folder automacity
        current_directory = os.getcwd()
        image_folder = 'images'
        image_directory = os.path.join(current_directory, image_folder)
        os.makedirs(image_directory, exist_ok=True)

        for index, img in enumerate(list_images, start=1):
            try:
                image_url = img
                # local_file_path = 'C:/Users/Jahanzaib/Desktop/img/'
                local_file_path = os.path.join(image_directory).replace('\\', '/')

                title_name = f'{names}_{index}.jpg'

                img_url = f'https://s3.ap-southeast-1.wasabisys.com/{current_bucket_name}/{title_name}'
                if image_url:
                    self.download_image(image_url, local_file_path, title_name)
                    # download_image(image_url,a)

                if object_count >= 10000000:
                    bucket_number += 1
                    current_bucket_name = self.create_new_bucket(bucket_prefix, bucket_number, s3)
                    object_count = 0

                bucket_policy = {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                            "Resource": f"arn:aws:s3:::{current_bucket_name}/*"
                        }
                    ]
                }

                s3.put_bucket_policy(Bucket=current_bucket_name, Policy=json.dumps(bucket_policy))
                # uploading
                file_path_on_disk = os.path.join(local_file_path, title_name)

                try:
                    content_type, _ = mimetypes.guess_type(file_path_on_disk)
                    s3.upload_file(
                        file_path_on_disk,
                        current_bucket_name,
                        title_name,
                        ExtraArgs={'ContentType': content_type} if content_type else None
                    )
                    self.delete_local_image(file_path_on_disk)
                except NoCredentialsError:
                    print("Credentials not available")
                except ClientError as e:
                    print(f"An error occurred: {e}")
                object_count += 1
                wasabi_url.append(img_url)
            except Exception as e:
                print(e)
        return wasabi_url

    def delete_local_image(self, file_path):
        try:
            os.remove(file_path)
        except OSError as e:
            print(f"Error deleting local image: {e}")


