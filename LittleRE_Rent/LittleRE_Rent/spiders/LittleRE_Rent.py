##############          LittleRE_Rent

import random
import mimetypes
import re
from copy import deepcopy

from botocore.exceptions import NoCredentialsError, ClientError
import requests, json, time, threading, queue, os
import boto3
import pymongo
from datetime import datetime
from botocore.exceptions import NoCredentialsError
import scrapy

class LittleRE_Rent(scrapy.Spider):
    name = "LittleRE_Rent"

    url = "https://mdaacomn2x-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(3.35.1)%3B%20Browser%20(lite)%3B%20instantsearch.js%201.12.1%3B%20JS%20Helper%20(2.28.1)&x-algolia-application-id=MDAACOMN2X&x-algolia-api-key=fedecca30d22b0d21b3be6943d055984"
    prefix = "https://www.littlerealestate.com.au"
    headers = {
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Origin': 'https://www.littlerealestate.com.au',
    'Referer': 'https://www.littlerealestate.com.au/sold',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    'accept': 'application/json',
    'content-type': 'application/x-www-form-urlencoded',
    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

    params = "query=&maxValuesPerFacet=20&page={}&facets=%5B%22type%22%2C%22location.state%22%2C%22propertyStatus%22%2C%22surroundingSuburbs%22%2C%22suburbStateFacet%22%2C%22location.state%22%2C%22location.region%22%2C%22title%22%2C%22type%22%2C%22category%22%2C%22price%22%2C%22bedrooms%22%2C%22bathrooms%22%2C%22carports%22%2C%22isPetsAllowed%22%2C%22isFurnished%22%5D&tagFilters=&facetFilters=%5B%5B%5D%2C%5B%22type%3Arent%22%5D%5D"
    data = {
  "requests": [
    {
      "indexName": "properties_suburb",
      "params": "query=&maxValuesPerFacet=20&page=1&facets=%5B%22type%22%2C%22location.state%22%2C%22propertyStatus%22%2C%22surroundingSuburbs%22%2C%22suburbStateFacet%22%2C%22location.state%22%2C%22location.region%22%2C%22title%22%2C%22type%22%2C%22category%22%2C%22price%22%2C%22bedrooms%22%2C%22bathrooms%22%2C%22carports%22%2C%22isPetsAllowed%22%2C%22isFurnished%22%5D&tagFilters=&facetFilters=%5B%5B%5D%2C%5B%22type%3Arent%22%5D%5D"
    },
    {
      "indexName": "properties_suburb",
      "params": "query=&maxValuesPerFacet=20&page=0&hitsPerPage=1&attributesToRetrieve=%5B%5D&attributesToHighlight=%5B%5D&attributesToSnippet=%5B%5D&tagFilters=&analytics=false&clickAnalytics=false&facets=propertyStatus&facetFilters=%5B%5B%22type%3Arent%22%5D%5D"
    },
    {
      "indexName": "properties_suburb",
      "params": "query=&maxValuesPerFacet=20&page=0&hitsPerPage=1&attributesToRetrieve=%5B%5D&attributesToHighlight=%5B%5D&attributesToSnippet=%5B%5D&tagFilters=&analytics=false&clickAnalytics=false&facets=type&facetFilters=%5B%5B%5D%5D"
    }
  ]
}

    count = 1
    db = 'LittleRE'
    collection = 'LittleRE_Rent'
    # bucket_prefix = f'P_{collection}'
    bucket_prefix = f'Images9'

    local_file_path = '/img'

    def __init__(self, *args, **kwargs):
        super(LittleRE_Rent, self).__init__(*args, **kwargs)
        self.links = []
        self.database_sale_matching_url = self.read_data_base()


    '''    SCRPAY SCRIPT START'S FROM HERE     '''
    def start_requests(self):
        page_no = 1
        offset = 0
        payloads = deepcopy(self.data)
        payloads['requests'][0]['params'] = self.params.format(page_no)
        yield scrapy.Request(self.url, method='POST', body=json.dumps(payloads), headers=self.headers, callback=self.parse,
                                 meta={'page_no':page_no,'offset':offset})

    def parse(self, response):
        '''    FARHAN'S LOGIC    '''
        data = json.loads(response.text)
        all_links = data['results'][0]['hits']
        links = []
        for each_url in all_links:
            url_slug = each_url.get('uri','')
            each_url = f'https://www.littlerealestate.com.au/{url_slug}'
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

        offset = response.meta['offset']
        page_no = response.meta['page_no']
        for each_url in all_links:
            offset += 1
            property = each_url
            url_slug = each_url.get('uri','')
            each_url = f'https://www.littlerealestate.com.au/{url_slug}'
            print(self.count,' # Property URL :', each_url)
            self.count += 1
            yield response.follow(each_url, headers=self.headers, callback=self.detail_parse, meta={'property':property})

        total_pages = data['results'][0]['nbPages']
        if page_no < total_pages:
            page_no += 1
            print("Next Page is :", page_no)
            payloads = deepcopy(self.data)
            payloads['requests'][0]['params'] = self.params.format(page_no)
            yield scrapy.Request(self.url, method='POST', body=json.dumps(payloads), headers=self.headers, callback=self.parse,
                                 meta={'page_no':page_no,'offset':offset})


    def detail_parse(self, response):
        property = response.meta['property']
        detail_page_url = response.url
        database_sale_matching_url = self.read_data_base()
        if detail_page_url not in database_sale_matching_url:
            item = dict()
            item['Field1'] = ''
            item['Field2'] = int(2619)
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'Little Real Estate'

            property_street1 = property.get('location',{}).get('street2')
            property_street = property.get('location',{}).get('street1')
            property_city = property.get('location',{}).get('city')
            property_state = property.get('location',{}).get('state')
            property_address = f"{property_street1} {property_street} {property_city} {property_state}"
            item['Field5'] = property_address  # full_adddress

            # item['Field6'] = response.xpath("//img[@alt='Bedroom']/following-sibling::text()[1]").get('').strip()     ## for bulship tags
            # item['Field6'] = response.xpath("//*[@class='property-details__headline']/following-sibling::text()[1]").get('').strip()     ## for bulship tags
            item['Field6'] = property.get('bedrooms','')
            item['Field7'] = property.get('bathrooms','')
            item['Field8'] = property.get('carports','')

            price = response.css('div.property-details__price ::text').get('').strip().replace('Sold','').replace('for','').replace('$','').replace(' ','').replace(',','')
            if re.search(r'\d', price):
                item['Field9'] = price.strip().replace('SOLD','').replace('$','').replace('Contact','').replace('Agent','')

            description_head = ' '.join(element.strip().replace('\t', ' ').replace('\r', ' ').replace('\n', ' ') for element in response.css('.property-details__headline ::text').getall())  # description
            detailed_desc = ' '.join(element.strip().replace('\t', ' ').replace('\r', ' ').replace('\n', ' ') for element in response.xpath("//*[@class='property-details__headline']/following-sibling::text()").getall())  # description
            if description_head or detailed_desc:
                item['Field12'] = f"{description_head} {detailed_desc}"  # description

            '''         Uploading Images on Wasabi S3 Bucket            '''
            prop_images = property.get('images',[])
            images = []
            for img in prop_images[:25]:        ## Scrape maximum 25 photos
                img = img.replace('background-image:url(','').replace(");","")
                images.append(f'{img}' if img else "")
            images_string = ', '.join(images)
            # item['Field13'] = images_string
            # print('Property images are :', images_string)
            random_number = random.randint(1, 10000000000)
            item['Field13'] = ", ".join(self.uploaded(images_string, random_number))

            item['Field14'] = response.url  # external_link
            '''          AGENTS          '''
            if response.css(".agent"):
                agent1 = response.css(".agent")[0]
                agent1_name = agent1.css('.agent__name a ::text').get('').strip()
                if agent1_name:
                    first_name1, last_name1 = agent1_name.split(maxsplit=1)
                    item['Field15'] = first_name1.strip()  # agent_first_name_1
                    item['Field16'] = last_name1.strip()  # agent_surname_name_1
                    item['Field17'] = agent1_name.strip()  # agent_full_name_1
                    item['Field18'] = agent1.css('.agent__position ::text').get('').strip()
                    item['Field19'] = agent1.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:','')
                    item['Field20'] = response.xpath("//a[contains(@href,'tel:') and contains(@class,'agent__phone')]/@href").get('').strip().replace('tel:','')
                    phone_nos = response.xpath("//a[contains(@href,'tel:') and contains(@class,'agent__phone')]/@href").getall()
                    if len(phone_nos) > 1:
                        item['Field21'] = phone_nos[-1].strip().replace('tel:','')
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    agent_image = agent1.css("img ::attr(src)").get('').strip().replace('background-image:url(','').replace(')','')
                    images_string = f'{agent_image}' if agent_image else ""
                    # item['Field22'] = images_string
                    # print('Agent1 image is :',images_string)
                    random_number = random.randint(1, 10000000000)
                    item['Field22'] = ",".join(self.uploaded(images_string, random_number))

            if len(response.css(".agent").getall()) > 1:
                agent2 = response.css(".agent")[1]
                agent2_name = agent2.css('.agent__name a ::text').get('').strip()
                if agent2:
                    item['Field23'] = agent2_name.strip()
                    item['Field24'] = agent2.css('.agent__position ::text').get('').strip()
                    item['Field25'] = agent2.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:', '')
                    item['Field26'] = agent2.xpath(".//a[contains(@href,'tel:')]/@href").get('').strip().replace('tel:','')
                    phone_nos = agent2.xpath(".//a[contains(@href,'tel:')]/@href").getall()
                    if len(phone_nos) > 1:
                        item['Field26A'] = phone_nos[-1].strip().replace('tel:','')
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    agent_image = agent2.css("img ::attr(src)").get('').strip().replace('background-image:url(','').replace(')','')
                    images_string = f'{agent_image}' if agent_image else ""
                    # item['Field27'] = images_string
                    # print('Agent2 image is :',images_string)
                    random_number = random.randint(1, 10000000000)
                    item['Field27'] = ",".join(self.uploaded(images_string, random_number))

            if len(response.css(".agent").getall()) > 2:
                agent3 = response.css(".agent")[2]
                agent3_name = agent3.css('.agent__name a ::text').get('').strip()
                if agent3:
                    item['Field28'] = agent3_name.strip()
                    item['Field29'] = agent3.css('.agent__position ::text').get('').strip()
                    item['Field30'] = agent3.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:', '')
                    item['Field31'] = agent3.xpath(".//a[contains(@href,'tel:')]/@href").get('').strip().replace('tel:','')
                    phone_nos = agent3.xpath(".//a[contains(@href,'tel:')]/@href").getall()
                    if len(phone_nos) > 1:
                        item['Field31A'] = phone_nos[-1].strip().replace('tel:','')
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    agent_image = agent3.css("img ::attr(src)").get('').strip().replace('background-image:url(','').replace(')','')
                    images_string = f'{agent_image}' if agent_image else ""
                    # item['Field32'] = images_string
                    # print('Agent3 image is :',images_string)
                    random_number = random.randint(1, 10000000000)
                    item['Field32'] = ", ".join(self.uploaded(images_string, random_number))


            item['Field33'] = property.get('objectID','')
            item['Field35'] = property.get('category','')

            # land_area = response.xpath("//*[contains(text(),'Land Size')]/following-sibling::*[1]/text()").get('').strip()
            # if land_area:
            #     if 'Acres' in land_area:
            #         pass
            #     else:
            #         item['Field36'] = land_area.replace('Land','').replace('is','').replace('m²','').replace('sqm','').replace('Sqm','').replace(' ','')   # land_area
            # floor_area = response.xpath('//*[contains(text(),"Floor area")]/following-sibling::span[1]/text()').get('').strip()
            # if floor_area:
            #     if 'Acres' in floor_area:
            #         pass
            #     else:
            #         item['Field37'] = floor_area.replace('Sqm','').replace(' ','')   # land_area
            #
            # feature_count = 58
            # for feature in response.css(".epl-property-features li"):   ##  first 4 features are ID, bed, bath etc.
            #     if 'condition' in feature.css('::text').get('').strip().lower():
            #         item['Field52'] = feature.css('::text').get('').strip()
            #     else:
            #         item[f'Field{feature_count}'] = feature.css('::text').get('').strip()
            #         feature_count += 1

            ####        ACTIVE / REMOVED Logic implemented in "Field102"
            item['Field102'] = 'ACTIVE' if 'Sale' in self.name or 'Rent' in self.name else None
            item['Field104'] = response.url
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


