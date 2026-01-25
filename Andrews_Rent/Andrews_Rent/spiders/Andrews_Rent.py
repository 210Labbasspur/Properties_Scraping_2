################            Andrews_Rent

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

class Andrews_Rent(scrapy.Spider):
    name = "Andrews_Rent"

    # url = 'https://www.andrewsproperty.com.au/residential-sales.cfm'
    url = 'https://www.andrewsproperty.com.au/residential-sales.cfm?page={}'
    prefix = "https://www.andrewsproperty.com.au"
    headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'content-type': 'application/x-www-form-urlencoded',
    'origin': 'https://www.andrewsproperty.com.au',
    'priority': 'u=0, i',
    'referer': 'https://www.andrewsproperty.com.au/andrews-property-buy-a-property.cfm',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    # 'cookie': '_gid=GA1.3.1975745155.1739853132; hubspotutk=c3be6da8ede3051b7d5719442b0670ff; __hssrc=1; messagesUtk=868b903a18f74fcd89f7ba34df7bc002; JSESSIONID=098A956E308EAFA4DE432A49177F9768.cfusion; __hstc=197801840.c3be6da8ede3051b7d5719442b0670ff.1739853146061.1739853146061.1739939456190.2; CFID=26344; CFTOKEN=b81a5b7b5973553b-1CD54019-C57B-0745-326EF14DFA3E3014; CFGLOBALS=urltoken%3DCFID%23%3D26344%26CFTOKEN%23%3Db81a5b7b5973553b%2D1CD54019%2DC57B%2D0745%2D326EF14DFA3E3014%26jsessionid%23%3D098A956E308EAFA4DE432A49177F9768%2Ecfusion%23lastvisit%3D%7Bts%20%272025%2D02%2D18%2022%3A53%3A09%27%7D%23hitcount%3D14%23timecreated%3D%7Bts%20%272025%2D02%2D17%2022%3A31%3A48%27%7D%23cftoken%3D42998c4f5aecc35d%2DE943CA12%2DE695%2D9346%2D6148AE810011F6B8%23cfid%3D21856%23; _ga=GA1.1.737746526.1739853132; __hssc=197801840.11.1739939456190; _ga_ZKFB6QJWNH=GS1.1.1739939455.2.1.1739940803.1.0.1204058674',
}
    data = {
    'residentialtype': 'residential-rent',
    'suburbSelect': 'All',
    'propertyMinSelect': '0',
    'propertyMaxSelect': '0',
    'rent_minPrice': '0',
    'rent_maxPrice': '0',
    'propertyBedroom': '0',
    'submitResidential': '',
}

    count = 1
    db = 'Andrews'
    collection = 'Andrews_Rent'
    # bucket_prefix = f'P_{collection}'
    bucket_prefix = f'Images4'
    local_file_path = '/img'

    def __init__(self, *args, **kwargs):
        super(Andrews_Rent, self).__init__(*args, **kwargs)
        self.links = []
        self.database_sale_matching_url = self.read_data_base()


    '''    SCRPAY SCRIPT START'S FROM HERE     '''
    def start_requests(self):
        payload = deepcopy(self.data)
        page_no = 1
        yield scrapy.FormRequest(self.url.format(page_no), method='POST', formdata=payload, headers=self.headers, callback=self.parse,
                                 meta={'page_no':page_no})


    def parse(self, response):
        '''    FARHAN'S LOGIC    '''
        all_links = response.xpath("//*[@class='card']/a/@href").getall()
        links = []
        for each_url in all_links:
            links.append(each_url)
            self.links.append(each_url)

        for each_db_detail_page_url in self.database_sale_matching_url:
            if each_db_detail_page_url in links:
                new_data = {
                    "Field102": "ACTIVE",
                    "Field104": each_db_detail_page_url,
                    "Field3": datetime.now().strftime("%d %b, %Y")
                }
                self.update_database(each_db_detail_page_url, new_data, 'true')
                index_to_remove = links.index(each_db_detail_page_url)
                links.pop(index_to_remove)

            elif each_db_detail_page_url not in self.links:
                new_data = {
                    "Field102": "REMOVED",
                    "Field104": each_db_detail_page_url,
                    "Field3": datetime.now().strftime("%d %b, %Y")
                }
                self.update_database(each_db_detail_page_url, new_data, 'false')


        for each_url in all_links:
            print(self.count,' # Property URL :', each_url)
            self.count += 1
            yield response.follow(each_url, headers=self.headers, callback=self.detail_parse)

        next_page = response.xpath("//*[contains(text(),'Next')]/@href").get('').strip()
        print("Next Page is :", next_page)
        if next_page:
            payload = deepcopy(self.data)
            page_no = response.meta['page_no'] + 1
            yield scrapy.FormRequest(self.url.format(page_no), method='POST', formdata=payload, headers=self.headers, callback=self.parse,
                                     meta={'page_no': page_no})

    def detail_parse(self, response):
        detail_page_url = response.url
        database_sale_matching_url = self.read_data_base()
        if detail_page_url not in database_sale_matching_url:
            item = dict()
            item['Field1'] = ''
            item['Field2'] = int(2392)
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'Andrews Property'

            property_street = response.css('.col h1 ::text').get('').strip()  # property_street
            property_suburb = ' '.join(e.strip() for e in response.css('.xxxxxxxxxxxx ::text').getall() if e.strip())  # property_suburb
            property_address = f"{property_street} {property_suburb}"
            item['Field5'] = property_address  # full_adddress

            # item['Field6'] = response.xpath("//img[@alt='Bedroom']/following-sibling::text()[1]").get('').strip()     ## for bulship tags
            item['Field6'] = response.xpath("//*[contains(text(),'Bedrooms')]/following-sibling::*[1]/text()").get('').strip()
            item['Field7'] = response.xpath("//*[contains(text(),'Bathrooms')]/following-sibling::*[1]/text()").get('').strip()
            item['Field8'] = response.xpath("//*[contains(text(),'Garages')]/following-sibling::*[1]/text()").get('').strip()

            price = response.css('p.text-uppercase ::text').get('').strip().split('|')[0]
            if '$' in price:
                price = price.replace('Sold','').replace('for','').replace('$','').replace(' ','').replace(',','')
                if re.search(r'\d', price):
                    item['Field9'] = price.strip().replace('SOLD','').replace('$','').replace('Contact','').replace('Agent','')

            description_head = ' '.join(element.strip().replace('\t', ' ').replace('\r', ' ').replace('\n', ' ') for element in response.css('.col-md-8 ::text').getall())  # description
            detailed_desc = ' '.join(element.strip().replace('\t', ' ').replace('\r', ' ').replace('\n', ' ') for element in response.css(".xxxxxxxxxxxx ::text").getall())  # description
            if description_head or detailed_desc:
                item['Field12'] = f"{description_head} {detailed_desc}"  # description

            '''         Uploading Images on Wasabi S3 Bucket            '''
            prop_images = response.xpath("//*[contains(@class,'img-fluid')]/@src").getall()
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
            '''          AGENTS          -      NO Agent Images on the website          '''
            if response.css(".ROW .col-md-6"):
                agent1 = response.css(".ROW .col-md-6")[0]
                agent1_name = agent1.css('strong ::text').get('').strip()
                if agent1_name:
                    first_name1, last_name1 = agent1_name.split(maxsplit=1)
                    item['Field15'] = first_name1.strip()  # agent_first_name_1
                    item['Field16'] = last_name1.strip()  # agent_surname_name_1
                    item['Field17'] = agent1_name.strip()  # agent_full_name_1
                    item['Field20'] = agent1.xpath(".//a[contains(@href,'tel:')]/@href").get('').strip().replace('tel:','')
                    '''         Uploading Images on Wasabi S3 Bucket            '''

            if len(response.css(".ROW .col-md-6").getall()) > 1:
                agent2 = response.css(".ROW .col-md-6")[1]
                agent2_name = agent2.css('strong ::text').get('').strip()
                if agent2:
                    item['Field23'] = agent2_name.strip()
                    item['Field26'] = agent2.xpath(".//a[contains(@href,'tel:')]/@href").get('').strip().replace('tel:','')
                    '''         Uploading Images on Wasabi S3 Bucket            '''

            if len(response.css(".ROW .col-md-6").getall()) > 2:
                agent3 = response.css(".ROW .col-md-6")[2]
                agent3_name = agent3.css('strong ::text').get('').strip()
                if agent3:
                    item['Field28'] = agent3_name.strip()
                    item['Field31'] = agent3.xpath(".//a[contains(@href,'tel:')]/@href").get('').strip().replace('tel:','')
                    '''         Uploading Images on Wasabi S3 Bucket            '''


            # item['Field33'] = re.search(r'/(\d+)/', response.url).group(1)
            # item['Field33'] = response.css('.ididididididid ::text').get('').strip()
            # id_from_url = response.url.rstrip('/').split('/')[-1]
            bbc = response.css(".propertyPage-property-icons ::text").getall()
            item['Field35'] = response.xpath("//*[contains(text(),'Property Type')]/following-sibling::*[1]/text()").get('').strip()

            land_area = response.xpath("//*[contains(text(),'Land Area')]/following-sibling::*[1]/text()").get('').strip()
            if land_area:
                if 'Acres' in land_area:
                    pass
                else:
                    item['Field36'] = land_area.replace('Land','').replace('is','').replace('squareMeter','').replace('sqm','').replace('Sqm','').replace(' ','')   # land_area
            floor_area = response.xpath("//*[contains(text(),'Building Area')]/following-sibling::*[1]/text()").get('').strip()
            if floor_area:
                if 'Acres' in floor_area:
                    pass
                else:
                    item['Field37'] = floor_area.replace('squareMeter','').replace(' ','')   # land_area

            feature_count = 58
            for feature in response.xpath("//*[text()='PROPERTY FEATURES']/following-sibling::*[@class='col-md-6']")[2:-4]:   ##  first 4 features are ID, bed, bath etc.
                if 'condition' in feature.css('::text').get('').strip().lower():
                    item['Field52'] = feature.css('::text').get('').strip()
                else:
                    item[f'Field{feature_count}'] = feature.css('::text').get('').strip()
                    feature_count += 1

            ####        ACTIVE / REMOVED Logic implemented in "Field102"
            item['Field102'] = 'ACTIVE'
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
                "Field3": new_data["Field3"],
                "Field102": new_data["Field102"],
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


