##################3             AscendRE_Sold

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

class AscendRE_Sold(scrapy.Spider):
    name = "AscendRE_Sold"

    url = 'https://gw.luxurypresence.com/graphql'
    prefix = "https://ascendre.com"
    headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
    'content-type': 'application/json',
    'origin': 'https://ascendre.com',
    'priority': 'u=1, i',
    'referer': 'https://ascendre.com/',
    'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'sec-fetch-storage-access': 'active',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
}
    data = {
        'query': '\n  query Properties(\n    $agentIds: [ID!]\n    $teamIds: [ID!]\n    $neighborhoodIds: [ID!]\n    $officeIds: [ID!]\n    $propertyId: ID\n    $companyId: String\n    $networkId: String\n    $network: Boolean\n    $statusId: String\n    $propertyIds: [ID!]\n    $prioritizeIds: Boolean\n    $statusIds: [String!]\n    $excludeStatusId: [String!]\n    $neighborhoodId: String\n    $addressState: [String!]\n    $addressCity: [String!]\n    $relatedNeighborhoodPropertyId: String\n    $developmentId: String\n    $featuredListing: Boolean\n    $leaseProperty: Boolean\n    $search: String\n    $searchTermMode: SearchTermModeEnum\n    $globalProperty: Boolean\n    $archived: Boolean\n    $salesPriceGTE: Float\n    $salesPriceLTE: Float\n    $leasePriceGTE: Float\n    $leasePriceLTE: Float\n    $livingSpaceSizeGTE: Float\n    $livingSpaceSizeLTE: Float\n    $bathCountGTE: Float\n    $bathCountLTE: Float\n    $bedroomCountGTE: Float\n    $bedroomCountLTE: Float\n    $architectureStyle: String\n    $lifestyle: String\n    $propertyTypeId: String\n    $propertyTypeIds: [String!]\n    $tag: String\n    $backfillMLSResults: Boolean\n    $displayMLSListings: String\n    $hostname: String\n    $websiteId: ID\n    $backfillProviders: [String!]\n    $backfillMLSListingIds: [String!]\n    $backfillMLSAgentIds: [String!]\n    $backfillMLSOfficeIds: [String!]\n    $backfillBoundary: JSON\n    $openHouse: Boolean\n    $withGeo: Boolean\n    $advancedFilters: JSON\n    $seasonalPriceId: ID\n    $offset: Int\n    $limit: Int\n    $sort: String\n    $sortDir: SortDirectionEnum\n  ) {\n    properties(\n      \n      agentIds: $agentIds\n      teamIds: $teamIds\n      propertyId: $propertyId\n      propertyIds: $propertyIds\n      prioritizeIds: $prioritizeIds\n      companyId: $companyId\n      network: $network\n      networkId: $networkId\n      statusId: $statusId\n      statusIds: $statusIds\n      excludeStatusId: $excludeStatusId\n      neighborhoodId: $neighborhoodId\n      neighborhoodIds: $neighborhoodIds\n      officeIds: $officeIds\n      addressState: $addressState\n      addressCity: $addressCity\n      developmentId: $developmentId\n      featuredListing: $featuredListing\n      leaseProperty: $leaseProperty\n      search: $search\n      searchTermMode: $searchTermMode\n      salesPriceGTE: $salesPriceGTE\n      salesPriceLTE: $salesPriceLTE\n      leasePriceGTE: $leasePriceGTE\n      leasePriceLTE: $leasePriceLTE\n      livingSpaceSizeGTE: $livingSpaceSizeGTE\n      livingSpaceSizeLTE: $livingSpaceSizeLTE\n      bathCountGTE: $bathCountGTE\n      bathCountLTE: $bathCountLTE\n      bedroomCountGTE: $bedroomCountGTE\n      bedroomCountLTE: $bedroomCountLTE\n      architectureStyle: $architectureStyle\n      lifestyle: $lifestyle\n      propertyTypeId: $propertyTypeId\n      propertyTypeIds: $propertyTypeIds\n      tag: $tag\n      archived: $archived\n      globalProperty: $globalProperty\n      withGeo: $withGeo\n      openHouse: $openHouse\n      displayMLSListings: $displayMLSListings\n      advancedFilters: $advancedFilters\n      seasonalPriceId: $seasonalPriceId\n\n      relatedNeighborhoodPropertyId: $relatedNeighborhoodPropertyId\n      backfillMLSResults: $backfillMLSResults\n      hostname: $hostname\n      websiteId: $websiteId\n      backfillProviders: $backfillProviders\n      backfillMLSListingIds: $backfillMLSListingIds\n      backfillMLSAgentIds: $backfillMLSAgentIds\n      backfillMLSOfficeIds: $backfillMLSOfficeIds\n      backfillBoundary: $backfillBoundary\n      offset: $offset\n      limit: $limit\n      sort: $sort,\n      sortDir: $sortDir\n    ){\n      id\n      name\n      status\n      salesPrice\n      reducedPrice\n      isPasswordProtected\n      bedroomCount\n      bathCount\n      fullBathCount\n      halfBathCount\n      threeQuarterBathCount\n      fullAddress\n      addressLine1\n      addressLine2\n      addressCity\n      addressState\n      addressCountry\n      postalCode\n      description\n      syncedAt\n      officeName\n      attributionContact\n      neighborhood {\n        id\n      }\n      media {\n        smallUrl\n        mediumUrl\n        largeUrl\n        xLargeUrl\n        xxLargeUrl\n        height\n        width\n      }\n      seoTitle\n      seoDescription\n      slug\n      fromMLS\n      mlsId\n      mlsLogo\n      mlsAttribution\n      openHouse\n      openHouseHours\n      priceUponRequest\n      privateAddress\n      leaseProperty\n      leasePrice\n      currency\n      leaseTermFrequencyInterval\n      leaseTermFrequencyCount\n      leasePeriod\n      livingSpaceSize\n      livingSpaceUnits\n      lotAreaSize\n      lotAreaUnits\n      tags\n      latitude\n      longitude\n      timeZone\n      buyerAgencyCompensation\n      buyerAgencyCompensationType\n    }\n    propertiesCount(\n    \n      agentIds: $agentIds\n      teamIds: $teamIds\n      propertyId: $propertyId\n      propertyIds: $propertyIds\n      prioritizeIds: $prioritizeIds\n      companyId: $companyId\n      network: $network\n      networkId: $networkId\n      statusId: $statusId\n      statusIds: $statusIds\n      excludeStatusId: $excludeStatusId\n      neighborhoodId: $neighborhoodId\n      neighborhoodIds: $neighborhoodIds\n      officeIds: $officeIds\n      addressState: $addressState\n      addressCity: $addressCity\n      developmentId: $developmentId\n      featuredListing: $featuredListing\n      leaseProperty: $leaseProperty\n      search: $search\n      searchTermMode: $searchTermMode\n      salesPriceGTE: $salesPriceGTE\n      salesPriceLTE: $salesPriceLTE\n      leasePriceGTE: $leasePriceGTE\n      leasePriceLTE: $leasePriceLTE\n      livingSpaceSizeGTE: $livingSpaceSizeGTE\n      livingSpaceSizeLTE: $livingSpaceSizeLTE\n      bathCountGTE: $bathCountGTE\n      bathCountLTE: $bathCountLTE\n      bedroomCountGTE: $bedroomCountGTE\n      bedroomCountLTE: $bedroomCountLTE\n      architectureStyle: $architectureStyle\n      lifestyle: $lifestyle\n      propertyTypeId: $propertyTypeId\n      propertyTypeIds: $propertyTypeIds\n      tag: $tag\n      archived: $archived\n      globalProperty: $globalProperty\n      withGeo: $withGeo\n      openHouse: $openHouse\n      displayMLSListings: $displayMLSListings\n      advancedFilters: $advancedFilters\n      seasonalPriceId: $seasonalPriceId\n\n    ) {\n      count\n    }\n  }\n',
        'variables': {
            'limit': 6,
            'offset': 6,
            'sortDir': 'DESC',
            'companyId': '6a4cca51-86be-425b-8e5b-5f12136e2485',
            'sort': 'soldDate',
            'statusIds': [
                'b0e88b38-7437-438d-a6fd-b13f135e1683',
            ],
            'websiteId': '3d9bcb7d-6d59-4615-9fe6-ff60e14be165',
        },
    }
    count = 1
    db = 'AscendRE'
    collection = 'AscendRE_Sold'
    # bucket_prefix = f'P_{collection}'
    bucket_prefix = f'Images4'

    local_file_path = '/img'

    def __init__(self, *args, **kwargs):
        super(AscendRE_Sold, self).__init__(*args, **kwargs)
        self.links = []
        self.database_sale_matching_url = self.read_data_base()


    '''    SCRPAY SCRIPT START'S FROM HERE     '''
    def start_requests(self):
        offset = 0
        payload = deepcopy(self.data)
        payload['variables']['offset'] = offset
        yield scrapy.Request(self.url, method='POST', body=json.dumps(payload), headers=self.headers, callback=self.parse, meta={'offset':offset})

    def parse(self, response):
        '''    FARHAN'S LOGIC    '''
        data = json.loads(response.text)
        all_links = data.get('data',{}).get('properties',[])
        links = []
        for property in all_links:
            each_url_slug = property.get('slug','')
            each_url = f'https://ascendre.com/properties/{each_url_slug}' if each_url_slug else None
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
        for property in all_links:
            offset += 1
            each_url_slug = property.get('slug','')
            each_url = f'https://ascendre.com/properties/{each_url_slug}' if each_url_slug else None
            print(self.count,' # Property URL :', each_url)
            self.count += 1
            yield response.follow(each_url, headers=self.headers, callback=self.detail_parse, meta={'property':property})

        total_offset = data.get('data',{}).get('propertiesCount',{}).get('count')
        print(f"Current-offset : ", offset, ' || Total-offset :', total_offset)
        if offset < total_offset:
            payload = deepcopy(self.data)
            payload['variables']['offset'] = offset
            yield scrapy.Request(self.url, method='POST', body=json.dumps(payload), headers=self.headers, callback=self.parse,
                                 meta={'offset': offset})

    def detail_parse(self, response):
        property = response.meta['property']
        detail_page_url = response.url
        database_sale_matching_url = self.read_data_base()
        if detail_page_url not in database_sale_matching_url:
            item = dict()
            item['Field1'] = ''
            item['Field2'] = int(2510)
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'Ascend Real Estate'

            item['Field5'] = property.get('fullAddress','')
            item['Field6'] = property.get('bedroomCount','')
            item['Field7'] = property.get('bathCount','')
            # item['Field8'] = property.get('bathCount','')
            item['Field9'] = property.get('salesPrice','')
            # item['Field11'] = 'sold_date'
            item['Field12'] = property.get('description','').replace('<p>','').replace('</p>','')

            '''         Uploading Images on Wasabi S3 Bucket            '''
            prop_images = response.xpath("//*[contains(@class,'item__image-wrap')]//img/@src").getall()
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
            if response.css(".agent-item"):
                agent1 = response.css(".agent-item")[0]
                agent1_name = agent1.css('.name ::text').get('').strip()
                if agent1_name:
                    first_name1, last_name1 = agent1_name.split(maxsplit=1)
                    item['Field15'] = first_name1.strip()  # agent_first_name_1
                    item['Field16'] = last_name1.strip()  # agent_surname_name_1
                    item['Field17'] = agent1_name.strip()  # agent_full_name_1
                    item['Field19'] = agent1.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:','')
                    item['Field20'] = agent1.xpath(".//a[contains(@href,'tel:')]/@href").get('').strip().replace('tel:','')
                    phone_nos = agent1.xpath(".//a[contains(@href,'tel:')]/@href").getall()
                    if len(phone_nos) > 1:
                        item['Field21'] = phone_nos[-1].strip().replace('tel:','')
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    agent_image = agent1.css("img ::attr(src)").get('').strip().replace('background-image:url(','').replace(')','')
                    images_string = f'{agent_image}' if agent_image else ""
                    # item['Field22'] = images_string
                    # print('Agent1 image is :',images_string)
                    random_number = random.randint(1, 10000000000)
                    item['Field22'] = ",".join(self.uploaded(images_string, random_number))

            if len(response.css(".agent-item").getall()) > 1:
                agent2 = response.css(".agent-item")[1]
                agent2_name = agent2.css('.name ::text').get('').strip()
                if agent2:
                    item['Field23'] = agent2_name.strip()
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

            if len(response.css(".agent-item").getall()) > 2:
                agent3 = response.css(".agent-item")[2]
                agent3_name = agent3.css('.name ::text').get('').strip()
                if agent3:
                    item['Field28'] = agent3_name.strip()
                    item['Field29'] = agent3.css('.agentTile ::text').get('').strip()
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


            item['Field33'] = property.get('id','')
            item['Field35'] = response.xpath("//*[contains(text(),'Type')]/following-sibling::*[1]/text()").get('').strip()

            # item['Field40'] = 'video'
            # item['Field41'] = 'floorplan'
            # item['Field42'] = 'virtualwalkthrough'
            # item['Field80'] = 'Contract'
            # item['Field81'] = 'Pest and Building inspection'

            item['Field36'] = property.get('livingSpaceSize','')
            item['Field37'] = property.get('lotAreaSize','')
            xxxxxxxxxxxxxxxx = property.get('xxxxxxxxxxxxxxxx','')

            feature_count = 58
            # for feature in response.css(".epl-property-features li"):   ##  first 4 features are ID, bed, bath etc.
            for feature in response.xpath("//*[contains(text(),'Exterior') or contains(text(),'Interior')]/following-sibling::div[1]/div"):   ##  first 4 features are ID, bed, bath etc.
                feature_text = ' '.join(e.strip() for e in feature.css('::text').getall() if e.strip())
                if 'condition' in feature_text.lower():
                    item['Field52'] = feature_text.strip()
                else:
                    item[f'Field{feature_count}'] = feature_text.strip()
                    feature_count += 1

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
