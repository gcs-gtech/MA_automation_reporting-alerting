# This example illustrates how to get all feed item approval status for specific customer.


import argparse
import sys

import pandas as pd 
from df2gspread import df2gspread as d2g

from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException

from google.protobuf.json_format import MessageToDict

def main(client, customer_id):
    ga_service = client.get_service('GoogleAdsService', version='v3')

    query = ('SELECT  feed_item.id, customer.id, campaign.id, feed.id, feed.name,  feed_item.attribute_values, feed_item.policy_infos '
             'FROM feed_item '
             'WHERE segments.date DURING LAST_30_DAYS ')

    # Issues a search request using streaming.
    response = ga_service.search_stream(customer_id, query=query)

    # Specify the spreadsheet ID and worksheet name where dataframe export to.
    spreadsheet = '1gLdgaqJ6W-yxAH9WYjeMD8JxspkWYpgf1VC7Zi9u0Fk'
    wks_name = 'Sheet1'

    approval_status_enum = client.get_type('PolicyApprovalStatusEnum').PolicyApprovalStatus

    try:

        for batch in response:
            #print(batch.results)
          
            d = []
            for row in batch.results:
                cus_id = row.customer.id.value
                campaign_id = row.campaign.id.value 
                feed_id = row.feed.id.value
                feed_name = row.feed.name.value
                feed_item_id = row.feed_item.id.value

                row_dict = MessageToDict(row, preserving_proto_field_name = True)
                #print(row_dict)
                attributes_list = row_dict["feed_item"]["attribute_values"]
                for i in attributes_list:
                    if i["feed_attribute_id"] == '1':
                        feed_item_attributes_1 = i['string_value']
                     
                    if i["feed_attribute_id"] == '3':
                        feed_item_attributes_3 = i['string_value']
          
                    if i["feed_attribute_id"] == '4':
                        feed_item_attributes_4 = i['string_value']

                    if i["feed_attribute_id"] == '5':
                        feed_item_attributes_5 = i['string_values']
                      


                feed_item_policy_infos = row.feed_item.policy_infos
                #approval_status = approval_status_enum.Name(feed_item_policy_infos[0].approval_status)
                policy_infos_list = row_dict["feed_item"]["policy_infos"]
                for j in policy_infos_list:
                    feed_item_approval_status = j["approval_status"]
                
 
                d_series = pd.Series([feed_item_id, cus_id, campaign_id, feed_id, feed_name, feed_item_attributes_1,  feed_item_attributes_3,  feed_item_attributes_4, feed_item_attributes_5, feed_item_approval_status], 
                    index=['feed_item_id', 'customer_id', 'campaign_id', 'feed_id', 'feed_name',  'feed_item_attributes_1',  'feed_item_attributes_3','feed_item_attributes_4', 'feed_item_attributes_5', 'feed_item_approval_status'])
                d.append(d_series)

            df = pd.DataFrame(d)
            print(df)

            # export dataframe to google sheet
            d2g.upload(df, spreadsheet, wks_name)


    except GoogleAdsException as ex:
        print(f'Request with ID "{ex.request_id}" failed with status '
              f'"{ex.error.code().name}" and includes the following errors:')
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f'\t\tOn field: {field_path_element.field_name}')
        sys.exit(1)


if __name__ == '__main__':
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    google_ads_client = GoogleAdsClient.load_from_storage()

    parser = argparse.ArgumentParser(
        description='List all feed status for specified customer.')
    # The following argument(s) should be provided to run the example.
    parser.add_argument('-c', '--customer_id', type=str,
                        required=True, help='The Google Ads customer ID.')
    args = parser.parse_args()

    main(google_ads_client, args.customer_id)
