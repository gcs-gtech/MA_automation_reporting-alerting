
# This example illustrates how to get all campaigns.


import argparse
import sys

import pandas as pd 
from df2gspread import df2gspread as d2g

from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException


def main(client, customer_id):
    ga_service = client.get_service('GoogleAdsService', version='v3')

    query = ('SELECT customer.id, customer.currency_code, campaign.id, campaign.status, campaign.name, campaign.advertising_channel_type, campaign_budget.amount_micros, metrics.cost_micros '
             'FROM campaign '
             'WHERE segments.date DURING LAST_30_DAYS AND campaign.status = \'ENABLED\'' )

    # Issues a search request using streaming.
    response = ga_service.search_stream(customer_id, query=query)

    # Specify the spreadsheet ID and worksheet name where dataframe export to.
    spreadsheet = '15nxxxxxxxxxxxxxxxxxxxxxxxAGM'
    wks_name = 'Sheet1'

    camp_type_enum = client.get_type('AdvertisingChannelTypeEnum')

    try:

        for batch in response:
            # print(batch.results)
          
            d = []
            for row in batch.results:
                cus_id = row.customer.id.value
                cus_currency = row.customer.currency_code.value
                camp_id = row.campaign.id.value
                camp_name = row.campaign.name.value
                camp_type = camp_type_enum.AdvertisingChannelType.Name(row.campaign.advertising_channel_type)
                camp_budget = round(row.campaign_budget.amount_micros.value/1000000, 2)
                cost = round(row.metrics.cost_micros.value/1000000, 2)
 
                d_series = pd.Series([cus_id, cus_currency, camp_id, camp_name, camp_type, camp_budget, cost], index=['customer_id', 'customer_currency', 'campaign_id', 'campaign_name', 'campaign_type', 'campaign_budget', 'cost last 7 DAYS'])
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
        description='List budget and last 7 days spending for all enabled campaigns of specified customer.')
    # The following argument(s) should be provided to run the example.
    parser.add_argument('-c', '--customer_id', type=str,
                        required=True, help='The Google Ads customer ID.')
    args = parser.parse_args()

    main(google_ads_client, args.customer_id)
