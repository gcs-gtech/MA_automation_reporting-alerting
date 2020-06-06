
"""This illustrates how to retrieve disapproved ads for a given customer."""


import argparse
import sys
import google.ads.google_ads.client

import pandas as pd
from df2gspread import df2gspread as d2g


_DEFAULT_PAGE_SIZE = 1000


def main(client, customer_id,  page_size):
    ga_service = client.get_service('GoogleAdsService', version='v3')

    query = ('SELECT campaign.id, ad_group.id, ad_group.name, ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.ad.type, '
             'ad_group_ad.policy_summary FROM ad_group_ad ')
            # 'WHERE campaign.id = %s' % campaign_id)

    spreadsheet = '1BVnUx3DNhWKOPoOEUo3E85e-wQXndTmyGYPmWlU2cQ8'
    wks_name = 'Sheet1'


    results = ga_service.search(customer_id, query=query, page_size=page_size)

    try:
        disapproved_ads_count = 0
        ad_type_enum = client.get_type('AdTypeEnum', version='v3').AdType
        policy_topic_entry_type_enum = client.get_type(
            'PolicyTopicEntryTypeEnum').PolicyTopicEntryType

        d = []
        # Iterate over all ads in all rows returned and count disapproved ads.
        for row in results:
            campaign_id = row.campaign.id.value
            ad_group_id = row.ad_group.id.value
            ad_group_name = row.ad_group.name.value
            ad_group_ad = row.ad_group_ad
            ad = ad_group_ad.ad
            policy_summary = ad_group_ad.policy_summary

            if not policy_summary.approval_status == client.get_type(
                  'PolicyApprovalStatusEnum').DISAPPROVED:
                continue

            disapproved_ads_count += 1


            d_series = pd.Series([campaign_id, ad_group_id, ad_group_name, ad.id.value, ad_type_enum.Name(ad.type), policy_summary.policy_topic_entries[0].topic.value, policy_topic_entry_type_enum.Name(policy_summary.policy_topic_entries[0].type)], 
                index=['campaign_id', 'ad_group_id', 'ad_group_name', 'ad_id', 'ad_type', 'policy_topic', 'policy_topic_type'])
            d.append(d_series)

        df = pd.DataFrame(d)
        print(df)

        # export dataframe to google sheet
        d2g.upload(df, spreadsheet, wks_name)

        print('\nNumber of disapproved ads found: %d' % disapproved_ads_count)
    except google.ads.google_ads.errors.GoogleAdsException as ex:
        print('Request with ID "%s" failed with status "%s" and includes the '
              'following errors:' % (ex.request_id, ex.error.code().name))
        for error in ex.failure.errors:
            print('\tError with message "%s".' % error.message)
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print('\t\tOn field: %s' % field_path_element.field_name)
        sys.exit(1)


if __name__ == '__main__':
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    google_ads_client = (google.ads.google_ads.client.GoogleAdsClient
                         .load_from_storage())

    parser = argparse.ArgumentParser(
        description=('Lists disapproved ads for a given customer'))
    # The following argument(s) should be provided to run the example.
    parser.add_argument('-c', '--customer_id', type=str,
                        required=True, help='The Google Ads customer ID.')
 
    args = parser.parse_args()

    main(google_ads_client, args.customer_id, 
         _DEFAULT_PAGE_SIZE)