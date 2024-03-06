import os
import random
import requests
import string

from tap_tester.logger import LOGGER


class TestClient():
    def __init__(self):
        self.base_url  = 'https://graph.facebook.com'
        self.api_version = 'v19.0'
        self.account_id = os.getenv('TAP_FACEBOOK_ACCOUNT_ID')
        self.access_token  = os.getenv('TAP_FACEBOOK_ACCESS_TOKEN')
        self.account_url = f"{self.base_url}/{self.api_version}/act_{self.account_id}"

        self.stream_endpoint_map = {'ads': '/ads',
                                    'adsets': '/adsets',
                                    'adcreative': '/adcreatives',
                                    'ads_insights': '/insights',  # GET only endpoint
                                    'campaigns': '/campaigns',
                                    'users': '/users',}

        self.campaign_special_ad_categories = ['NONE',
                                               'EMPLOYMENT',
                                               'HOUSING',
                                               'CREDIT',
                                               # 'ISSUES_ELECTIONS_POLITICS',  # acct unauthorized
                                               'ONLINE_GAMBLING_AND_GAMING']

        self.adset_billing_events = ['APP_INSTALLS',
                                     'CLICKS',
                                     'IMPRESSIONS',
                                     'LINK_CLICKS',
                                     'NONE',
                                     'OFFER_CLAIMS',
                                     'PAGE_LIKES',
                                     'POST_ENGAGEMENT',
                                     'THRUPLAY',
                                     'PURCHASE',
                                     'LISTING_INTERACTION']

        self.adset_optimization_goals = ['NONE',
                                         'APP_INSTALLS',
                                         'AD_RECALL_LIFT',
                                         'ENGAGED_USERS',
                                         'EVENT_RESPONSES',
                                         'IMPRESSIONS',
                                         'LEAD_GENERATION',
                                         'QUALITY_LEAD',
                                         'LINK_CLICKS',
                                         'OFFSITE_CONVERSIONS',
                                         'PAGE_LIKES',
                                         'POST_ENGAGEMENT',
                                         'QUALITY_CALL',
                                         'REACH',
                                         'LANDING_PAGE_VIEWS',
                                         'VISIT_INSTAGRAM_PROFILE',
                                         'VALUE',
                                         'THRUPLAY',
                                         'DERIVED_EVENTS',
                                         'APP_INSTALLS_AND_OFFSITE_CONVERSIONS',
                                         'CONVERSATIONS',
                                         'IN_APP_VALUE',
                                         'MESSAGING_PURCHASE_CONVERSION',
                                         'MESSAGING_APPOINTMENT_CONVERSION',
                                         'SUBSCRIBERS',
                                         'REMINDERS_SET']

        # valid and verified objectives listed below, other objectives should be re-mapped to these
        self.campaign_objectives = ['OUTCOME_APP_PROMOTION',
                                    'OUTCOME_AWARENESS',
                                    'OUTCOME_ENGAGEMENT',
                                    'OUTCOME_LEADS',
                                    'OUTCOME_SALES',
                                    'OUTCOME_TRAFFIC']

    def get_account_objects(self, stream, limit, time_range):
        # time_range defines query start and end dates and should match tap config
        assert stream in  self.stream_endpoint_map.keys(), \
            f'Endpoint undefined for specified stream: {stream}'
        endpoint = self.stream_endpoint_map[stream]
        url = self.account_url + endpoint
        params = {'access_token': self.access_token,
                  'limit': limit,
                  'time_range': str({'since': time_range['since'], 'until': time_range['until']})}
        LOGGER.info(f"Getting url: {url}")
        response = requests.get(url, params)
        response.raise_for_status()
        LOGGER.info(f"Returning get response: {response}")
        return response.json()

    def create_account_objects(self, stream):
        assert stream in  self.stream_endpoint_map.keys(), \
            f'Endpoint undefined for specified stream: {stream}'
        endpoint = self.stream_endpoint_map[stream]
        url = self.account_url + endpoint
        LOGGER.info(f"Posting to url: {url}")
        params = self.generate_post_params(stream)
        response = requests.post(url, params)
        response.raise_for_status()
        LOGGER.info(f"Returning post response: {response}")
        return response

    def generate_post_params(self, stream):
        if stream == 'adcreative':
            params = {
                'access_token': self.access_token,
                'name': ''.join(random.choices(string.ascii_letters + string.digits, k=18)),
                'object_story_spec': str({'page_id': '453760455405317',
                                          'link_data': {'link': 'http://fb.me'}})}
            return params

        elif stream == 'ads':
            params = {
                'access_token': self.access_token,
                'name': ''.join(random.choices(string.ascii_letters + string.digits, k=17)),
                # adset is bound to parent campaign_objective, can cause errors posting new ads
                #   as certain objectives have different requirements. 50 ads per adset max
                #   adset below can be found under campaign: 120203395323750059
                'adset_id': 120203403135680059,
                'creative': str({'creative_id': 23843561378450058}),
                'status': "PAUSED"}
            return params

        elif stream == 'adsets':
            # TODO In order to randomize optimization_goal and billing_event the campaign_id
            #   would need to be examined to determine which goals were supported.  Then an option
            #   could be selected from the available billing events supported by that goal.
            params = {
                'access_token': self.access_token,
                'name': ''.join(random.choices(string.ascii_letters + string.digits, k=16)),
                'optimization_goal': 'REACH',
                'billing_event': 'IMPRESSIONS',
                'bid_amount': 2,  # TODO random?
                'daily_budget': 1000, # TODO random? tie to parent campaign?
                'campaign_id': 120203241386960059,
                'targeting': str({'geo_locations': {'countries': ["US"]},
                                  'facebook_positions': ["feed"]}),
                'status': "PAUSED",
                'promoted_object': str({'page_id': '453760455405317'})}
            return params

        elif stream == 'campaigns':
            params = {  # generate a campaign with random name, ojbective, and ad category
                'access_token': self.access_token,
                'name': ''.join(random.choices(string.ascii_letters + string.digits, k=15)),
                'objective': random.choice(self.campaign_objectives),
                'special_ad_categories': random.choice(self.campaign_special_ad_categories)}
            return params

        else:
            assert False, f"Post params for stream {stream} not implemented / supported"


    # Ad Insights TODO
    # endpoint is "GET" only.  We cannot post fake insights data for test. As of Oct 27, 2023
    # data lists for original 3 AdSet Ids, and Ads account as a whole are empty.
    #   1 - Can we run enough ads to get enough data to paginate?
    #   2 - Can we interact with our own ads?
    # if 1 or 2 == True then use setUp to conditionally test ads_insights if there is enough data
