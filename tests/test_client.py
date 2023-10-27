# import backoff
import os
import random
import requests
import string

# from requests.exceptions import HTTPError
# from requests.auth import HTTPBasicAuth

# TODO try below syncronous interaction or go all http?
# from facebookads.objects import Ad, AdAccount, AdSet, Campaign

from tap_tester.logger import LOGGER


class TestClient():
    # def __init__(self, config):  # TODO move to dynamic config model?
    def __init__(self):
        # pass in config above and get() from it or hard code?
        self.base_url  = 'https://graph.facebook.com'
        self.api_version = 'v18.0'
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

        # list of campaign objective values from fb docs below give "Invalid" error via api 18.0
        # 'APP_INSTALLS', 'BRAND_AWARENESS', 'CONVERSIONS', 'EVENT_RESPONSES', 'LEAD_GENERATION',
        # 'LINK_CLICKS', 'MESSAGES', 'OFFER_CLAIMS', 'PAGE_LIKES', 'POST_ENGAGEMENT',
        # 'PRODUCT_CATALOG_SALES', 'REACH', 'STORE_VISITS', 'VIDEO_VIEWS'

        # LOCAL_AWARENESS gives deprecated error, use REACH (reach is invalid from above)

        # valid and verified objectives listed below, objectives above should be re-mapped to these
        self.campaign_objectives = ['OUTCOME_APP_PROMOTION',
                                    'OUTCOME_AWARENESS',
                                    'OUTCOME_ENGAGEMENT',
                                    'OUTCOME_LEADS',
                                    'OUTCOME_SALES',
                                    'OUTCOME_TRAFFIC']

    def get_account_objects(self, stream):
        assert stream in  self.stream_endpoint_map.keys(), \
            f'Endpoint undefined for specified stream: {stream}'
        endpoint = self.stream_endpoint_map[stream]
        url = self.account_url + endpoint
        params = {'access_token': self.access_token,
                  'limit': 100}
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
                'creative': str({'creative_id': 23843561378450058}),  # TODO pick rand creative_id?
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
                'campaign_id': 120203241386960059,  # TODO pull from campaigns dynamically?
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


    # TODO refactor or remove below this line from jira test client to facebook
    # def url(self, path):
    #     if self.is_cloud:
    #         return self.base_url.format(self.cloud_id, path)

    #     # defend against if the base_url does or does not provide https://
    #     base_url = self.base_url
    #     base_url = re.sub('^http[s]?://', '', base_url)
    #     base_url = 'https://' + base_url
    #     return base_url.rstrip("/") + "/" + path.lstrip("/")

    # def _headers(self, headers):
    #     headers = headers.copy()
    #     if self.user_agent:
    #         headers["User-Agent"] = self.user_agent

    #     if self.is_cloud:
    #         # Add OAuth Headers
    #         headers['Accept'] = 'application/json'
    #         headers['Authorization'] = 'Bearer {}'.format(self.access_token)

    #     return headers

    # @backoff.on_exception(backoff.expo,
    #                       (requests.exceptions.ConnectionError, HTTPError),
    #                       jitter=None,
    #                       max_tries=6,
    #                       giveup=lambda e: not should_retry_httperror(e))
    # def send(self, method, path, headers={}, **kwargs):
    #     if self.is_cloud:
    #         # OAuth Path
    #         request = requests.Request(method,
    #                                    self.url(path),
    #                                    headers=self._headers(headers),
    #                                    **kwargs)
    #     else:
    #         # Basic Auth Path
    #         request = requests.Request(method,
    #                                    self.url(path),
    #                                    auth=self.auth,
    #                                    headers=self._headers(headers),
    #                                    **kwargs)
    #     return self.session.send(request.prepare())

    # @backoff.on_exception(backoff.constant,
    #                       RateLimitException,
    #                       max_tries=10,
    #                       interval=60)
    # def request(self, tap_stream_id, *args, **kwargs):
    #     response = self.send(*args, **kwargs)
    #     if response.status_code == 429:
    #         raise RateLimitException()

    #     try:
    #         response.raise_for_status()
    #     except requests.exceptions.HTTPError as http_error:
    #         LOGGER.error("Received HTTPError with status code %s, error message response text %s",
    #                      http_error.response.status_code,
    #                      http_error.response.text)
    #         raise

    #     return response.json()

    # def refresh_credentials(self):
    #     body = {"grant_type": "refresh_token",
    #             "client_id": self.oauth_client_id,
    #             "client_secret": self.oauth_client_secret,
    #             "refresh_token": self.refresh_token}
    #     try:
    #         resp = self.session.post("https://auth.atlassian.com/oauth/token", data=body)
    #         resp.raise_for_status()
    #         self.access_token = resp.json()['access_token']
    #     except Exception as ex:
    #         error_message = str(ex)
    #         if resp:
    #             error_message = error_message + ", Response from Jira: {}".format(resp.text)
    #         raise Exception(error_message) from ex
    #     finally:
    #         LOGGER.info("Starting new login timer")
    #         self.login_timer = threading.Timer(REFRESH_TOKEN_EXPIRATION_PERIOD,
    #                                            self.refresh_credentials)
    #         self.login_timer.start()

    # def test_credentials_are_authorized(self):
    #     # Assume that everyone has issues, so we try and hit that endpoint
    #     self.request("issues", "GET", "/rest/api/2/search",
    #                  params={"maxResults": 1})
