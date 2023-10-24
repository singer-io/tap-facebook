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
        self.base_url  = 'https://graph.facebook.com/'
        self.api_version = 'v18.0'
        self.account_id = os.getenv('TAP_FACEBOOK_ACCOUNT_ID')
        self.access_token  = os.getenv('TAP_FACEBOOK_ACCESS_TOKEN')
        self.account_url = self.base_url + self.api_version +'/act_{}'.format(self.account_id)

        self.stream_endpoint_map = {'ads': '/ads',
                                    'adsets': '/adsets',
                                    'adcreative': '/adcreatives',
                                    'ads_insights': '/insights',
                                    'campaigns': '/campaigns',
                                    'users': '/users',}

        self.campaign_special_ad_categories = ['NONE',
                                               'EMPLOYMENT',
                                               'HOUSING',
                                               'CREDIT',
                                               # 'ISSUES_ELECTIONS_POLITICS',  # acct unauthorized
                                               'ONLINE_GAMBLING_AND_GAMING']

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
            f'Endpoint undefiend for specified stream: {stream}'
        endpoint = self.stream_endpoint_map[stream]
        url = self.account_url + endpoint
        params = {'access_token': self.access_token,
                  'limit': 100}
        LOGGER.info(f"Getting url: {url}")
        response = requests.get(url, params)
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
        LOGGER.info(f"Returning post response: {response}")
        return response

    def generate_post_params(self, stream):
        if stream == 'campaigns':
            params = {  # generate a campaign with random name, ojbective, and ad category
                'access_token': self.access_token,
                'name': ''.join(random.choices(string.ascii_letters + string.digits, k=15)),
                'objective': random.choice(self.campaign_objectives),
                'special_ad_categories': random.choice(self.campaign_special_ad_categories)}
            return params
        else:
            assert False, f"Post params for stream {stream} not implemented / supported"


    # Create multiplue ads at a time async $ get notif when complete
    # Make an HTTP POST to:
    # https://graph.facebook.com/{API_VERSION}/act_{AD_ACCOUNT_ID}/asyncadrequestsets

    # HTTP to get ads for an account
    # GET /v18.0/act_{ad-account-id}/ads HTTP/1.1
    # Host: graph.facebook.com

    # cURL to read all ads from one ad account example
    # curl -G \
    #     -d "fields=name" \
    #     -d "access_token=<ACCESS_TOKEN>" \
    #     "https://graph.facebook.com/<API_VERSION>/act_<AD_ACCOUNT_ID>/ads"

    # Ad IDs TODO
    # "data": [{"id": "23843561338620058"},
    #          {"id": "23847656838300058"},
    #          {"id": "23847292383430058"}],
    # creative = {'asset_feed_spec': {'audios': [{'type': 'random'}]},
    #             'contextual_multi_ads': {'eligibility': ['POST_AD_ENGAGEMENT_FEED',
    #                                                      'POST_AD_ENGAGEMENT_SEED_AD',
    #                                                      'STANDALONE_FEED'],
    #                                      'enroll_status': 'OPT_IN'},
    #             'degrees_of_freedom_spec': {'degrees_of_freedom_type': 'USER_ENROLLED_NON_DCO',
    #                                         'text_transformation_types': ['TEXT_LIQUIDITY']},
    #             'object_story_spec': {'instagram_actor_id': '2476947555701417',
    #                                   'link_data': {'call_to_action': {'type': 'SIGN_UP'},
    #                                                 'link': 'http://fb.me',
    #                                                 'picture': 'https://foo.x.y.net/v/dir/1.png'},
    #                                   'page_id': '453760455405317'},
    #             'object_type': 'SHARE'}

    # Ad Insights TODO
    # Empty data list for all 3 AdSet Ids

    # AdSet Ids TODO
    # "data": [{"id": "23847656838230058"},
    #          {"id": "23847292383400058"},
    #          {"id": "23843561338600058"}],

    # Campaign Ids TODO
    # "data": [{"id": "23847656838160058"},
    #          {"id": "23847292383380058"},
    #          {"id": "23843561338580058"},
    #          {"id": "120203241386960059"}]  # API added campaign, returned on API get, verified
    # cam_post_params = {'access_token': token,
    #                    'name': 'BHT test campaign',
    #                    'objective': 'OUTCOME_TRAFFIC',
    #                    'special_ad_categories': ['NONE']}

    # Creative Ids TODO
    # "data": [{"id": "23850233534140058"},
    #          {"id": "23850233532300058"},
    #          {"id": "23850233210620058"},
    #          {"id": "23850232986710058"},
    #          {"id": "23849554079380058"},
    #          {"id": "23849066774220058"},
    #          {"id": "23849066252410058"},
    #          {"id": "23849063425570058"},
    #          {"id": "23847674484290058"},
    #          {"id": "23847292410940058"},
    #          {"id": "23843561378450058"}],

    # Users
    # "data": [{"name": "Stitch IntegrationDev",
    #           "tasks": ["DRAFT", "ANALYZE", "ADVERTISE", "MANAGE"],
    #           "id": "113504146635004"}]

    # TODO refactor below this line from jira test client to facebook
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
