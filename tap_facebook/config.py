INSIGHTS_MAX_WAIT_TO_START_SECONDS = 2 * 60
INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS = 30 * 60
INSIGHTS_MAX_ASYNC_SLEEP_SECONDS = 5 * 60


UPDATED_TIME_KEY = 'updated_time'
START_DATE_KEY = 'start_date'

ALL_ACTION_BREAKDOWNS = [
    'action_type',
    'action_target_id',
    'action_destination'
]

ALL_ACTION_ATTRIBUTION_WINDOWS = [
    '1d_click',
    '7d_click',
    '28d_click',
    '1d_view',
    '7d_view',
    '28d_view'
]

REQUIRED_CONFIG_KEYS = ['start_date', 'account_id', 'access_token']

BOOKMARK_KEYS = {
    'ads': UPDATED_TIME_KEY,
    'adsets': UPDATED_TIME_KEY,
    'campaigns': UPDATED_TIME_KEY,
    'ads_insights': START_DATE_KEY,
    'ads_insights_age_and_gender': START_DATE_KEY,
    'ads_insights_country': START_DATE_KEY,
    'ads_insights_platform_and_device': START_DATE_KEY,
    'ads_insights_region': START_DATE_KEY,
    'ads_insights_dma': START_DATE_KEY,
}

RESULT_RETURN_LIMIT = 100

INSIGHTS_BREAKDOWNS_OPTIONS = {
    'ads_insights': {"breakdowns": []},
    'ads_insights_age_and_gender': {"breakdowns": ['age', 'gender'],
                                    "primary-keys": ['age', 'gender']},
    'ads_insights_country': {"breakdowns": ['country']},
    'ads_insights_platform_and_device': {"breakdowns": ['publisher_platform',
                                                        'platform_position', 'impression_device'],
                                         "primary-keys": ['publisher_platform',
                                                          'platform_position', 'impression_device']},
    'ads_insights_region': {'breakdowns': ['region'],
                            'primary-keys': ['region']},
    'ads_insights_dma': {"breakdowns": ['dma'],
                         "primary-keys": ['dma']},
}

STREAMS = [
    'adcreative',
    'ads',
    'adsets',
    'campaigns',
    'ads_insights',
    'ads_insights_age_and_gender',
    'ads_insights_country',
    'ads_insights_platform_and_device',
    'ads_insights_region',
    'ads_insights_dma',
]