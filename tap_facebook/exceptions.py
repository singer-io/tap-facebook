from facebook_business.exceptions import FacebookRequestError

class TapFacebookException(Exception):
    pass

class InsightsJobTimeout(TapFacebookException):
    pass
