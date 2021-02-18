from os import path
from typing import Union
from datetime import datetime
from dateutil import parser

from singer import utils


def get_abs_path(filepath):
    return path.join(path.dirname(path.realpath(__file__)), filepath)


def load_schema(stream_name):
    path = get_abs_path("schemas/{}.json".format(stream_name))
    return utils.load_json(path)


def parse_date(dateobj: Union[str, datetime]):
    if isinstance(dateobj, datetime):
        return dateobj.date()
    elif isinstance(dateobj, str):
        return parser.isoparse(dateobj).date()

    raise ValueError(f"invalid date: {dateobj}")
