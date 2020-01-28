from os import path

from singer import utils


def get_abs_path(filepath):
    return path.join(path.dirname(path.realpath(__file__)), filepath)


def load_schema(stream_name):
    path = get_abs_path("schemas/{}.json".format(stream_name))
    return utils.load_json(path)
