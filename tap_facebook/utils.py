from os import path

from singer import utils


def get_abs_path(path):
    return path.join(path.dirname(path.realpath(__file__)), path)


def load_schema(stream):
    path = get_abs_path("schemas/{}.json".format(stream.name))
    return utils.load_json(path)
