import argparse
import collections
import datetime
import functools
import json
import os
import time

import requests

import singer

DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
LOGGER = singer.get_logger()


class Timer(object):
    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, type, value, traceback):
        self.elapsed = int((time.time() - self.start_time) * 1000)


def request(method, url, url_params=None, **kwargs):
    if url_params:
        url.format(**url_params)

    req = requests.Request(method, url, **kwargs).prepare()
    LOGGER.info("GET {}".format(req.url))
    session = requests.Session()
    with Timer() as t:
        resp = session.send(req)

    endpoint = req.path_url
    if "?" in endpoint:
        endpoint, _ = endpoint.split("?")

    metric = {
        "endpoint": endpoint,
        "elapsed": t.elapsed,
        "status_code": resp.status_code,
        "success": resp.status_code < 400,
    }
    # TODO: emit metric
    # singer.write_metric(metric)
    return resp


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_json(path):
    with open(path) as f:
        return json.load(f)


def load_schema(entity):
    return load_json(get_abs_path("schemas/{}.json".format(entity)))


def update_state(state, entity, dt):
    if dt is None:
        return

    if isinstance(dt, datetime.datetime):
        dt = strftime(dt)

    if entity not in state:
        state[entity] = dt

    if dt >= state[entity]:
        state[entity] = dt


def parse_args(required_config_keys):
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    parser.add_argument('--schemas', help='Schemas file')
    args = parser.parse_args()

    config = load_json(args.config)
    check_config(config, required_config_keys)

    if args.state:
        state = load_json(args.state)
    else:
        state = {}

    if args.schemas:
        schemas = load_json(args.schemas)
    else:
        schemas = {}

    return config, state, schemas


def check_config(config, required_keys):
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise Exception("Config is missing required keys: {}".format(missing_keys))
