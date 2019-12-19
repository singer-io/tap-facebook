import os
import json

import pendulum
import singer
import dateutil
from datetime import timezone

from singer import utils

from tap_facebook.stream import IncrementalStream

LOGGER = singer.get_logger()




def transform_datetime_string(dts):
    parsed_dt = dateutil.parser.parse(dts)
    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
    else:
        parsed_dt = parsed_dt.astimezone(timezone.utc)
    return singer.strftime(parsed_dt)

def iter_delivery_info_filter(stream_type):
    filt = {
        "field": stream_type + ".delivery_info",
        "operator": "IN",
    }

    filt_values = [
        "active", "archived", "completed",
        "limited", "not_delivering", "deleted",
        "not_published", "pending_review", "permanently_deleted",
        "recently_completed", "recently_rejected", "rejected",
        "scheduled", "inactive"]

    sub_list_length = 3
    for i in range(0, len(filt_values), sub_list_length):
        filt['value'] = filt_values[i:i+sub_list_length]
        yield filt


def transform_date_hook(data, typ, schema):
    if typ == 'string' and schema.get('format') == 'date-time' and isinstance(data, str):
        transformed = transform_datetime_string(data)
        return transformed
    return data

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(stream):
    path = get_abs_path('schemas/{}.json'.format(stream.name))
    field_class = stream.field_class
    schema = utils.load_json(path)

    for k in schema['properties']:
        if k not in field_class.__dict__:
            LOGGER.warning(
                'Property %s.%s is not defined in the facebook_business library',
                stream.name, k)

    return schema

def load_shared_schema_refs():
    shared_schemas_path = get_abs_path('schemas/shared')

    shared_file_names = [f for f in os.listdir(shared_schemas_path)
                         if os.path.isfile(os.path.join(shared_schemas_path, f))]

    shared_schema_refs = {}
    for shared_file in shared_file_names:
        with open(os.path.join(shared_schemas_path, shared_file)) as data_file:
            shared_schema_refs[shared_file] = json.load(data_file)

    return shared_schema_refs