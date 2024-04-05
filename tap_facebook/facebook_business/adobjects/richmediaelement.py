# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class RichMediaElement(
    AbstractObject,
):

    def __init__(self, api=None):
        super(RichMediaElement, self).__init__()
        self._isRichMediaElement = True
        self._api = api

    class Field(AbstractObject.Field):
        element = 'element'
        element_type = 'element_type'
        name = 'name'

    _field_types = {
        'element': 'Object',
        'element_type': 'string',
        'name': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


