# -*- coding: utf-8 -*-

import json
import attr
from attrs_mate import AttrsClass


@attr.s
class Event(AttrsClass):
    @classmethod
    def from_json(cls, js):
        return cls(**json.loads(js))

    def to_json(self):
        return json.dumps(self.to_dict())


class LambdaFunction:
    name = None

    def handler(self, event, context):
        raise NotImplementedError
