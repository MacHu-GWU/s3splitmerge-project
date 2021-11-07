# -*- coding: utf-8 -*-

"""
Verify if Dynamodb allow duplicate compound key.

For example::

    class Measurement(Model):
        device_id = UnicodeAttribute(hash_key=True)
        time = UnicodeAttribute(range_key=True)
        value = NumberAttribute()

    # if this two item? or overwrite the same one.
    Measurement(device_id="d1", time="2000-01-01", value=1).save()
    Measurement(device_id="d1", time="2000-01-01", value=2).save()

Conclusion: NO
"""

from pynamodb.models import Model, PAY_PER_REQUEST_BILLING_MODE
from pynamodb.attributes import UnicodeAttribute, NumberAttribute
from pynamodb.connection import Connection

conn = Connection()


class Measurement(Model):
    class Meta:
        table_name = "measurement"
        region = "us-east-1"
        billing_mode = PAY_PER_REQUEST_BILLING_MODE

    device_id = UnicodeAttribute(hash_key=True)
    time = UnicodeAttribute(range_key=True)
    value = NumberAttribute()


# Measurement.create_table(wait=True)

# Measurement(device_id="d1", time="2000-01-01", value=1).save()

Measurement.delete_table()
