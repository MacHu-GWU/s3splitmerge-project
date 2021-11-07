# -*- coding: utf-8 -*-

import os
import boto3

if "AWS_LAMBDA_FUNCTION_NAME" in os.environ:
    boto_ses = boto3.session.Session()
else:
    boto_ses = boto3.session.Session()
