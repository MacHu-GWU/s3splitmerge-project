# -*- coding: utf-8 -*-

"""
S3 object for testing naming convention::

    s3://{bucket}/{prefix}/{module}/{function/method_name}/{filename}
"""
import boto3

# --- manually configure following settings
aws_profile = None
bucket = "aws-data-lab-sanhe-aws-etl-solutions"
prefix = "s3splitmerge/tests"

# --- create variables for testing
boto_ses = boto3.session.Session(region_name="us-east-1")
s3_client = boto_ses.client("s3")
