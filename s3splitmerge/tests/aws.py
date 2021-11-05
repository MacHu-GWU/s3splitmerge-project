# -*- coding: utf-8 -*-

"""
S3 object for testing naming convention::

    s3://{bucket}/{prefix}/{module}/{function/method_name}/{filename}
"""
import boto3

# --- manually configure following settings
aws_profile = None
aws_region = "us-east-1"
bucket = "aws-data-lab-sanhe-aws-etl-solutions"
prefix = "s3splitmerge/tests"

boto_ses = boto3.session.Session(profile_name=aws_profile, region_name=aws_region)

s3_client = boto_ses.client("s3")
