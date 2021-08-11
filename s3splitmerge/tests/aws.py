# -*- coding: utf-8 -*-

import boto3

aws_profile = None
bucket = "aws-data-lab-sanhe-aws-etl-solutions"

boto_ses = boto3.session.Session(profile_name=aws_profile, region_name="us-east-1")
s3_client = boto_ses.client("s3")


