# infra/app.py (using AWS CDK in Python)

from aws_cdk import (
    aws_s3   as s3,
    aws_kms  as kms,
    aws_lambda as _lambda,
    aws_s3_notifications as s3n,
    core,
)

class ViewingPipelineStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # 1. KMS key for end-to-end encryption
        key = kms.Key(self, "DataKey", enable_key_rotation=True)

        # 2. Raw bucket (encrypted) + Processed bucket (encrypted)
        raw_bucket = s3.Bucket(self, "RawBucket",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=key,
            removal_policy=core.RemovalPolicy.DESTROY
        )
        proc_bucket = s3.Bucket(self, "ProcessedBucket",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=key,
            removal_policy=core.RemovalPolicy.DESTROY
        )

        # 3. Lambda for aggregation & anonymization
        fn = _lambda.Function(self, "AggregatorFn",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="etl.handler",
            code=_lambda.Code.from_asset("lambda/etl"),
            environment={
                "RAW_BUCKET": raw_bucket.bucket_name,
                "PROC_BUCKET": proc_bucket.bucket_name
            }
        )
        raw_bucket.grant_read(fn)
        proc_bucket.grant_write(fn)

        # 4. Trigger lambda on new S3 objects under raw/
        notif = s3n.LambdaDestination(fn)
        raw_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            notif,
            s3.NotificationKeyFilter(prefix="raw/")
        )


