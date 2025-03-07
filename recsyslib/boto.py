def delete_all_obj(bucket_name, key, secret):
    import boto3
    s3 = boto3.resource('s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=key,
        aws_secret_access_key=secret)
        
    bucket = s3.Bucket(bucket_name)

    # Deleting all versions (works for non-versioned buckets too).
    bucket.object_versions.delete()

    # Aborting all multipart uploads, which also deletes all parts.
    for multipart_upload in bucket.multipart_uploads.iterator():
        # Part uploads that are currently in progress may or may not succeed,
        # so it might be necessary to abort a multipart upload multiple times.
        while len(list(multipart_upload.parts.all())) > 0:
            multipart_upload.abort()
