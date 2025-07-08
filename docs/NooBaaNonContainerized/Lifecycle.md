# NooBaa Non Containerized - Lifecycle

1. [Introduction](#introduction)
2. [General Information](#general-information)
3. [AWS S3 Lifecycle Policy reminder](#aws-s3-lifecycle-policy-reminder)
4. [Supported Lifecycle Policy Rules](#noobaa-nc-supported-lifecycle-policy-rules)
5. [Lifecycle policy configuration instructions](#lifecycle-policy-configuration-instructions)


## Introduction
This document provides step-by-step instructions to help a user to successfully configure lifecycle policy on a bucket, and run NooBaa NC lifecycle CLI.

## General Information
The NC Lifecycle feature contains 2 parts - 
1. Lifecycle policy configuration.
2. Lifecycle background worker (CLI) run.


## AWS S3 Lifecycle Policy reminder

An S3 lifecycle policy is a set of rules that define actions to be taken on objects in an S3 bucket over time.<br>
These policies allow you to automate the deletion of objects after a specified retention period or transition of objects between different storage classes.
Lifecycle policies work based on predefined rules set by the user. 
These rules specify the conditions that an object must meet to trigger a particular action. For instance, you can create rules to permanently delete objects that are no longer needed after a specific timeframe. 

The lifecycle configuration policy contains a set of rules that each one contains one or more elements. <br>
AWS S3 supports the following elements that describe lifecycle actions -

1. **Filter** 

    Filter objects based on -
    - Prefix
    - ObjectSizeGreaterThan
    - ObjectSizeLessThan
    - Tags

2. **Expiration** 

    Find expired objects (current version only) based on -
    - Date
    - Days - Number of days passed from creation of the object.
    - ExpiredObjectDeleteMarker - boolean configuration that cleans also the last delete marker if sets to true.

    ##### Note - Expiration works only on latest versions
    - On versioning disabled buckets - deletes permanently the object.
    - On versioning enabled/suspended buckets - create a delete-marker.

3. **NoncurrentVersionExpiration**

    Find expired objects (non current version) based on -
    - NoncurrentDays - Number of days passed from creation of the object.
    - NewerNoncurrentVersions - Number of newer versions that must exist in addition to the expired objects (the most old versions). 

4. **AbortIncompleteMultipartUpload** 

    Find incomplete multipart uploads  based on -
    - DaysAfterInitiation - Number of days passed from initiation of the multipart upload.

5. **Transition**

6. **NonCurrentVersionTransition**

For more info, see - 
* [AWS S3 object lifecycle mgmt documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
* [lifecycle introduction rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html) 

## NooBaa NC Supported Lifecycle Policy Rules
The following list contains the supported lifecycle policy rules in NooBaa Non Containerized - 
1. Filter
2. Expiration
3. NoncurrentVersionExpiration
4. AbortIncompleteMultipartUpload

## Lifecycle policy configuration instructions

### Prerequisites

- NooBaa deployed and running
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/reference/s3api/) installed. 
- An account already created.
- A target bucket already created by the account of the prerequisite above.


1. Create a lifecycle policy file - 
This example will delete all objects older than 30 days.

```bash
cat policy.json
{
  "Rules": [
    {
      "ID": "expire-old-objects",
      "Filter": {
        "Prefix": ""
      },
      "Status": "Enabled",
      "Expiration": {
        "Days": 30
      }
    }
  ]
}
```

2. Apply the lifecycle policy - 
```bash
AWS_ACCESS_KEY_ID=<access_key> AWS_SECRET_ACCESS_KEY=<secret_key> aws s3api put-bucket-lifecycle-configuration \
  --bucket <your-bucket-name> \
  --lifecycle-configuration file://lifecycle.json \
  --endpoint-url <noobaa-endpoint>
```

3. Verify the lifecycle policy - 
```bash
AWS_ACCESS_KEY_ID=<access_key> AWS_SECRET_ACCESS_KEY=<secret_key> aws s3api get-bucket-lifecycle-configuration \
  --bucket <your-bucket-name> \
  --endpoint-url <noobaa-endpoint>
```

For more info, see - 
[S3 api CLI put bucket lifecycle configuration](https://docs.aws.amazon.com/cli/latest/reference/s3api/put-bucket-lifecycle-configuration.html)