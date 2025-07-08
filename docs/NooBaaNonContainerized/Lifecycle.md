# NooBaa Non Containerized - Lifecycle

1. [Introduction](#introduction)
2. [General Information](#general-information)
3. [AWS S3 Lifecycle Policy reminder](#aws-s3-lifecycle-policy-reminder)
4. [Supported Lifecycle Policy Rules](#noobaa-nc-supported-lifecycle-policy-rules)


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

## NooBaa NC Supported Lifecycle Policy Rules
The following list contains the supported lifecycle policy rules in NooBaa Non Containerized - 
1. Filter
2. Expiration
3. NoncurrentVersionExpiration
4. AbortIncompleteMultipartUpload