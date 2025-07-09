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

## Lifecycle CLI Worker
The NooBaa NC Lifecycle worker is a noobaa-cli command that is executed manually or by a nightly cron job.

### Lifecycle worker configuration - 
```js
NC_LIFECYCLE_LOGS_DIR = '/var/log/noobaa/lifecycle';
NC_LIFECYCLE_TIMEOUT_MS = 8 * 60 * 60 * 1000;
NC_LIFECYCLE_RUN_TIME = '00:00';
NC_LIFECYCLE_RUN_DELAY_LIMIT_MINS = 2;
config.NC_LIFECYCLE_TZ = 'LOCAL';
```

#### NC_LIFECYCLE_RUN_TIME 
The NooBaa NC Lifecycle worker is a noobaa-cli command that is expected to run at 00:00 by default, with a NC_LIFECYCLE_RUN_DELAY_LIMIT_MINS delay - by default 2 minutes. </br>
A nightly cron job that should run the CLI command nightly should be external to NooBaa (CES or other callers). Notice that NC_LIFECYCLE_RUN_TIME should be the same timezone as specified in NC_LIFECYCLE_TZ.

#### NC_LIFECYCLE_TIMEOUT_MS
The lifecycle worker has a timeout of NC_LIFECYCLE_TIMEOUT_MS = 8 hours by default.

#### NC_LIFECYCLE_LOGS_DIR
Each lifecycle run ends with a report that is written to NC_LIFECYCLE_LOGS_DIR which is located in `/var/log/noobaa/lifecycle` by default. </br>
When NooBaa is deployed on GPFS, NooBaa offloads the deletion candidates search to the file system but translating lifecycle policies to GPFS ILM policies.
During that process ILM policies and ILM candidates files will be created under NC_LIFECYCLE_LOGS_DIR. 

### Lifecycle CLI run example -
```
> noobaa-cli lifecycle                                 
> noobaa-cli lifecycle --disable_runtime_validation    // used for dev/tests
> noobaa-cli lifecycle --disable_service_validation    // used for dev/tests
``` 

##### Dev/Tests flags -
- **--disable_runtime_validation** - will successfully run without validating the runtime
- **--disable_runtime_validation** - will successfully run without validating noobaa service is up

### Lifecycle process algorithm 
At the beginning of the lifecycle worker run, NooBaa will check if GPFS optimization is enabled. If it's disabled, the default directory scanning (POSIX) process will start, else the GPFS optimization will be used when possible. <br>
Currently, GPFS optimization is enabled on expiration rules only. We would like to optimize more flows like the NonCurrentExpiration (Days) and AbortIncompleteMultipartUploads.

### General Algorithm - 
1. List all buckets.
2. Create GPFS candidates files - If GPFS and bucket lifecycle policy has rules that are supported in the GPFS ILM policies flow
    2.1. Find all mount points and create a map.
    2.2. Iterate all buckets
        2.2.1. If bucket has an expiration lifecycle policy - translate to a GPFS ILM policy (expiration) 
        2.2.2. Concat the GPFS ILM policy string to that matching policy file per file system mount.
    2.3. Iterate all mount points in the map from step 2.1.
        2.3.1 Apply the GPFS ILM policy - will create a candidates list of expired objects per rule. 
3. Iterate over buckets
   3.1. If the bucket has no lifecycle policy - continue
   3.2. While bucket processing is not finished and the number of processed objects is less than 10K - Iterate over the rules
   3.2.1. Process rule -
   3.2.2. For each rule element get the expired objects - 
      3.2.2.1. If rule element is expiration and GPFS enabled - read and parse 1000 candidates from the candidates list file. 
      3.2.2.2. Else - List/Scan the directory while filtering for finding 1000 expired objects according to the rule element.
3. Delete/Abort the expired objects/incomplete MPU candidates.

#### Lifecycle run status
Each lifecycle run generates a status report written to `var/log/noobaa/lifecycle` and contains the following information - 
* running_host
* lifecycle_run_times
  - contains start_time, end_time and took_ms of the whole run, and general functions such as list_buckets and process_buckets.
* total_stats
  - contains stats of successful/failed deletions/abortions.
* state
  - contains information about the state of the whole lifecycle worker run.
* buckets_statuses
  - A map that maps bucket_name to the lifecycle processing information per that bucket such as bucket_process_times, bucket_stats, state, rules_statuses and errors.
  * rules_statuses
  -  A map that maps rule_id to the lifecycle processing information per that rule such as rule_process_times, rule_stats, state and errors.
* errors
 - An array that contains informatio about errors that happened during the lifecycle process run.
* mount_points_statuses
  - contains mount_point_process_times and errors.

##### Example - 
```bash
sudo cat /var/log/noobaa/lifecycle/lifecycle_run_1750145866911.json | jq
{
  "running_host": "host1",
  "lifecycle_run_times": {
    "run_lifecycle_start_time": 1750145866911,
    "list_buckets_start_time": 1750145866911,
    "list_buckets_end_time": 1750145866912,
    "list_buckets_took_ms": 1,
    "process_buckets_start_time": 1750145866912,
    "process_buckets_end_time": 1750145866927,
    "process_buckets_took_ms": 15,
    "run_lifecycle_end_time": 1750145866928,
    "run_lifecycle_took_ms": 17
  },
  "total_stats": {
    "num_objects_deleted": 0,
    "num_objects_delete_failed": 0,
    "objects_delete_errors": [],
    "num_mpu_aborted": 0,
    "num_mpu_abort_failed": 0,
    "mpu_abort_errors": []
  },
  "state": {
    "is_finished": true
  },
  "buckets_statuses": {
    "bucket1": {
      "bucket_process_times": {
        "process_bucket_start_time": 1750145866912,
        "process_bucket_end_time": 1750145866927,
        "process_bucket_took_ms": 15
      },
      "bucket_stats": {},
      "state": {
        "num_processed_objects": 0,
        "is_finished": true
      },
      "rules_statuses": {
        "expire-data": {
          "state": {
            "expire": {},
            "noncurrent": {},
            "is_finished": true
          },
          "rule_process_times": {
            "process_rule_start_time": 1750145866914,
            "get_candidates_start_time": 1750145866917,
            "get_candidates_end_time": 1750145866927,
            "get_candidates_took_ms": 10,
            "process_rule_end_time": 1750145866927,
            "process_rule_took_ms": 13
          },
          "rule_stats": {},
          "errors": [
            "No such file or directory"
          ]
        }
      }
    }
  },
  "mount_points_statuses": {}
}
```
## GPFS optimization

As mentioned above, GPFS optimization is enabled on expiration rules only. In the future, we would like to optimize more flows like the NonCurrentExpiration (Days) and AbortIncompleteMultipartUploads.
During the lifecycle worker run, NooBaa will translate expiration rules to GPFS ILM policies and will create a single lifecycle file per mount point, this is done in order to optimize GPFS files search.
After creating the GPFS ILM policies, the lifecycle worker will apply the policies and GPFS will create candidates files per rule.

### Example - 
In the below example, there are 3 buckets in NooBaa that configured with a lifecycle policy that contains expiration element.
- bucket1 - bucket path is under `/mnt/gpfs0/`, lifecycle policy contains 2 rules.
- bucket2 - bucket path is under `/mnt/gpfs0/`, lifecycle policy contains 1 rule.
- bucket3 - bucket path is under `/mnt/cesSharedRoot/`, lifecycle policy contains 1 rule.

#### Policy and candidates files structure - 
```
tree /var/log/noobaa/lifecycle/
/var/log/noobaa/lifecycle/
├── lifecycle_ilm_candidates
│   ├── list.bucket1_rule1_1750346487715
│   ├── list.bucket1_rule2_1750346487715
│   ├── list.bucket2.rule1_1750346487715
│   ├── list.bucket3.rule1_1750346487715
├── lifecycle_ilm_policies
│   ├── noobaa_ilm_policy_%2Fmnt%2FcesSharedRoot_1750673997496
│   └── noobaa_ilm_policy_%2Fmnt%2Fgpfs0_1750673997496
├── lifecycle_run_1750346286043.json
```

##### Policy file example - 
```bash
sudo cat /var/log/noobaa/lifecycle/lifecycle_ilm_policies/noobaa_ilm_policy_%2Fmnt%2Fgpfs0_1750673997496
define( mod_age, (DAYS(CURRENT_TIMESTAMP) - DAYS(MODIFICATION_TIME)) )
define( change_age, (DAYS(CURRENT_TIMESTAMP) - DAYS(CHANGE_TIME)) )
RULE 'bucket1_rule1_1750346487715' LIST 'bucket1_rule1_1750346487715'
WHERE PATH_NAME LIKE '/mnt/gpfs0/bucket1/%' ESCAPE '\'
AND PATH_NAME NOT LIKE '/mnt/gpfs0/bucket1/.noobaa-nsfs%/%' ESCAPE '\'
AND PATH_NAME NOT LIKE '/mnt/gpfs0/bucket1/.versions/%' ESCAPE '\'
AND PATH_NAME NOT LIKE '/mnt/gpfs0/bucket1/%/.versions/%' ESCAPE '\'
AND mod_age > 1
AND PATH_NAME LIKE '/mnt/gpfs0/bucket1/dir1/file%' ESCAPE '\'

define( mod_age, (DAYS(CURRENT_TIMESTAMP) - DAYS(MODIFICATION_TIME)) )
define( change_age, (DAYS(CURRENT_TIMESTAMP) - DAYS(CHANGE_TIME)) )
RULE 'bucket1_rule2_1750346487715' LIST 'bucket1_rule2_1750346487715'
WHERE PATH_NAME LIKE '/mnt/gpfs0/bucket1/%' ESCAPE '\'
AND PATH_NAME NOT LIKE '/mnt/gpfs0/bucket1/.noobaa-nsfs%/%' ESCAPE '\'
AND PATH_NAME NOT LIKE '/mnt/gpfs0/bucket1/.versions/%' ESCAPE '\'
AND PATH_NAME NOT LIKE '/mnt/gpfs0/bucket1/%/.versions/%' ESCAPE '\'
AND mod_age > 1
AND PATH_NAME LIKE '/mnt/gpfs0/bucket1/dir2/file%' ESCAPE '\'

define( mod_age, (DAYS(CURRENT_TIMESTAMP) - DAYS(MODIFICATION_TIME)) )
define( change_age, (DAYS(CURRENT_TIMESTAMP) - DAYS(CHANGE_TIME)) )
RULE 'bucket2_rule1_1750346487715' LIST 'bucket2_rule1_1750346487715'
WHERE PATH_NAME LIKE '/mnt/gpfs0/bucket2/%' ESCAPE '\'
AND PATH_NAME NOT LIKE '/mnt/gpfs0/bucket2/.noobaa-nsfs%/%' ESCAPE '\'
AND PATH_NAME NOT LIKE '/mnt/gpfs0/bucket2/.versions/%' ESCAPE '\'
AND PATH_NAME NOT LIKE '/mnt/gpfs0/bucket2/%/.versions/%' ESCAPE '\'
AND mod_age > 1
AND PATH_NAME LIKE '/mnt/gpfs0/bucket2/dir1/file%' ESCAPE '\'
```

##### Candidates file example - 
```bash 
sudo cat /var/log/noobaa/lifecycle/lifecycle_ilm_candidates/list.bucket1_rule1_1750346487715
91097 759635934 0   -- /mnt/gpfs0/bucket1/dir1/file1.txt
91097 759635934 0   -- /mnt/gpfs0/bucket1/dir1/file2.txt
91097 759635934 0   -- /mnt/gpfs0/bucket1/dir1/file3.txt
91097 759635934 0   -- /mnt/gpfs0/bucket1/dir1/file4.txt
91097 759635934 0   -- /mnt/gpfs0/bucket1/dir1/file5.txt
```

## Health CLI
The health CLI will have a new flag --lifecycle that will provide health information originated from the NC lifecycle process on the current host. 
Example - `noobaa-cli diagnose health --lifecycle`.
The health CLI will read the last lifecycle run status and will evaluate that last lifecycle worker run health on the same host.

Example - 
```
> noobaa-cli diagnose health --lifecycle
{
  "response": {
    "code": "HealthStatus",
    "message": "Health status retrieved successfully",
    "reply": ...,
    "checks": {
     ...
     "latest_lifecycle_run_status": {
          "total_stats": {
            "num_objects_deleted": 0,
            "num_objects_delete_failed": 0,
            "objects_delete_errors": [],
            "num_mpu_aborted": 0,
            "num_mpu_abort_failed": 0,
            "mpu_abort_errors": []
          },
          "lifecycle_run_times": {
            "run_lifecycle_start_time": 1750145995676,
            "list_buckets_start_time": 1750145995676,
            "list_buckets_end_time": 1750145995677,
            "list_buckets_took_ms": 1,
            "process_buckets_start_time": 1750145995677,
            "process_buckets_end_time": 1750145995692,
            "process_buckets_took_ms": 15,
            "run_lifecycle_end_time": 1750145995692,
            "run_lifecycle_took_ms": 16
        }
      }
    }
  }
}
```
## Lifecycle Events

The following new events will be logged to the events.log file -
1. Lifecycle worker started 
2. Lifecycle worker completed 
3. Lifecycle worker failed 

Examples of the new events - 
```
EVENT]{"timestamp":"2025-03-09T14:09:54.580Z","host":"hostname1","event":{"code":"noobaa_lifecycle_worker_started","message":"NooBaa Lifecycle worker run started.","description":"NooBaa Lifecycle worker run started.","entity_type":"NODE","event_type":"INFO","scope":"NODE","severity":"INFO","state":"HEALTHY","pid":91017}}
EVENT]{"timestamp":"2025-03-09T14:09:54.597Z","host":"hostname1","event":{"code":"noobaa_lifecycle_worker_finished_successfully","message":"NooBaa Lifecycle worker run finished successfully.","description":"NooBaa Lifecycle worker finished successfully.","entity_type":"NODE","event_type":"INFO","scope":"NODE","severity":"INFO","state":"HEALTHY","pid":91017}}
EVENT]{"timestamp":"2025-03-09T14:48:22.773Z","host":"hostname1","event":{"code":"noobaa_lifecycle_worker_failed","message":"NooBaa Failed to run lifecycle worker.","description":"NooBaa Lifecycle worker run failed due to an error.","entity_type":"NODE","event_type":"ERROR","scope":"NODE","severity":"ERROR","state":"DEGRADED","pid":97456}
```

## Notifications 
2 new event types are available for the NC lifecycle feature - 
1. `s3:LifecycleExpiration:Delete` - notifies when -
 - An object in an unversioned bucket is deleted.
 - An object version is permanently deleted.
2. `s3:LifecycleExpiration:DeleteMarkerCreated` - notifies when -
 - A delete marker is created after a current version of an object in a versioned bucket is deleted.