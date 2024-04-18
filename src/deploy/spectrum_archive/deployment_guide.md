# NooBaa Spectrum Scale/Archive Deployment Guide
This deployment guide goes over the steps that needs to be taken to get NooBaa running on GPFS filesystem. The steps in this guide should be followed in the same order as mentioned in the guide.

## NooBaa Prerequisites
NooBaa has the following prerequisites and expects them to be in place before proceeding with its installation and deployment.
1. Host is a Red Hat OS like Centos/RHEL.
2. Spectrum Scale along with Spectrum Archive must be in installed on the host machine.
3. libboost RPM packages must be already installed. In particular, `boost-system` and `boost-thread` packages are required. Without them in place, NooBaa installation will fail. Can be checked by running `rpm -q boost-system boost-thread`.

## NooBaa Installation
NooBaa is packaged as a RPM which needs to be installed in order to be able to use NooBaa.

1. Install by using `dnf`, `yum` or `rpm`.
   - Example: `rpm -i noobaa-core-5.15.0-1.el8.x86_64.20231009`.
2. NooBaa RPM installation should provide the following things
	1. `noobaa-s3.service` file located at `/usr/lib/systemd/system/noobaa-s3.service`.
	2. NooBaa source available at `/usr/local/noobaa-core`.

## NooBaa Configuration
NooBaa needs some configurations to be in place before we start up the NooBaa process and it is important to ensure that this is done before starting up the service.

### Configure NooBaa User
Before proceeding, please ensure there are no stale items in `/etc/noobaa.conf.d` from a previous (not in use) NooBaa setup. If there is this directory with stale data then remove it by runnign `rm -r /etc/noobaa.conf.d`.

In order to be able to access NooBaa, the user should create a account. This can be done in the following way.
```console
$ cd /usr/local/noobaa-core
$ bin/node src/cmd/noobaa-cli.js account add --access_key <access-key> --secret_key <secret-key> --name <name-of-user> --new_buckets_path <path-to-store-bucket-data>
```

NOTE: `<path-to-store-bucket-data>` should already exist or else the above command will throw error.

Following the above steps we will create a new user for NooBaa with the given name. The user will be able to access the NooBaa S3 endpoint with the access key and secret key pair.

#### Example
```console
$ cd /usr/local/noobaa-core
$ mkdir /ibm/gpfs/noobaadata #Bucket Data Path should already exist
$ export AWS_ACCESS_KEY_ID=$(openssl rand -hex 20)
$ export AWS_SECRET_ACCESS_KEY=$(openssl rand -hex 20)
$ bin/node src/cmd/noobaa-cli.js account add --access_key $AWS_ACCESS_KEY_ID --secret_key $AWS_SECRET_ACCESS_KEY --name noobaa --new_buckets_path /ibm/gpfs/noobaadata
```

### Configure NooBaa
```console
$ cat >/etc/noobaa.conf.d/.env <<EOF
ENDPOINT_PORT=80
ENDPOINT_SSL_PORT=443
ENDPOINT_FORKS=8
UV_THREADPOOL_SIZE=64

EOF
```

```console
$ cat >/usr/local/noobaa-core/config-local.js <<EOF
/* Copyright (C) 2023 NooBaa */
'use strict';

/** @type {import('./config')} */
const config = exports;

config.NSFS_RESTORE_ENABLED = true;
EOF
```

### Configure Archiving
The following will setup appropriate spectrum scale policies which will assist NooBaa in moving data between different pools.

```console
$ cd /usr/local/noobaa-core
$ chmod +x ./src/deploy/spectrum_archive/setup_policies.sh
$ ./src/deploy/spectrum_archive/setup_policies.sh <device-or-directory-name> <noobaa-bucket-data-path> <tape-pool-name>
```
Here,
- `device-or-directory-name` is the name of the GPFS device or directory name. You may be able to find this by running `mount | grep gpfs`.
- `noobaa-bucket-data-path` is the path on GPFS where NooBaa is storing the data. This path should be the same as we passed in the [Configure NooBaa User](#configure-noobaa-user)'s `<path-to-store-bucket-data>`.
- `tape-pool-name` should be a valid tape pool name. You can find this by running `eeadm pool list`.

#### Example
```console
$ cd /usr/local/noobaa-core
$ chmod +x ./src/deploy/spectrum_archive/setup_policies.sh
$ ./src/deploy/spectrum_archive/setup_policies.sh /ibm/gpfs /ibm/gpfs/noobaadata pool1
```

## Start NooBaa
```console
$ systemctl start noobaa-s3
$ systemctl enable noobaa-s3 #optional
$ systemctl status noobaa-s3 # You should see status "Active" in green color
```

## Test NooBaa Installation
Now that NooBaa has been installed and is active, we can test out the deployment.
These AWS commands will read `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from the environment, ensure that these are available in the environment and should be the same that we used in [configure NooBaa user](#configure-noobaa-user).

### Basic Testing
```console
$ aws s3 --endpoint https://localhost:443 --no-verify-ssl mb s3://first.bucket # Create a bucket named first.bucket
make_bucket: first.bucket
$ aws s3 --endpoint https://localhost:443 --no-verify-ssl ls # List all of the buckets
2023-10-05 21:18:45 first.bucket
```

### Test `GLACIER` Storage Class
```console
$ # Upload "somefile" to "first.bucket" bucket that we created in the previous step. Here "--storage-class GLACIER"
$ # tells the S3 server to store the file onto tape instead of disk.
$ aws s3 --endpoint https://localhost:443 --no-verify-ssl cp somefile s3://first.bucket --storage-class GLACIER
upload: ./somefile to s3://first.bucket/somefile
$ # Trying to copy file back without restoring will fail because the file is in GLACIER storage class
$ aws s3 cp --endpoint https://localhost:443 --no-verify-ssl s3://first.bucket/somefile somefile.cp
warning: Skipping file s3://first.bucket/somefile. Object is of storage class GLACIER. Unable to perform download operations on GLACIER objects. You must restore the object to be able to perform the operation. See aws s3 download help for additional parameter options to ignore or force these transfers.
$ # Let's issue a restore request. This will return immediately but will take about 15 mins for the restore to finish.
$ aws s3api --endpoint https://localhost:443 --no-verify-ssl restore-object --bucket first.bucket --key somefile --restore-request '{"Days": 1}'
$ # We can check the restore request status
$ aws s3api head-object --endpoint https://localhost:443 --no-verify-ssl --bucket first.bucket --key somefile
{
    "AcceptRanges": "bytes",
    "Restore": "ongoing-request=\"true\"",
    "LastModified": "2023-10-12T11:01:06+00:00",
    "ContentLength": 8883,
    "ETag": "\"mtime-cw6eqqo588w0-ino-5n5k\"",
    "ContentType": "application/octet-stream",
    "Metadata": {
        "storage_class": "GLACIER"
    },
    "StorageClass": "GLACIER"
}
$ # Checking again after 15 minutes - The object has been restored
$ aws s3api head-object --endpoint https://localhost:443 --no-verify-ssl --bucket first.bucket --key somefile
{
    "AcceptRanges": "bytes",
    "Restore": "ongoing-request=\"false\", expiry-date=\"Thu, 26 Oct 2023 00:00:00 GMT\"",
    "LastModified": "2023-10-12T11:01:06+00:00",
    "ContentLength": 8883,
    "ETag": "\"mtime-cw6eqqo588w0-ino-5n5k\"",
    "ContentType": "application/octet-stream",
    "Metadata": {
        "storage_class": "GLACIER"
    },
    "StorageClass": "GLACIER"
}
$ # Once the restore succeeds we can download the file
$ aws s3 cp --endpoint https://localhost:443 --no-verify-ssl s3://first.bucket/somefile somefile.cp
download: s3://first.bucket/somefile to ./somefile.cp
```

## Log and Logrotate
Noobaa logs are configured using rsyslog and logrotate. RPM will configure rsyslog and logrotate if both are already running.

Rsyslog status check
```
systemctl status rsyslog
```

Noobaa logs are pushed to `/var/log/noobaa.log` and the log is rotated and compressed daily.

Verify the rsyslog and logrotate rpm configuration is complete by checking the files `/etc/rsyslog.d/noobaa_syslog.conf` and `/etc/rsyslog.d/noobaa_rsyslog.conf` for rsyslog and `/etc/logrotate.d/noobaa/logrotate_noobaa.conf` for logrotate.These files contain the noobaa specific configuration for rsyslog and logrotate.

Rotate the logs manually.

```
logrotate /etc/logrotate.d/noobaa/logrotate_noobaa.conf
```

# FAQ
- What happens if I forget the credentials used to generate NooBaa User?
  - You can find all of the NooBaa accounts details here: `/etc/noobaa.conf.d/accounts`.
- How do I add new users?
  - You can repeat the command that we ran in the section [configure NooBaa User](#configure-noobaa-user). You need to make sure that the access key **must not be reused**.
- My migrations/restores aren't working!
  - You can find the migration/restore related logs in your crontab logs.
