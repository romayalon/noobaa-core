# IAM

## Glossary
**Access keys** = a pair of access key ID (in short: access key) and secret access key (in short: secret key)  
**ARN** = Amazon Resource Name  
**CRUD** = Create, Read, Update, Delete  
**IAM** =  Identity and Access Management  
**NC** = Non-Containerized  
**NSFS** = Namespace Store File System  


## Goal
Ability to operate NooBaa accounts for NC NSFS using IAM API ([AWS documentation](https://docs.aws.amazon.com/iam/)).  
A created user will be able to get access to NooBaa resources (buckets, objects).

## Background
- Currently, we create NC NSFS accounts using the NooBaa CLI, which is a CLI command with root (privileged) permissions:
    ```bash
    noobaa-cli account add [flags]
    ```
- The NS NSFS account is saved as a JSON file with root permissions (under the default path: `/etc/noobaa.conf.d/accounts/<name>.json`).
- The structure of a valid account is determined by schema and validated using avj.  
There are a couple of required properties specific to NSFS: `nsfs_account_config` that include a UID and GID or a Distinguished Name.
- When an account is created the json reply contains all the details of the created account (as they are stored in the JSON file).

## Problem
As mentioned, for NooBaa NC NSFS deployments, the only way to create and update accounts is via the CLI.   
For certain deployments exposing the CLI is not a viable option (for security reasons, some organizations disable the SSH to a machine with root permissions).

## Scenarios
### In Scope
Support IAM API:  
- CreateUser, GetUser, UpdateUser, DeleteUser, ListUsers.  
- CreateAccessKey, GetAccessKeyLastUsed, UpdateAccessKey, DeleteAccessKey, ListAccessKeys.
### Out of Scope
At this point we will not support additional IAM resources (group, policy, role, etc).

## Architecture
![IAM FLOW](./images/IamCreateUserSd.png)

- The boilerplate code is based on STS and S3 services  
- IAM service will be supported in NSFS service (which requires the endpoint)
- In the endpoint we created the `https_server_iam`
- The server would listen to a new port `https_port_iam`
  - It will be a separate port  
  - During development phase will default to -1 to avoid listening to the port
- To create the server we created the `endpoint_request_handler_iam`.
  - The `iam_rest` that either `handle_request` or `handle_error`
  - The `IamError` class.
  - The the ops directory and each supported action will be a file with name `iam_<action>`
- We created the `AccountSDK` class and the `AccountSpace` interface:
  - The `AccountSpace` interface is defined in `nb.d.ts`
  - The initial (current) implementation is only `AccountSpaceFS`
  - `AccountSpaceFS` will contain all our implementations related to users and access keys - like we have for other resources: `NamespaceFS` for objects, `BucketSpaceFS` for buckets, etc

### Clarification:
- NC NSFS account config - represents root accounts and IAM users details (both are called “account” in our system), contains the details of: `user`, `nsfs_account_config`, `access keys`.
- The design approach:
  - Multi-users FS - serves different GID and UIDs.
  - Multi-tenant - can be several root users.
- `owner` vs `creator` - owner is permission wise, creator is for internal information.


### The user and access keys creation flow:
One root and one user (just to understand the basic API relations and hierarchy)
![One root and one user diagram](https://github.com/noobaa/noobaa-core/assets/57721533/b77ade91-11dd-415c-b3f0-5f3f1747a694)

One root account, multiple users (Multi-users FS)
![One root account, multiple users diagram](https://github.com/noobaa/noobaa-core/assets/57721533/792b7115-f6cb-40b3-89ee-4f47c6489924)

Multiple root accounts, multiple users (Multi-users FS, Multi-tenant)
![Multiple root accounts, multiple users diagram](https://github.com/noobaa/noobaa-core/assets/57721533/ae642825-81e1-4a27-bd2d-00583da7d663)


- Using NooBaa CLI to create a root account.
  - We need the request to have access key id and secret key in a known account.
- Use the access key and secret key of the root account to CreateUser
  - We will create the NSFS account with the same: `uid` and `gid` or `distinguished_name`, `new_buckets_path` and `allow_bucket_creation`.
  - At this point the user doesn’t have access keys (empty array), hence `account_data.access_keys = []`
- Use the access key and secret key of the root account to CreateAccessKey
  - First time - the root account will generate the access keys.
  - Then, CreateAccessKey can also be used by the user.
  - When a CreateAccessKey - need to verify that the array length is maximum 2.
Source: AccessKeys
- Then the user can run action from the S3 service on the resources (bucket and object operations in NC NSFS).
- **Implicit policy** that we use:
  - User (Create, Get, Update, Delete, List) - only root account
  - AccessKey (Create, Update, Delete, List)
    - root account
    - all IAM users only for themselves (except the first creation that can be done only by the root account).

### Root Accounts Manager
The root accounts managers are a solution for creating root accounts using the IAM API.

- The root accounts managers will be created only using the CLI (can have more than one root account manager).
- It is not mandatory to have a root account manager, it is only for allowing the IAM API for creating new root accounts, but this account does not owns the root accounts.
- The root accounts manager functionality is like root account in the IAM API perspective:
  - We use root accounts to create IAM users: We use root accounts manager to create root accounts
  - We use root accounts to create the first access key of an IAM user: We use root accounts manager to create the first access key of a root account.
- When using IAM users API:
  - root accounts manager can run IAM users create/update/delete/list - only on root accounts (not on other IAM users).
root accounts manager can run IAM access keys create/update/delete/list - only on root accounts and himself.

Here attached a diagram with all the accounts that we have in our system:
![All accounts diagram](https://github.com/noobaa/noobaa-core/assets/57721533/c4395c06-3ab3-4425-838b-c020ef7cc38a)