# jepsen.datomic

Tests for the Datomic distributed database.

<b>WARNING: THIS WILL DELETE AND CREATE IAM ROLES AND DATABASES.</b>
Specifically, it deletes and re-creates the DynamoDB table `datomic-jepsen`,
and deletes *all* IAM roles starting with `datomic-aws`; it creates
automatically-numbered IAM roles beginning with this prefix. It is probably a
bad idea to give this test access to an AWS account that stores Datomic data
you care about.</b>

## Quickstart

```
lein run test TODO
```

## Setup

There's some kind of issue around SequencedCollection I haven't figured out
that breaks JDK17 and below. Not sure what's pulling it in--the stacktrace
doesn't make sense. To upgrade Debian bookworm to trixie (testing), which has a
newer JDK, you can run:

```bash
sudo sed -i 's/bookworm/trixie/g' /etc/apt/sources.list.d/debian.sources
sudo apt update
sudo apt install -y openjdk-22-jdk-headless
```

This test runs Datomic on top of DynamoDB. You'll need an AWS account, and an
AWS IAM user for Datomic.

To create a user, go to the [IAM
console](https://us-east-1.console.aws.amazon.com/iam/home?region=us-east-1#/users)
and click "Create User". Choose a name (e.g. "datomic-jepsen") and click
"Next". Choose "Attach policies directly", and create a policy adapted from [Datomic's
list](https://docs.datomic.com/pro/overview/storage.html#iam-role-configuration). The docs are missing several IAM permissions you'll need, so there are more here.

<b>WARNING: THIS IS PROBABLY TOO BROAD A POLICY.</b> Every time I try to use IAM I enter a dissociative fugue state. If you are an IAM expert and can make this more secure, please send a PR.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
              "iam:CreateInstanceProfile",
              "iam:DeleteInstanceProfile",
              "iam:GetRole",
              "iam:DeleteRole",
              "iam:DeleteRolePolicy",
              "iam:PassRole",
              "iam:ListRoles",
              "iam:CreateRole",
              "iam:PutRolePolicy",
              "iam:GetUser",
              "iam:ListRolePolicies",
              "iam:AddRoleToInstanceProfile",
              "iam:ListInstanceProfilesForRole",
              "iam:RemoveRoleFromInstanceProfile"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": "dynamodb:*",
            "Resource": "arn:aws:dynamodb:*:AWS-ACCOUNT-ID:table/datomic-jepsen"
        },
        {
            "Effect": "Allow",
            "Action": "dynamodb:ListTables",
            "Resource": "arn:aws:dynamodb:*:AWS-ACCOUNT-ID:table/*"
        }
    ]
}
```

For `TABLE-NAME`, use "datomic-jepsen". For `AWS-ACCOUNT-ID`, click your
username in the top right side of the AWS console; "Account ID" is at the top
of the menu. Click "Next", name the policy (e.g. "datomic-jepsen"), and click" "Create policy".

Now return to the create-user workflow, and type the name of the newly-created
policy into "Permissions policies", and select it. Click "Next", then "Create
user".

Click the newly-created user name, then in the top bar for the user, click
"Create access key". Choose "Application running outside AWS". Click "Next".
Copy the access key and secret, and create a file in the top-level
`jepsen.datomic` directory called `aws.edn`:

```edn
{:access-key-id "ABC..."
 :secret-key "123..."}
```

The test will use this file to provision and talk to DynamoDB.

## Design

Datomic uses a fat client library called a "peer". You can't easily run
multiple peers in a single JVM process, so rather than run several peers inside
of Jepsen directly, we deploy a small Clojure service that embeds the peer
library to each node. That service speaks to Jepsen using an HTTP+EDN API.

Datomic supports multiple storage engines. In this test, we use DynamoDB
storage. This requires an AWS account.

## License

Copyright © 2024 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
