# jepsen.datomic

Tests for the Datomic distributed database.

## Quickstart

```
lein run test TODO
```

## Setup

This test runs Datomic on top of DynamoDB. You'll need an AWS account, and an
AWS IAM user for Datomic.

To create a user, go to the [IAM
console](https://us-east-1.console.aws.amazon.com/iam/home?region=us-east-1#/users)
and click "Create User". Choose a name (e.g. "datomic-jepsen") and click
"Next". Choose "Attach policies directly", and create a policy adapted from [Datomic's
list](https://docs.datomic.com/pro/overview/storage.html#iam-role-configuration). The docs are missing several IAM permissions you'll need, so there are more here.

<b>THIS IS PROBABLY TOO BROAD A POLICY. Every time I try to use IAM I enter a dissociative fugue state. If you are an IAM expert and can make this more secure, please send a PR.</b>

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "iam:CreateInstanceProfile",
                "iam:GetRole",
                "iam:PassRole",
                "iam:ListRoles",
                "iam:CreateRole",
                "iam:PutRolePolicy",
                "iam:GetUser",
                "iam:ListRolePolicies",
                "iam:AddRoleToInstanceProfile"
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
