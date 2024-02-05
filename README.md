# jepsen.datomic

Tests for the Datomic distributed database.

<b>WARNING: THIS WILL DELETE AND CREATE IAM ROLES AND DATABASES.</b>
Specifically, it deletes and re-creates the DynamoDB table `datomic-jepsen`,
and deletes *all* IAM roles starting with `datomic-aws`; it creates
automatically-numbered IAM roles beginning with this prefix. It is probably a
bad idea to give this test access to an AWS account that stores Datomic data
you care about.</b>

## Quickstart

For a quick test with process pauses:

```
lein run test --concurrency 2n --rate 500 --nemesis pause --time-limit 60
```

For a long test which switches randomly between different subsets of all
available nemeses:

```
lein run test --concurrency 5n --rate 10000 --nemesis-stable-period 100 --time-limit 10000
```

For a very long suite of tests with different combinations of nemeses and sync:

```
lein run test-all --concurrency 5n --rate 10000 --nemesis-stable-period 100 --time-limit 10000 --test-count 10
```

This runs a whole sequence of tests with different choices of nemesis.

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

There are two modes for AWS auth. You can let the test harness provision a DynamoDB table and various IAM roles for you, or you can provide an existing Dynamo table and handle IAM roles yourself.

To handle Dynamo and IAM roles yourself, pass a dynamo table name and IAM roles
to the test suite. The test suite will use the table you provided. Note that
you cannot run more than one test in a row safely: `--test-count 4` and `lein
run test-all` will keep using the same Dynamo table, leaking state from one run
into the next.

```
lein run test --dynamo-table datomic-jepsen --aws-transactor-role datomic-aws-transactor --aws-peer-role datomic-aws-peer ...
```

To have the test suite provision Dynamo and roles for you, start by creating an IAM user. Go to the [IAM
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

The test will use this file to provision and talk to DynamoDB and other AWS
resources. It will also provide these credentials to both transactor and peer,
so they can interact with Dynamo. Credentials are stored in
`/etc/systemd/system/datomic-transactor.service` on transactors, and
`/etc/systemd/system/datomic-peer.service` on peers, and are passed in as
environment variables to the actual transactor and peer processes by systemd.

## Workloads

There are four main workloads, selectable with `-w workload-name`. `append` and
`append-cas` perform transactions of reads and appends to lists encoded in two
differnet ways. `append` provides a blend of Strong Session Serializable and
Strong Serializable; see the Jepsen report for details. `append-cas` yields
Strong Session Snapshot Isolation. `internal` is a very short test with a
handful of hand-coded examples of internal consistency; it measures both
Datomic's behavior, and what you'd expect from a database with serial
intra-transaction semantics. `grant` demonstrates a write-skew analogue within
a single transaction: an invariant violation that arises due to concurrent
logical execution of two transaction functions.

## Variations

To sync on every read, use `--sync`.

To test an alternate Datomic version, use (e.g.) `--version 1.0.7021`. This controls the version used by both transactor and peer.

To test a specific consistency model, use (e.g.) `--consistency-model
strong-serializable`.

Select faults to inject with (e.g.) `--nemesis partition,pause`:

- `partition`: isolates nodes from one another
- `partition-storage`: isolates nodes from storage
- `pause`: pauses and resumes processes with SIGSTOP
- `kill`: kills processes with SIGKILL
- `clock`: introduces clock errors, including bumping the clock forward or back
  by millis to hundreds of seconds, and strobing the clock rapidly between two
  times.
- `gc`: Requests that a random peer begin a garbage collection of all records
  older than `new Date()`

Nemesis actions occur roughly every `--nemesis-interval n` seconds.

For full docs, see `lein run test --help`.

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
