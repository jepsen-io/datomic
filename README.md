# jepsen.datomic

Tests for the Datomic distributed database.

## Quickstart

```
lein run test TODO
```

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
