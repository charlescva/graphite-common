# graphite-common
Graphite Related Daemons to push metrics into graphite.

Currently, the zkCli class is a singleton that calls on a public method in the other Test class every 60 seconds that will fetch storm topology and executor information and push metrics to graphite's carbon-cache.

There are some issues w/ it im sure, and the base pom.xml doesnt include the dependencies, but the code works once imports are sorted.

I'll fix the pom some other time.
