egossip
=======

*egossip* allows you to quickly implement gossip protocols in Erlang.

Reason
======

This application was built to DRY up one of my existing projects. Having
implemented several gossip protocols in the past I needed a way to re-use shared code common
to many gossip based algorithms.

Features
========

* Safe-guards for preventing your network from being flood with gossip messages
* Ability to register on node events: netsplits, joining, leaving, etc.
* Prevent new nodes from joining in on a aggregation based protocols

Status
======

Still being developed, just pushing it to my Github account to work from home.
