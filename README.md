gen_gossip
==========

*gen_gossip* allows you to quickly implement gossip protocols in Erlang.

Reason
======

This application was built to DRY up one of my existing projects. Having
implemented several gossip protocols in the past I needed a way to re-use shared
code common to many gossip based algorithms.

Features
========

* Supports Aggregation-Based Protocols based on this [whitepaper](http://www.cs.unibo.it/bison/publications/aggregation-tocs.pdf)
* Safe-guards for preventing your network from being flood with gossip messages
* Ability to subscribe to node events: netsplits, joining, leaving, etc.

Usage
=====

See examples folder in src directory

Status
======

This is still a young project if you see anything wrong please
open an issue or send a pull request.
