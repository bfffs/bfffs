BFFFS Implementation
====================

---
author: Alan Somers <asomers@gmail.com>
---

Layers
------

TODO

Testing strategy
----------------

BFFFS relies on four types of tests.  All functionality must be tested by at
least one, whichever is most appropriate, but more is better.

* Unit tests.  These live in the `src` directory and target one layer only.
  Each layer's unit tests operate on a mock version of the lower layer.  For
  example, the unit tests for `Pool` use a `MockCluster` instead of a real
  `Cluster`.  Unit tests are therefore the fastest and most reliable type of
  test, but have relatively high maintenance (because they access private
  implementation details), and don't always reflect realistic usage.  They are
  also useful for injecting error conditions that can't be tested any other
  way.

* Functional tests.  These live in the `tests` directory. Each functional test
  module targets one layer, but operates on a full stack, up to the layer of
  interest.  That is, the functional tests for `Pool` construct real `Cluster`,
  `Raid`, etc.  So these are somewhat slower than the unit tests, somewhat
  lower maintenance, and somewhat more realistic.  But they're still pretty
  fast.

* Integration tests.  These also live in the `tests` directory.  Unlike unit
  and functional tests, these run the entire application.  That makes them
  relatively slow, but very similar to what an actual user would do.  Still,
  each integration test should be as targetted as possible.

* Torture tests.  These are _not_ focused.  Instead, they just do a lot of
  random actions in an attempt to stress the system.  They may operate on the
  entire application, or on some subset.  What they have in common is that they
  are _very_ slow.  The environment variable `BFFFS_TORTURE_SCALE` may be used
  to adjust their runtimes.
