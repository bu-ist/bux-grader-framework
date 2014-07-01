# Changelog

## 0.4.0

* Adds automated recovery for extended XQueue outages.
* Adds dead letter queues / expiration times to appropriately handle failed evaluation attempts due to exhausted system resources.
* Adds a utility method -- safe_multi_call -- to wrap function calls that may fail intermittently and warrant reattempts.
* Moves XQueue `push_failure` method from worker classes to XQueueClient.
* Removes unused queue classes.