cloud-tasks-deferred
====================

.. image:: https://github.com/grktsh/python-cloud-tasks-deferred/workflows/CI/badge.svg
   :alt: CI
   :target: https://github.com/grktsh/python-cloud-tasks-deferred/actions

.. image:: https://codecov.io/gh/grktsh/python-cloud-tasks-deferred/branch/master/graph/badge.svg
   :alt: Coverage
   :target: https://codecov.io/gh/grktsh/python-cloud-tasks-deferred

A deferred library for Google Cloud Tasks.

Prerequisites
-------------

- Environment variable ``QUEUE_LOCATION`` in which Cloud Tasks service runs

Unavailable options
-------------------

- ``_transactional``: Transactional tasks are unavailable in Cloud Tasks
- ``_retry_options``: Retry options for tasks are unavailable in Cloud Tasks

Unimplemented features
----------------------

- Support for tasks larger than 10 KB
