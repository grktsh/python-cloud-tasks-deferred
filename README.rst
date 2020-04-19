cloud-tasks-deferred
====================

.. image:: https://img.shields.io/pypi/v/cloud-tasks-deferred.svg
   :alt: PyPI
   :target: https://pypi.org/project/cloud-tasks-deferred

.. image:: https://img.shields.io/travis/grktsh/python-cloud-tasks-deferred/master.svg
   :alt: Travis
   :target: https://travis-ci.org/grktsh/python-cloud-tasks-deferred

.. image:: https://img.shields.io/codecov/c/github/grktsh/python-cloud-tasks-deferred/master.svg
   :alt: Codecov
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
