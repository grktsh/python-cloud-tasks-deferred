#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import pickle
import types

from google.cloud import tasks_v2

_TASKQUEUE_HEADERS = {'Content-Type': 'application/octet-stream'}
_DEFAULT_URL = '/api/tasks/deferred'
_DEFAULT_QUEUE = 'default'


class Error(Exception):
    """Base class for exceptions in this module."""


class PermanentTaskFailure(Error):
    """Indicates that a task failed, and will never succeed."""


def run(data):
    """Unpickle and execute a task."""
    try:
        func, args, kwargs = pickle.loads(data)
    except Exception as e:
        raise PermanentTaskFailure(e)
    else:
        return func(*args, **kwargs)


def _curry_callable(obj, *args, **kwargs):
    """Take a callable and arguments and return a task queue tuple."""
    if isinstance(obj, (types.FunctionType, types.BuiltinFunctionType)):
        return obj, args, kwargs
    raise NotImplementedError


def _serialize(obj, *args, **kwargs):
    curried = _curry_callable(obj, *args, **kwargs)
    return pickle.dumps(curried, protocol=pickle.HIGHEST_PROTOCOL)


def defer(obj, *args, **kwargs):
    """Defer a callable for execution later."""
    if '_transactional' in kwargs:
        raise NotImplementedError("'_transactional' is not supported.")

    for x in (
        '_countdown',
        '_eta',
        '_name',
        '_target',
        '_retry_options',
    ):
        if x in kwargs:
            raise NotImplementedError('{!r} is not supported yet.'.format(x))

    url = kwargs.pop('_url', _DEFAULT_URL)
    headers = dict(_TASKQUEUE_HEADERS)
    headers.update(kwargs.pop('_headers', {}))
    queue = kwargs.pop('_queue', _DEFAULT_QUEUE)

    pickled = _serialize(obj, *args, **kwargs)
    task = _Task(body=pickled, relative_uri=url, headers=headers)
    return _create_task(task, queue)


def _create_task(task, queue):
    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(
        _google_cloud_project(), _queue_location(), queue
    )
    return client.create_task(parent, task.to_dict())


class _Task(object):
    def __init__(self, body=None, relative_uri=None, headers=None):
        self._body = body
        self._relative_uri = relative_uri or _DEFAULT_URL
        self._headers = headers or _TASKQUEUE_HEADERS

    def to_dict(self):
        return {
            'app_engine_http_request': {
                'http_method': 'POST',
                'app_engine_routing': {'version': _gae_version()},
                'relative_uri': self._relative_uri,
                'headers': self._headers,
                'body': self._body,
            }
        }

    @property
    def payload(self):
        return self._body


def _google_cloud_project():
    try:
        return os.environ['GOOGLE_CLOUD_PROJECT']
    except KeyError:  # pragma: no cover
        from google.appengine.api import app_identity

        return app_identity.get_application_id()


def _queue_location():
    try:
        return os.environ['QUEUE_LOCATION']
    except KeyError:
        # TODO: Use admin API
        return 'asia-northeast1'


def _gae_version():
    try:
        return os.environ['GAE_VERSION']
    except KeyError:  # pragma: no cover
        from google.appengine.api import modules

        return modules.get_current_version_name()
