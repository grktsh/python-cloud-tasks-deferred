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
"""A module that handles deferred execution of callables via Cloud Tasks.

Tasks consist of a callable and arguments to pass to it. The callable and its
arguments are serialized and put on the Cloud Tasks queue, which deserializes
and executes them. The following callables can be used as tasks:

1) Functions defined in the top level of a module
2) Classes defined in the top level of a module
3) Instances of classes in (2) that implement __call__
4) Instance methods of objects of classes in (2)
5) Class methods of classes in (2)
6) Built-in functions
7) Built-in methods

The following callables can NOT be used as tasks:
1) Nested functions or closures
2) Nested classes or objects of them
3) Lambda functions
4) Static methods

The arguments to the callable, and the object (in the case of method or object
calls) must all be pickleable.

If you want your tasks to execute reliably, don't use mutable global variables;
they are not serialized with the task and may not be the same when your task
executes as they were when it was enqueued (in fact, they will almost certainly
be different).

If your app relies on manipulating the import path, make sure that the function
you are deferring is defined in a module that can be found without import path
manipulation. Alternately, you can include wsgi.application in your own
webapp application instead of using the easy-install method detailed below.

In order for tasks to be processed, you need to set up the handler. Add the
following to your app.yaml handlers section:

handlers:
- url: /_tasks/deferred
  script: cloud_tasks_deferred.wsgi.application
  login: admin

By default, the deferred module uses the URL above, and the default queue.

Example usage::

    def do_something_later(key, amount):
        entity = MyModel.get(key)
        entity.total += amount
        entity.put()

    # Use default URL and queue name, no task name, execute ASAP.
    deferred.defer(do_something_later, my_key, 20)

    # Providing non-default task queue arguments
    deferred.defer(do_something_later, my_key, 20, _queue="foo", _countdown=60)
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import logging
import os
import pickle
import types

from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

_DEFAULT_LOG_LEVEL = logging.INFO
_DEFAULT_URL = '/_tasks/deferred'
_DEFAULT_QUEUE = 'default'


class Error(Exception):
    """Base class for exceptions in this module."""


class PermanentTaskFailure(Error):
    """Indicates that a task failed, and will never succeed."""


class SingularTaskFailure(Error):
    """Indicates that a task failed once."""


class InvalidTaskError(Error):
    """The parameters, headers, or method of the task is invalid."""


def set_log_level(log_level):  # pragma: no cover
    """Sets the log level deferred will log to in normal circumstances.

    Args:
        log_level: one of logging log levels, e.g. logging.DEBUG,
        logging.INFO, etc.
    """
    global _DEFAULT_LOG_LEVEL
    _DEFAULT_LOG_LEVEL = log_level


def run(data):
    """Unpickle and execute a task.

    Args:
        data: A pickled tuple of (function, args, kwargs) to execute.
    Returns:
        The return value of the function invocation.
    """
    try:
        func, args, kwargs = pickle.loads(data)
    except Exception as e:
        raise PermanentTaskFailure(e)
    else:
        return func(*args, **kwargs)


def _invoke_member(obj, membername, *args, **kwargs):
    """Retrieve a member of an object, then call it with the provided arguments.

    Args:
        obj: The object to operate on.
        membername: The name of the member to retrieve from ojb.
        args: Positional arguments to pass to the method.
        kwargs: Keyword arguments to pass to the method.
    Returns:
        The return value of the method invocation.
    """
    return getattr(obj, membername)(*args, **kwargs)


def _curry_callable(obj, *args, **kwargs):
    """Take a callable and arguments and return a task queue tuple.

    The returned tuple consists of (callable, args, kwargs), and can be pickled
    and unpickled safely.

    Args:
        obj: The callable to curry. See the module docstring for restrictions.
        args: Positional arguments to call the callable with.
        kwargs: Keyword arguments to call the callable with.
    Returns:
        A tuple consisting of (callable, args, kwargs) that can be evaluated by
        run() with equivalent effect of executing the function directly.
    Raises:
        ValueError: If the passed in object is not of a valid callable type.
    """
    if isinstance(obj, types.MethodType):
        return (
            _invoke_member,
            (obj.__self__, obj.__func__.__name__) + args,
            kwargs,
        )
    if isinstance(obj, (types.BuiltinFunctionType, types.BuiltinMethodType)):
        # https://stackoverflow.com/a/42909466
        is_builtin_function = (
            isinstance(obj.__self__, types.ModuleType)  # Python 3
            or obj.__self__ is None  # Python 2
        )
        if is_builtin_function:
            return obj, args, kwargs
        else:
            return (
                _invoke_member,
                (obj.__self__, obj.__name__) + args,
                kwargs,
            )
    if isinstance(obj, object) and hasattr(obj, '__call__'):
        return obj, args, kwargs
    raise ValueError('obj must be callable')


def _serialize(obj, *args, **kwargs):
    """Serializes a callable into a format recognized by the deferred executor.

    Args:
        obj: The callable to serialize. See module docstring for restrictions.
        args: Positional arguments to call the callable with.
        kwargs: Keyword arguments to call the callable with.
    Returns:
        A serialized representation of the callable.
    """
    curried = _curry_callable(obj, *args, **kwargs)
    return pickle.dumps(curried, protocol=pickle.HIGHEST_PROTOCOL)


def defer(obj, *args, **kwargs):
    """Defer a callable for execution later.

    The default deferred URL of /_tasks/deferred will be used unless an
    alternate URL is explicitly specified. If you want to use the default URL
    for a queue, specify _url=None. If you specify a different URL, you will
    need to install the handler on that URL (see the module docstring for
    details).

    Args:
        obj: The callable to execute. See module docstring for restrictions.
            _countdown, _eta, _headers, _name, _target, _url,
            _queue: Passed through to the task queue - see the
            task queue documentation for details.
        args: Positional arguments to call the callable with.
        kwargs: Any other keyword arguments are passed through to the callable.
    Returns:
        A deferred._Task object which represents an enqueued callable.
    """
    task_kwargs_defaults = {
        'countdown': None,
        'eta': None,
        'headers': None,
        'name': None,
        'target': None,
        'url': _DEFAULT_URL,
    }
    task_kwargs = {
        k: kwargs.pop('_' + k, v) for k, v in task_kwargs_defaults.items()
    }
    queue = kwargs.pop('_queue', _DEFAULT_QUEUE)

    pickled = _serialize(obj, *args, **kwargs)
    task = _Task(pickled, **task_kwargs)
    return task.add(queue)


class _Task(object):
    def __init__(
        self,
        payload=None,
        countdown=None,
        eta=None,
        headers=None,
        name=None,
        target=None,
        url=None,
    ):
        self.payload = payload
        self._task_id = name
        self._app_engine_routing = _get_app_engine_routing(target)
        self._relative_uri = url or _DEFAULT_URL
        self._headers = headers
        self._schedule_time = _get_schedule_time(countdown, eta)

    def add(self, queue_name=_DEFAULT_QUEUE):
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(
            _get_project_id(), _get_location_id(), queue_name
        )
        task_dict = self._create_task_dict(parent)
        return client.create_task(parent, task_dict)

    def _create_task_dict(self, parent):
        result = {
            'app_engine_http_request': {
                'http_method': 'POST',
                'app_engine_routing': self._app_engine_routing,
                'relative_uri': self._relative_uri,
                'headers': self._headers,
                'body': self.payload,
            },
        }
        if self._schedule_time is not None:
            result['schedule_time'] = _to_timestamp(self._schedule_time)
        if self._task_id is not None:
            result['name'] = '{parent}/tasks/{task_id}'.format(
                parent=parent, task_id=self._task_id
            )
        return result


def _get_app_engine_routing(target=None):
    if target is None:
        target = _get_default_target()

    return dict(
        zip(('service', 'version', 'instance'), reversed(target.split('.')))
    )


def _get_default_target():
    gae_service = _get_gae_service()
    gae_version = _get_gae_version()
    if gae_version is None:
        return gae_service
    else:
        return gae_version + '.' + gae_service


def _get_schedule_time(countdown=None, eta=None, now=datetime.datetime.utcnow):
    if countdown is not None and eta is not None:
        raise InvalidTaskError('Cannot use a countdown and ETA together')

    if countdown is None:
        return eta
    else:
        return now() + datetime.timedelta(seconds=countdown)


def _to_timestamp(dt):
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(dt)
    return timestamp


def _get_project_id():
    try:
        return os.environ['GOOGLE_CLOUD_PROJECT']
    except KeyError:  # pragma: no cover
        from google.appengine.api import app_identity

        return app_identity.get_application_id()


def _get_gae_service():
    try:
        return os.environ['GAE_SERVICE']
    except KeyError:  # pragma: no cover
        from google.appengine.api import modules

        return modules.get_current_module_name()


def _get_gae_version():
    try:
        return os.environ['GAE_VERSION']
    except KeyError:  # pragma: no cover
        from google.appengine.api import modules

        return modules.get_current_version_name()


def _get_location_id():
    return os.environ['QUEUE_LOCATION']
