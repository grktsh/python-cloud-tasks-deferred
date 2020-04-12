from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import pickle

import pytest

from cloud_tasks_deferred import deferred

PROJECT_ID = 'my-project'
GAE_SERVICE = 'my-service'
GAE_VERSION = 'my-version'
LOCATION = 'my-location'
QUEUE = 'my-queue'


def function(x, y=42):
    pass


@pytest.fixture
def mock_client_class(mocker):
    return mocker.patch('google.cloud.tasks_v2.CloudTasksClient')


@pytest.fixture
def mock__create_task(mocker):
    return mocker.patch('cloud_tasks_deferred.deferred._create_task')


@pytest.fixture
def mock__google_cloud_project(mocker):
    return mocker.patch(
        'cloud_tasks_deferred.deferred._google_cloud_project',
        return_value=PROJECT_ID,
    )


@pytest.fixture
def mock__queue_location(mocker):
    return mocker.patch(
        'cloud_tasks_deferred.deferred._queue_location', return_value=LOCATION
    )


@pytest.fixture
def mock__gae_version(mocker):
    return mocker.patch(
        'cloud_tasks_deferred.deferred._gae_version', return_value=GAE_VERSION
    )


# 1) Functions defined in the top level of a module
def top_level_function(a, b):
    return a + b


# 2) Classes defined in the top level of a module
class TopLevelClass:
    def __init__(self, a=0, b=0):
        self.c = a + b

    # 4) Instance methods of objects of classes in (2)
    def instance_method(self, a, b):
        return a + b

    # 3) Instances of classes in (2) that implement __call__
    __call__ = instance_method

    # 5) Class methods of classes in (2)
    @classmethod
    def class_method(cls, a, b):
        return a + b


def test__curry_callable_1_top_level_function():
    curried = deferred._curry_callable(top_level_function, 2, b=3)
    assert curried == (top_level_function, (2,), dict(b=3))

    result = deferred.run(pickle.dumps(curried))
    assert result == 5


def test__curry_callable_2_top_level_class():
    curried = deferred._curry_callable(TopLevelClass, 2, b=3)
    assert curried == (TopLevelClass, (2,), dict(b=3))

    result = deferred.run(pickle.dumps(curried))
    assert isinstance(result, TopLevelClass)
    assert result.c == 5


def test__curry_callable_3___call__():
    obj = TopLevelClass()
    curried = deferred._curry_callable(obj, 2, b=3)
    assert curried == (obj, (2,), dict(b=3))

    result = deferred.run(pickle.dumps(curried))
    assert result == 5


def test__curry_callable_4_instance_method():
    obj = TopLevelClass()
    curried = deferred._curry_callable(obj.instance_method, 2, b=3)
    assert curried == (
        deferred._invoke_member,
        (obj, 'instance_method', 2),
        dict(b=3),
    )

    result = deferred.run(pickle.dumps(curried))
    assert result == 5


def test__curry_callable_5_class_method():
    curried = deferred._curry_callable(TopLevelClass.class_method, 2, b=3)
    assert curried == (
        deferred._invoke_member,
        (TopLevelClass, 'class_method', 2,),
        dict(b=3),
    )

    result = deferred.run(pickle.dumps(curried))
    assert result == 5


def test__curry_callable_6_builtin_function():
    curried = deferred._curry_callable(len, [2, 3, 5])
    assert curried == (len, ([2, 3, 5],), {})

    result = deferred.run(pickle.dumps(curried))
    assert result == 3


def test__curry_callable_7_builtin_method():
    obj = []
    curried = deferred._curry_callable(obj.append, 42)
    assert curried == (deferred._invoke_member, (obj, 'append', 42), {})

    data = pickle.dumps(curried)
    func, args, kwargs = pickle.loads(data)
    func(*args, **kwargs)
    assert args[0] == [42]
    assert obj == []


def test__curry_callable_not_callable():
    with pytest.raises(ValueError, match='obj must be callable'):
        deferred._curry_callable('not callable')


@pytest.mark.parametrize(
    'option',
    [
        '_countdown',
        '_eta',
        '_name',
        '_target',
        '_retry_options',
        '_transactional',
    ],
)
def test_defer_not_implemented_error(option):
    with pytest.raises(NotImplementedError):
        deferred.defer(function, **{option: True})


def test_defer(mock__create_task):
    args = ('x',)
    kwargs = {'y': 42}
    relative_uri = '/path'
    headers = {'X-Env': 'testing'}

    result = deferred.defer(
        function,
        *args,
        _url=relative_uri,
        _headers=headers,
        _queue=QUEUE,
        **kwargs
    )

    (task, queue), _ = mock__create_task.call_args
    assert pickle.loads(task._body) == (function, args, kwargs)
    assert task._relative_uri == relative_uri
    assert task._headers == dict(deferred._TASKQUEUE_HEADERS, **headers)
    assert result == mock__create_task.return_value


@pytest.mark.usefixtures(
    'mock__google_cloud_project', 'mock__queue_location', 'mock__gae_version'
)
def test__create_task(mock_client_class):
    task = deferred._Task()
    result = deferred._create_task(task, QUEUE)

    mock_client = mock_client_class.return_value
    mock_client.queue_path.assert_called_once_with(
        PROJECT_ID, LOCATION, QUEUE,
    )
    mock_client.create_task.assert_called_once_with(
        mock_client.queue_path.return_value, task.to_dict()
    )
    assert result == mock_client.create_task.return_value


@pytest.mark.usefixtures('mock__gae_version')
def test__task():
    body = b'body'
    relative_uri = '/path'
    headers = {'X-Env': 'testing'}
    task = deferred._Task(
        body=body, relative_uri=relative_uri, headers=headers
    )
    assert task._body == body
    assert task._relative_uri == relative_uri
    assert task._headers == headers
    assert task.payload == body

    assert task.to_dict() == {
        'app_engine_http_request': {
            'http_method': 'POST',
            'app_engine_routing': {'version': GAE_VERSION},
            'relative_uri': relative_uri,
            'headers': headers,
            'body': body,
        }
    }


def test__task_default():
    task = deferred._Task()
    assert task._body is None
    assert task._relative_uri == deferred._DEFAULT_URL
    assert task._headers == deferred._TASKQUEUE_HEADERS


def test__google_cloud_project(mocker):
    mocker.patch.dict('os.environ', {'GOOGLE_CLOUD_PROJECT': PROJECT_ID})
    assert deferred._google_cloud_project() == PROJECT_ID


def test__queue_location(mocker):
    mocker.patch.dict('os.environ', {'QUEUE_LOCATION': LOCATION})
    assert deferred._queue_location() == 'my-location'


def test__queue_location_without_environ():
    assert deferred._queue_location() == 'asia-northeast1'


def test__gae_version(mocker):
    mocker.patch.dict('os.environ', {'GAE_VERSION': GAE_VERSION})
    assert deferred._gae_version() == GAE_VERSION
