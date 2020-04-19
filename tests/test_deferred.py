from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import functools
import pickle

import google.auth
import pytest
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

from cloud_tasks_deferred import deferred

PROJECT_ID = 'my-project'
GAE_SERVICE = 'my-service'
GAE_VERSION = 'my-version'
LOCATION_ID = 'my-location'
QUEUE_ID = 'my-queue'


@pytest.fixture
def mock_create_task(mocker):
    class CredentialsStub(google.auth.credentials.Credentials):
        def refresh(self, request):
            self.token = 'stub'

    mock_create_task = mocker.patch(
        'google.cloud.tasks_v2.CloudTasksClient.create_task',
        return_value=mocker.sentinel.Task,
        autospec=True,
    )
    mocker.patch(
        'google.cloud.tasks_v2.CloudTasksClient',
        return_value=tasks_v2.CloudTasksClient(credentials=CredentialsStub()),
    )
    return mock_create_task


@pytest.fixture
def mock_project_id(monkeypatch):
    monkeypatch.setenv(str('GOOGLE_CLOUD_PROJECT'), str(PROJECT_ID))


@pytest.fixture
def mock_gae_service(monkeypatch):
    monkeypatch.setenv(str('GAE_SERVICE'), str(GAE_SERVICE))


@pytest.fixture
def mock_gae_version(monkeypatch):
    monkeypatch.setenv(str('GAE_VERSION'), str(GAE_VERSION))


@pytest.fixture
def mock_location_id(monkeypatch):
    monkeypatch.setenv(str('QUEUE_LOCATION'), str(LOCATION_ID))


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


def test_defer(mocker):
    mock_add = mocker.patch(
        'cloud_tasks_deferred.deferred._Task.add', autospec=True
    )

    result = deferred.defer(
        top_level_function,
        2,
        b=3,
        _countdown=10,
        _headers={'X-API-Key': 'testing'},
        _name='task-id',
        _target='worker',
        _url='/deferred',
        _queue=QUEUE_ID,
    )

    assert result == mock_add.return_value
    (task, queue), kwargs = mock_add.call_args
    assert deferred.run(task.payload) == 5
    assert task._task_id == 'task-id'
    assert task._app_engine_routing == {'service': 'worker'}
    assert task._relative_uri == '/deferred'
    assert task._headers == {'X-API-Key': 'testing'}
    assert task._schedule_time is not None
    assert queue == QUEUE_ID
    assert kwargs == {}


def test_defer_default(mocker):
    mock_task_class = mocker.patch('cloud_tasks_deferred.deferred._Task')

    deferred.defer(top_level_function)

    _, kwargs = mock_task_class.call_args
    assert kwargs == {
        'countdown': None,
        'eta': None,
        'headers': None,
        'name': None,
        'target': None,
        'url': deferred._DEFAULT_URL,
    }


def test__task():
    task = deferred._Task(
        b'data',
        eta=datetime.datetime(2020, 1, 1),
        headers={'X-API-Key': 'testing'},
        name='task-id',
        target='worker',
        url='/deferred',
    )
    assert task.payload == b'data'
    assert task._task_id == 'task-id'
    assert task._app_engine_routing == {'service': 'worker'}
    assert task._relative_uri == '/deferred'
    assert task._headers == {'X-API-Key': 'testing'}
    assert task._schedule_time == datetime.datetime(2020, 1, 1)


@pytest.mark.usefixtures('mock_gae_service', 'mock_gae_version')
def test__task_default():
    task = deferred._Task()
    assert task.payload is None
    assert task._task_id is None
    assert task._app_engine_routing == deferred._get_app_engine_routing()
    assert task._relative_uri == deferred._DEFAULT_URL
    assert task._headers is None
    assert task._schedule_time is None


@pytest.mark.usefixtures('mock_project_id', 'mock_location_id')
def test__task_add(mock_create_task):
    task = deferred._Task(target='worker')
    result = task.add(QUEUE_ID)

    assert result is mock_create_task.return_value
    (client, parent, task_dict), kwargs = mock_create_task.call_args
    assert parent == client.queue_path(PROJECT_ID, LOCATION_ID, QUEUE_ID)
    assert task_dict == {
        'app_engine_http_request': {
            'http_method': 'POST',
            'app_engine_routing': {'service': 'worker'},
            'relative_uri': deferred._DEFAULT_URL,
            'headers': None,
            'body': None,
        },
    }
    assert kwargs == {}


def test__task__create_task_dict():
    task = deferred._Task(b'data', target='worker')
    task._task_id = 'task-id'
    task._relative_uri = '/deferred'
    task._headers = {'X-API-Key': 'testing'}
    task._schedule_time = datetime.datetime(2020, 1, 1)

    result = task._create_task_dict('queue-path')

    assert result == {
        'app_engine_http_request': {
            'http_method': 'POST',
            'app_engine_routing': {'service': 'worker'},
            'relative_uri': '/deferred',
            'headers': {'X-API-Key': 'testing'},
            'body': b'data',
        },
        'schedule_time': deferred._to_timestamp(datetime.datetime(2020, 1, 1)),
        'name': 'queue-path/tasks/task-id',
    }


@pytest.mark.parametrize(
    'target,expected',
    [
        ('s', {'service': 's'}),
        ('v.s', {'service': 's', 'version': 'v'}),
        ('i.v.s', {'service': 's', 'version': 'v', 'instance': 'i'}),
    ],
)
def test__get_app_engine_routing(target, expected):
    assert deferred._get_app_engine_routing(target) == expected


@pytest.mark.parametrize(
    'gae_service,gae_version,expected',
    [
        ('s', 'v', {'service': 's', 'version': 'v'}),
        ('s', None, {'service': 's'}),
    ],
)
def test__get_app_engine_routing_default(
    mocker, gae_service, gae_version, expected
):
    mocker.patch(
        'cloud_tasks_deferred.deferred._get_gae_service',
        return_value=gae_service,
    )
    mocker.patch(
        'cloud_tasks_deferred.deferred._get_gae_version',
        return_value=gae_version,
    )

    assert deferred._get_app_engine_routing() == expected


@pytest.mark.parametrize(
    'countdown,eta,expected',
    [
        (None, None, None),
        (None, datetime.datetime(2020, 1, 1), datetime.datetime(2020, 1, 1)),
        (60 * 60 * 24, None, datetime.datetime(2020, 1, 2)),
    ],
)
def test__get_schedule_time(countdown, eta, expected):
    now = functools.partial(datetime.datetime, 2020, 1, 1)
    assert deferred._get_schedule_time(countdown, eta, now=now) == expected


def test__get_schedule_time_error():
    with pytest.raises(deferred.InvalidTaskError):
        assert deferred._get_schedule_time(1, datetime.datetime.utcnow())


def test__to_timestamp():
    dt = datetime.datetime(2020, 1, 1)
    ts = deferred._to_timestamp(dt)
    assert timestamp_pb2.Timestamp.ToDatetime(ts) == dt


@pytest.mark.usefixtures('mock_project_id')
def test__get_project_id():
    assert deferred._get_project_id() == PROJECT_ID


@pytest.mark.usefixtures('mock_location_id')
def test__get_location_id():
    assert deferred._get_location_id() == LOCATION_ID
