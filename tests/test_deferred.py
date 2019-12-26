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


class Curried(object):
    def instance_method(self):
        pass


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


def test__curry_callable_not_implemented_error():
    with pytest.raises(NotImplementedError):
        deferred._curry_callable(Curried().instance_method)


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
