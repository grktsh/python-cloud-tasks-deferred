from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import functools
import pickle

import falcon
import pytest
from falcon import testing

from cloud_tasks_deferred import deferred
from cloud_tasks_deferred import wsgi


@pytest.fixture
def client():
    return testing.TestClient(wsgi.application)


@pytest.fixture
def simulate_post(client):
    return functools.partial(
        client.simulate_post,
        headers={
            'Content-Type': 'application/octet-stream',
            'X-AppEngine-TaskName': 'testing',
        },
    )


def test_application_method_not_allowed(client):
    response = client.simulate_get()
    assert response.status == falcon.HTTP_METHOD_NOT_ALLOWED


def test_application_unsupported_media_type(simulate_post):
    response = simulate_post(json='')
    assert response.status == falcon.HTTP_UNSUPPORTED_MEDIA_TYPE


def test_application_without_task_queue_header(simulate_post):
    response = simulate_post(
        headers={'Content-Type': 'application/octet-stream'}
    )
    assert response.status == falcon.HTTP_FORBIDDEN


def test_application(simulate_post):
    body = pickle.dumps((bool, (), {}))
    response = simulate_post(body=body)
    assert response.status == falcon.HTTP_NO_CONTENT


def test_application_singular_task_failure(simulate_post, mocker):
    mocker.patch(
        'cloud_tasks_deferred.deferred.run',
        side_effect=deferred.SingularTaskFailure,
    )
    body = pickle.dumps((bool, (), {}))
    response = simulate_post(body=body)
    assert response.status == '408 Request Timeout'


def test_application_permanent_task_failure(simulate_post):
    body = pickle.dumps('can not deserialize')
    response = simulate_post(body=body)
    assert response.status == falcon.HTTP_NO_CONTENT


def test_application_error(simulate_post):
    body = pickle.dumps((max, (), {}))
    response = simulate_post(body=body)
    assert response.status == falcon.HTTP_INTERNAL_SERVER_ERROR
