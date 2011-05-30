import os
import sys

import fudge
from fudge.inspector import arg
from nose.tools import eq_, raises

import sync


base_env = dict(CHIRP_S3_BUCKET='', CHIRP_ARCHIVES_DIR='/somewhere/archives')


def call_sync(**kw):
    environ = base_env.copy()
    environ.update(os.environ)
    environ.update(kw.pop('environ', {}))
    expected_code = kw.pop('returncode', 0)
    saved_stderr = sys.stderr
    sys.stderr = sys.stdout
    try:
        return sync.main(argv=['sync.py'] + kw.pop('args', []),
                         environ=environ, **kw)
    except SystemExit, exc:
        eq_(exc.code, expected_code)
    finally:
        sys.stderr = saved_stderr


@raises(EnvironmentError)
@fudge.patch('os.path.exists')
def test_no_s3cmd(exists):
    exists.expects_call().returns(False)
    call_sync()


@fudge.patch('subprocess.call')
def test_successful_sync(call):
    env = dict(CHIRP_S3_BUCKET='bucket', CHIRP_ARCHIVES_DIR='/this/archives')
    call.expects_call().returns(0).with_args([sync.which('s3cmd'),
                                              'sync', '--skip-existing',
                                              '/this/archives',
                                              's3://bucket/archives'])
    call_sync(environ=env)


@fudge.patch('subprocess.call')
def test_full_sync(call):
    env = dict(CHIRP_S3_BUCKET='bucket', CHIRP_ARCHIVES_DIR='/this/archives')
    call.expects_call().returns(0).with_args([sync.which('s3cmd'),
                                              'sync', '/this/archives',
                                              's3://bucket/archives'])
    call_sync(args=['--full'], environ=env)


def test_wrong_directory_name():
    env = dict(CHIRP_S3_BUCKET='bucket', CHIRP_ARCHIVES_DIR='/archives-tmp')
    call_sync(environ=env, returncode=2)


@fudge.patch('subprocess.call')
def test_non_zero_return_code_triggers_retry(call):
    call.expects_call().returns(1).times_called(5)
    call_sync(max_tries=5, returncode=1)
