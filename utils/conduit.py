import os
import re
import signal
import socket
import subprocess
import time
from subprocess import DEVNULL, PIPE

import psycopg2


def conduit_connect(service, space, conduit_process_filepath=None):
    if conduit_process_filepath is None:
        conduit_process_filepath = '/tmp/cf_conduit/{}_{}.txt'.format(service, space)
    directory = os.path.split(conduit_process_filepath)[0]
    if not os.path.exists(directory):
        os.makedirs(directory)
    cf_login(space)
    cf_set_target(space)
    start_conduit(conduit_process_filepath, service, space)
    uri = get_conduit_uri(conduit_process_filepath)
    try:
        connection = connect_to_database(uri)
    except Exception:
        kill_conduit(service, space)
        start_conduit(conduit_process_filepath, service, space)
        uri = get_conduit_uri(conduit_process_filepath)
        connection = connect_to_database(uri)
    return connection


def connect_to_database(uri):
    print('\033[31mconnecting to database\033[0m')
    try:
        connection = psycopg2.connect(uri)
    except Exception() as e:
        print('\033[31mfailed to connection\033[0m')
        raise e
    print('\033[32mconnected\033[0m')
    return connection


def get_conduit_uri(conduit_process_filepath):
    retries = 10
    while retries > 0:
        try:
            grep = subprocess.run(
                ['grep', 'uri', conduit_process_filepath], stdout=PIPE
            )
            output = grep.stdout.decode('utf-8')
            uri = re.search('(?<= uri: ).*', output).group(0)
            if retries < 10:
                print()
                print('uri: {}'.format(uri))
            break
        except AttributeError:
            if retries == 10:
                print('waiting for conduit process to initialise')
        print('.', end='', flush=True),
        time.sleep(1)
        retries -= 1

    if retries == 0:
        with open(conduit_process_filepath) as f:
            raise Exception('did not find a conduit process, \n{}'.format(f.read()))

    return uri


def kill_conduit(service, space):
    pgrep = subprocess.run(
        ['pgrep', '-f', 'conduit {} -s {}'.format(service, space)], stdout=PIPE
    )
    processes = pgrep.stdout.decode('utf-8').strip()
    processes = [int(pid) for pid in processes.split('\n')] if processes != '' else []

    print('killing conduit processes: {}'.format(processes))
    for pid in processes:
        print('killing {}: '.format(pid), end='', flush=True)
        os.kill(pid, signal.SIGKILL)
        print('OK')


def cf_login(space):
    orgs = subprocess.run(['cf', 'orgs'], stdout=DEVNULL, stderr=DEVNULL)
    if orgs.returncode == 0:
        print('logged in')
    else:
        print('not logged in')
        print('logging in')
        output = subprocess.run(
            [
                "cf",
                "login",
                "-a",
                os.environ['CF_ENDPOINT'],
                '-u',
                os.environ['CF_USER'],
                '-p',
                os.environ['CF_PASSWORD'],
                '-s',
                space,
            ]
        )  # , stdout=PIPE)
        if output.returncode != 0:
            raise Exception('failed to log in')


def cf_set_target(space):
    proc = subprocess.run(['cf', 'target', '-s', space], stdout=PIPE)
    assert proc.returncode == 0, 'Failed to set space'


def start_conduit(conduit_process_filepath, service, space, port=7080):
    retries = 100
    while (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect_ex(
            ('127.0.0.1', port)
        )
        == 0
    ):
        if retries == 0:
            raise Exception('Found no open ports')
        port += 1
        retries -= 1

    pgrep = subprocess.run(
        ['pgrep', '-f', 'conduit {} -s {}'.format(service, space)], stdout=PIPE
    )
    processes = pgrep.stdout.decode('utf-8').strip()
    processes = [int(pid) for pid in processes.split('\n')] if processes != '' else []

    if len(processes) == 2 and os.path.exists(conduit_process_filepath):
        print('conduit is already running')
    else:
        for pid in processes:
            os.kill(pid, signal.SIGKILL)
            print('conduit is not running')
            print('starting conduit process')
        with open(conduit_process_filepath, "wb") as err:
            subprocess.Popen(
                ['cf', 'conduit', service, '-s', space, '-p', str(port)], stderr=err
            )

    pgrep = subprocess.run(
        ['pgrep', '-f', 'conduit {} -s {}'.format(service, space)], stdout=PIPE
    )
    processes = [int(pid) for pid in pgrep.stdout.decode('utf-8').strip().split('\n')]
    print('processes:', processes)


if __name__ == '__main__':

    space = 'datahub-dev'
    service = 'datahub-dev-db'
    connection = conduit_connect(service, space)
    print('connection:', connection)
