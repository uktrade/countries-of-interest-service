import json

import click
import requests
from flask.cli import with_appcontext
from mohawk import Sender


GET = 'GET'
POST = 'POST'


@click.command('hawk_api_request')
@with_appcontext
@click.option(
    '--url',
    type=str,
    help='Url endpoint to query',
)
@click.option('--client_id', type=str, help="The hawk id for the endpoint")
@click.option(
    '--client_key',
    type=str,
    help="The hawk secret key for the endpoint",
)
@click.option(
    '--method',
    type=click.Choice([GET, POST]),
    help='HTTP method',
)
@click.option(
    '--post_data',
    type=str,
    help="Post data in json format",
)
@click.option(
    '--query_params',
    type=str,
    help="Query parameters to be added to url",
)
def hawk_api_request(url, client_id, client_key, method, post_data, query_params):
    """
    Make an API Request to a Hawk protected endpoint
    """
    if not any([url, client_id, client_key, method]):
        print_help()
    elif method == GET:
        process_get_request(url, client_id, client_key, query_params)
    else:
        if is_post_data_valid(post_data):
            process_post_request(url, client_id, client_key, post_data, query_params)
        else:
            print_help()


def print_help():
    ctx = click.get_current_context()
    click.echo(ctx.get_help())


def is_post_data_valid(post_data):
    if not post_data:
        click.echo('\nPost data is required to make a POST request\n')
        return False
    try:
        json.loads(post_data)
    except json.decoder.JSONDecodeError:
        click.echo('\nInvalid json record for post data\n')
        return False
    return True


def append_query_params_to_url(url, query_params):
    if not query_params:
        return url

    prefix = ''
    if '?' not in url:
        prefix = '?'
    return f'{url}{prefix}{query_params}'


def process_get_request(url, client_id, client_key, query):
    url = append_query_params_to_url(url, query)
    sender = get_sender(url, GET, client_id, client_key, '')
    response = requests.get(url, headers={'Authorization': sender.request_header})
    display_response(response)


def process_post_request(url, client_id, client_key, post_data, query_params):
    url = append_query_params_to_url(url, query_params)
    sender = get_sender(url, POST, client_id, client_key, post_data)
    response = requests.post(
        url,
        headers={'Authorization': sender.request_header},
        json=json.loads(post_data),
    )
    display_response(response)


def get_sender(url, method, client_id, client_key, query):
    content = query
    content_type = 'application/json'

    if not query:
        content = ''
        content_type = ''

    return Sender(
        credentials={'id': client_id, 'key': client_key, 'algorithm': 'sha256'},
        url=url,
        method=method,
        content=content,
        content_type=content_type,
    )


def display_response(response):
    click.echo(f'REQUEST RESPONSE {response.status_code}\n')
    if response.status_code == requests.codes.ok:
        json_response = response.json()
        if json_response:
            click.echo(json_response)
        else:
            click.echo(response.text)
    else:
        click.echo(response.reason)
        if response.text:
            click.echo(response.text)
