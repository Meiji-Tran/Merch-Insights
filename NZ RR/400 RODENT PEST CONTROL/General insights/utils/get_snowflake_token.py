#!/usr/bin/env python3
from getpass import getpass

import requests
import os
import argparse
import json
import sys
import urllib.parse


def get_okta_reponse(
    username: str,
    password: str,
    scope: str,
    client_id: str,
    client_secret: str
):
    url = os.environ.get("SNOWFLAKE_OKTA_TOKEN_URL")

    # certain roles scope=SESSION:ROLE-ANY. For others, scope=session:role:<sf_role_name>
    payload = f"username={username}&password={password}&grant_type=password&scope={scope}&client_id={client_id}&client_secret={client_secret}"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()


def get_snowflake_secrets():
    client_id = os.environ.get("SNOWFLAKE_OKTA_CLIENT_ID")
    if not client_id:
        raise EnvironmentError("SNOWFLAKE_OKTA_CLIENT_ID environment variable not set")
    client_secret = os.environ.get("SNOWFLAKE_OKTA_CLIENT_SECRET")
    if not client_secret:
        raise EnvironmentError(
            "SNOWFLAKE_OKTA_CLIENT_SECRET environment variable not set"
        )
    return client_id, client_secret


def get_snowflake_user_credentials(
    forced_username: str = None,
):
    if forced_username:
        username = forced_username
    else:
        username = os.environ.get("SNOWFLAKE_TEAM_MEMBER_ID")
        if not username:
            sys.stderr.write("Enter TM ID: ")
            username = input()
    password = getpass("Enter Password: ", stream=sys.stderr)

    return username, password


def get_role_from_environment():
    return os.environ.get("SNOWFLAKE_OKTA_ROLE")


def main():
    parser = argparse.ArgumentParser(
        "Snowflake Token Retrieval", "Get a user snowflake token from Snowflake"
    )
    parser.add_argument("-r", "--role", help="Snowflake Role", default=None)
    parser.add_argument("-u", "--username", help="Okta Username", default=None)
    parser.add_argument("-s", "--scope", help="Okta scope, typically SESSION:ROLE-ANY", default=None)
    parser.add_argument(
        "-e", "--env", action="store_true", help="Print result as an export command"
    )
    parser.add_argument(
        "-p", "--print-env", action="store_true", help="Print output in env format"
    )
    args = parser.parse_args()

    # Can only set role or scope.
    if args.scope is not None and args.role is not None:
        raise ValueError("Both the scope and role cannot be set, use one or the other")

    username, password = get_snowflake_user_credentials(args.username)
    client_id, client_secret = get_snowflake_secrets()
    # Example scope: SESSION%3AROLE%2DANY
    # First use scope
    if args.scope is not None:
        scope = urllib.parse.quote(args.scope, safe='')
    # Next use set role or environment role
    else:
        role = args.role or get_role_from_environment()
        if role is None:
            raise EnvironmentError("SNOWFLAKE_OKTA_ROLE env variable not set")
        scope = f'session%3Arole%3A{role}'

    response = get_okta_reponse(username, password, scope, client_id, client_secret)

    if args.env:
        print(f'export SNOWFLAKE_OKTA_TOKEN={response["access_token"]}')
    elif args.print_env:
        print(f'SNOWFLAKE_OKTA_TOKEN={response["access_token"]}')

    else:
        print(json.dumps(response, indent=2))


if __name__ == "__main__":
    main()
