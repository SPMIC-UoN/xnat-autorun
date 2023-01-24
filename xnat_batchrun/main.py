"""
XNAT-BATCHRUN: Run an XNAT container command on all sessions in a project
"""
import argparse
import csv
import getpass
import io
import json
import logging
import requests
import sys
import time
import traceback
import urllib3, urllib

from ._version import __version__

LOG = logging.getLogger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def run_command(options, session, idx):
    LOG.info(f"Running command {options.command} on session {idx} {session['ID']} : {session['label']}")
    url = f"{options.host}/xapi/projects/{options.project_id}/commands/{options.command_id}/wrappers/{options.command_wrapper_id}/launch/"
    params = {"session" : session["ID"]}
    LOG.debug(f"Launching command: {url} {params}")
    tries = 0
    while tries < 10:
        tries += 1
        r = requests.post(url, verify=False, auth=(options.user, options.password), params=params)
        if r.status_code == 200:
            break
        elif r.status_code in (501, 502):
            # We ignore proxy errors as usually this means the launch has succeeded?
            LOG.warning(f"Proxy error - assuming success for session {session['ID']}: {r.text}")
            break
        # Try again as a lot of errors are temporary
        time.sleep(10)

    if r.status_code not in (200, 501, 502):
        LOG.warning(f"Failed to run command on session {session['ID']}: {r.text} after 10 attempts")
    LOG.info("Started successfully")

def get_command(options):
    url = f"{options.host}/xapi/commands/available"
    params = {"project" : options.project_id, "xsiType" : "xnat:mrSessionData"}
    LOG.debug(f"Getting commands {url} {params}")
    r = requests.get(url, verify=False, auth=(options.user, options.password), params=params)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to query commands for project {options.project_id}: {r.text}")
    commands = json.loads(r.text)
    command = [c for c in commands if c["command-name"] == options.command]
    if not command:
        known_commands = [c["command-name"] for c in commands]
        raise RuntimeError(f"Unable to find command {options.command} - known commands: {known_commands}")
    return command[0]

def get_sessions(options):
    """
    Get session details for all sessions in specified project
    """
    url = f"{options.host}/data/projects/{options.project_id}/experiments/"
    params = {"xsiType": "xnat:mrSessionData", "format" : "csv"}
    LOG.debug(f"Getting sessions {url} {params}")
    r = requests.get(url, verify=False, auth=(options.user, options.password), params=params)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to download sessions for project {options.project_id}: {r.text}")
    return list(csv.DictReader(io.StringIO(r.text)))

def login(options):
    url = f"{options.host}/data/services/auth"
    auth_params={"username" : options.user, "password" : options.password}
    LOG.info(f"Logging in: {url}")
    r = requests.put(url, verify=False, data=urllib.parse.urlencode(auth_params))
    if r.status_code != 200:
        raise RuntimeError(f"Failed to log in: {r.text}")
    return r.text

def get_project(options):
    """
    Get project ID from specified project name/ID
    """
    options.jsession_id = login(options)
    url = f"{options.host}/data/projects/"
    params={"format" : "csv"}
    cookies = {"JSESSIONID" : options.jsession_id}
    LOG.debug(f"Getting projects {url} {params}")
    tries = 0
    while tries < 10:
        tries += 1
        #r = requests.get(url, verify=False, auth=(options.user, options.password), params=params)
        r = requests.get(url, verify=False, cookies=cookies, params=params)
        if r.status_code == 200:
            break

    if r.status_code != 200:
        raise RuntimeError(f"Failed to download projects after 10 tries: {r.status_code} {r.text}")
    projects = list(csv.DictReader(io.StringIO(r.text)))
    for project in projects:
        if project["ID"] == options.project or project["name"] == options.project:
            return project["ID"]
    
    projects = [p["name"] for p in projects]
    raise RuntimeError("Project not found: {options.project} - known project: {projects}")

class ArgumentParser(argparse.ArgumentParser):
    def __init__(self, **kwargs):
        argparse.ArgumentParser.__init__(self, prog="xnat-batchrun", add_help=False, **kwargs)
        self.add_argument("--host", help="XNAT host", required=True)
        self.add_argument("--project", help="XNAT project", required=True)
        self.add_argument("--user", help="XNAT username")
        self.add_argument("--command", help="Name of command to run")
        self.add_argument("--sleep", help="Time to sleep between commands in seconds", type=int, default=60)
        self.add_argument("--skip", help="Number of sessions to skip", type=int, default=0)
        self.add_argument("--yes", help="Run without prompting", action="store_true", default=False)

def main():
    """
    Main script entry point
    """
    options = ArgumentParser().parse_args()
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    LOG.info(f"XNAT batch run v{__version__}")

    try:
        if not options.user:
            options.user = input("XNAT username: ")
        options.password = getpass.getpass()
        LOG.info(f"Using XNAT: {options.host} with user: {options.user}")

        options.project_id = get_project(options)
        LOG.info(f"Found project: {options.project} with ID {options.project_id}")
        sessions = get_sessions(options)
        LOG.info(f"Found {len(sessions)} sessions")
        command = get_command(options)
        options.command_wrapper_id = command["wrapper-id"]
        options.command_id = command["command-id"]
        LOG.info(f"Found command {options.command} with ID {options.command_id} / {options.command_wrapper_id}")
        if not options.yes:
            confirm = input(f"Run command {options.command} on {len(sessions)} sessions? (yes/no): ")
            if confirm.strip().lower() not in ("y", "yes"):
                LOG.info("Aborting run")
                sys.exit(1)

        for idx, session in enumerate(sessions):
            if idx >= options.skip:
                run_command(options, session, idx)
                time.sleep(options.sleep)
            else:
                LOG.info(f"Skipping session {idx}: {session['label']}")

    except Exception as exc:
        LOG.error(exc)
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
