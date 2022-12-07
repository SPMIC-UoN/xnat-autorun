"""
XNAT-AUTORUN: Run a command on all sessions in a project
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
import urllib3

LOG = logging.getLogger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def run_command(options, session):
    LOG.info(f"Running command {options.command} on session {session['ID']} : {session['label']}")
    url = f"{options.host}/xapi/projects/{options.project_id}/commands/{options.command_id}/wrappers/{options.command_wrapper_id}/launch/"
    params = {"session" : session["ID"]}
    LOG.debug(f"Launching command: {url} {params}")
    r = requests.post(url, verify=False, auth=(options.user, options.password), params=params)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to run command: {r.text}")
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

def get_project(options):
    """
    Get project ID from specified project name/ID
    """
    url = f"{options.host}/data/projects/"
    params={"format" : "csv"}
    LOG.debug(f"Getting projects {url} {params}")
    r = requests.get(url, verify=False, auth=(options.user, options.password), params=params)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to download projects: {r.text}")
    projects = list(csv.DictReader(io.StringIO(r.text)))
    for project in projects:
        if project["ID"] == options.project or project["name"] == options.project:
            return project["ID"]
    
    projects = [p["name"] for p in projects]
    raise RuntimeError("Project not found: {options.project} - known project: {projects}")

class ArgumentParser(argparse.ArgumentParser):
    def __init__(self, **kwargs):
        argparse.ArgumentParser.__init__(self, prog="imgqc", add_help=False, **kwargs)
        self.add_argument("--host", help="XNAT host", required=True)
        self.add_argument("--project", help="XNAT project", required=True)
        self.add_argument("--user", help="XNAT username")
        self.add_argument("--command", help="Name of command to run")
        self.add_argument("--sleep", help="Time to sleep between commands in seconds", type=int, default=60)
        self.add_argument("--yes", help="Run without prompting", action="store_true", default=False)

def main():
    """
    Main script entry point
    """
    options = ArgumentParser().parse_args()
    version = "0.0.1" # FIXME

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    LOG.info(f"XNAT Autorun v{version}")

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

        for session in sessions:
            run_command(options, session)
            time.sleep(options.sleep)

    except Exception as exc:
        LOG.error(exc)
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
