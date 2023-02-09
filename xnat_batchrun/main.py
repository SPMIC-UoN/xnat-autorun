"""
XNAT-BATCHRUN: Run an XNAT container command on all sessions in a project
"""
import argparse
import logging
import sys
import time

from ._version import __version__
from .xnat_nott import xnat_login, get_project, get_all_sessions, get_credentials, get_command, run_command, setup_logging

LOG = logging.getLogger(__name__)

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
        self.add_argument("--debug", help="Use debug logging")

def main():
    """
    Main script entry point
    """
    options = ArgumentParser().parse_args()
    setup_logging(options)

    LOG.info(f"XNAT batch run v{__version__}")

    try:
        get_credentials(options)
        xnat_login(options)

        project = get_project(options, options.project)
        LOG.info(f"Found project: ID {project['ID']}")

        sessions = get_all_sessions(options, project)
        LOG.info(f"Found {len(sessions)} sessions")

        command = get_command(options, project, options.command)
        LOG.info(f"Found command {options.command} with ID {command['command-id']} / {command['wrapper-id']}")

        if not options.yes:
            confirm = input(f"Run command {options.command} on {len(sessions)} sessions? (yes/no): ")
            if confirm.strip().lower() not in ("y", "yes"):
                LOG.info("Aborting run")
                sys.exit(1)

        for idx, session in enumerate(sessions):
            if idx >= options.skip:
                run_command(options, project, session, command, idx=idx)
                time.sleep(options.sleep)
            else:
                LOG.info(f"Skipping session {idx}: {session['label']}")

    except Exception as exc:
        LOG.exception("Unexpected error")
        sys.exit(1)

if __name__ == "__main__":
    main()
