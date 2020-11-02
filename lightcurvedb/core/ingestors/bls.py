import os
from configparser import ConfigParser


def get_bls_run_parameters(orbit, camera):
    """
    Open each QLP config file and attempt to determine what
    were the parameters for legacy BLS execution.
    """
    run_dir = orbit.get_sector_directory("ffi", "run")
    parser = ConfigParser()

    config_name = "example-lc-pdo{0}.cfg".format(camera)
    path = os.path.join(run_dir, config_name)

    parser.read(path)
    parameters = {
        "config_parameters": parser["BLS"],
        "bls_program": "vartools",
        "legacy": True,
    }

    return parameters
