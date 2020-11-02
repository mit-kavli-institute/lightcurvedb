import os
from configparser import ConfigParser


def parameter_key_check(*keys):
    pass


def get_bls_run_parameters(orbit, cameras):
    """
    Open each QLP config file and attempt to determine what
    were the parameters for legacy BLS execution.
    """
    run_dir = orbit.get_sector_directory(
        "ffi", "run"
    )
    parser = ConfigParser()

    parameter_checks = []

    for camera in cameras:
        config_name = "example-lc-pdo{0}.cfg".format(camera)
        path = os.path.join(run_dir, config_name)

        parser.read(path)
        parameters = parser["BLS"]
        parameter_checks.append(parameters)

    keys = [params.keys() for params in parameter_checks]

    # If the # of keys do not match then something is wrong
