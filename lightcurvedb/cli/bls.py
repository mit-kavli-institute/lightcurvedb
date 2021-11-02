import os
from glob import glob
from multiprocessing import Manager
from time import sleep

import click
from loguru import logger
from tabulate import tabulate
from tqdm import tqdm

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.core.ingestors.bls import BaseBLSIngestor
from lightcurvedb.models import Lightcurve
from lightcurvedb.models.bls import BLS
from lightcurvedb.util.contexts import extract_pdo_path_context


def get_tic(bls_summary):
    return int(os.path.basename(bls_summary).split(".")[0])


@lcdbcli.group()
@click.pass_context
def bls(ctx):
    """
    BLS Result Commands
    """


@bls.command()
@click.pass_context
@click.argument(
    "paths", type=click.Path(file_okay=False, exists=True), nargs=-1
)
@click.option("--n-processes", type=click.IntRange(0), default=32)
@click.option(
    "--qlp-data-path",
    type=click.Path(file_okay=False, exists=True),
    default="/pdo/qlp-data",
)
def legacy_ingest(ctx, paths, n_processes, qlp_data_path):
    req_contexts = ("sector", "camera", "ccd")
    path_context_map = {}
    path_files_map = {}

    for path in paths:
        context = extract_pdo_path_context(path)
        if not all(req_context in context for req_context in req_contexts):
            click.echo(
                f"Path {path} does not contain needed contexts "
                "(sector, camera, ccd)!"
            )
            click.abort()
        path_context_map[path] = context

    for path in paths:
        pattern = os.path.join(path, "*.blsanal")
        files = glob(pattern)
        path_files_map[path] = files
        click.echo(f"Found {len(files)} bls summary files at {path}")

    workers = []
    manager = Manager()
    job_queue = manager.Queue()

    for path in paths:
        files = path_files_map[path]
        context = path_context_map[path]
        sector_run_directory = os.path.join(
            qlp_data_path,
            "sector-{sector}".format(**context),
            "ffi",
            "run",
        )
        logger.debug(
            f"Setting {path} runtime parameters at {sector_run_directory}"
        )
        for summary in tqdm(files, unit=" files"):
            try:
                job = {
                    "path": summary,
                    "sector": int(context["sector"]),
                    "camera": int(context["camera"]),
                    "sector_run_directory": sector_run_directory,
                    "tic_id": int(os.path.basename(summary).split(".")[0]),
                }
            except (ValueError, IndexError):
                logger.warning(f"Unable to parse path {summary}")
                continue
            job_queue.put(job)

    for i in range(n_processes):
        worker = BaseBLSIngestor(
            ctx.obj["dbconf"]._config, f"Worker-{i}", job_queue
        )
        worker.start()
        workers.append(worker)

    while not job_queue.empty():
        sleep(1)

    for worker in workers:
        worker.join()


@bls.command()
@click.pass_context
@click.argument("tics", type=int, nargs=-1)
@click.option("--parameter", "-p", multiple=True, type=BLS.click_parameters)
def query(ctx, tics, parameter):
    with ctx.obj["dbconf"] as db:
        cols = [getattr(BLS, param) for param in parameter]
        q = (
            db.query(*cols)
            .join(BLS.lightcurve)
            .filter(Lightcurve.tic_id.in_(tics))
        )
        click.echo(tabulate(q.all(), headers=list(parameter)))
