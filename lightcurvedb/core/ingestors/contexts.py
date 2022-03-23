import pandas as pd


_TIC_CATALOG_NAMES = (
    "tic_id",
    "ra",
    "dec",
    "tmag",
    "pmra",
    "pmdec",
    "jmag",
    "kmag",
    "vmag",
)
_QUALITY_FLAG_NAMES = (
    "cadence",
    "quality_flag",
)


def load_tic_catalog(path, sort=False):
    df = pd.read_csv(
        path,
        sep="\s+",
        names=_TIC_CATALOG_NAMES,
        index_col="tic_id"
    )
    return df.sort_index() if sort else df


def load_quality_flags(path):
    df = pd.read_csv(
        path,
        sep="\s+",
        names=_QUALITY_FLAG_NAMES,
        index_col="cadence"
    )
    return df
