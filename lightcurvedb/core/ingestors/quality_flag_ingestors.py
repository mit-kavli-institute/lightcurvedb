import pandas as pd
import os
import re
from lightcurvedb.util.logger import lcdb_logger as logger
from .temp_table import QualityFlags


def load_qflag_file(
        db_session,
        qflag_file,
        camera_override=None,
        ccd_override=None
        ):

    base_file = os.path.basename(qflag_file)
    if camera_override:
        camera = camera_override
    else:
        camera = re.search(
            r'cam(?P<camera>[1-4])',
            base_file
        ).groupdict()['camera']

    if ccd_override:
        ccd = ccd_override
    else:
        ccd = re.search(
            r'ccd(?P<ccd>[1-4])',
            base_file
        ).groupdict()['ccd']

    try:
        q_df = pd.read_csv(
            qflag_file,
            delimiter=' ',
            names=['cadence', 'quality_flag'],
            dtype={'cadence': int, 'quality_flag': int}
        )
        q_df['camera'] = camera
        q_df['ccd'] = ccd
        db_session.bulk_insert_mappings(
            QualityFlags,
            q_df.to_dict('records')
        )
    except FileNotFoundError:
        logger.debug(
            'Could not find specified quality flag file {}'.format(
                qflag_file
            )
        )
    except OSError:
        logger.debug(
            'Could not open specified quality flag file {}'.format(
                qflag_file
            )
        )


def get_qflag_df(db_session):
    q = db_session.query(
        QualityFlags.cadence.label('cadences'),
        QualityFlags.camera,
        QualityFlags.ccd,
        QualityFlags.quality_flag.label('quality_flags')
    )
    df = pd.read_sql(
        q.statement,
        db_session.bind,
        index_col=['cadences', 'camera', 'ccd']
    )
    return df


def update_qflag(quality_flag_df, lp_df):
    """
    Update the lightpoint dataframe with known quality flags in
    the TempDatabase.

    Assumes that camera and ccd information is in the dataframe.

    Will modify the given dataframe
    """
    lp_df.reset_index(inplace=True)
    lp_df.set_index(['cadences', 'camera', 'ccd'], inplace=True)
    lp_df.update(quality_flag_df)
    lp_df['quality_flags'] = lp_df['quality_flags'].astype(int)

    lp_df.reset_index(inplace=True)
    lp_df.set_index(['cadences'])
