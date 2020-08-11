from sqlalchemy import func, bindparam
from sqlalchemy.dialects.postgresql import aggregate_order_by

from lightcurvedb.models import Lightcurve
from lightcurvedb.en_masse.temp_table import declare_lightcurve_cadence_map
from lightcurvedb.util.logger import lcdb_logger as logger


def perform_update(
        sql_session,
        temp_table,
        lightcurve_q,
        cadences,
        quality_flags
        ):
    insert_q = temp_table.insert().from_select(
        ['lightcurve_id', 'cadence', 'quality_flag'],
        lightcurve_q.subquery('lightcurve_subq')
    )
    sql_session.execute(insert_q)

    update_q = temp_table.update().where(
        temp_table.c.cadence == bindparam('_cadence')
    ).values({
        update_q.c.quality_flag: bindparam('_quality_flag')
    })

    sql_session.execute(
        update_q,
        [
            {
                '_cadence': cadence,
                '_quality_flag': qflag
            } for cadence, qflag in zip(cadences, quality_flags)
        ]
    )

    lightcurve_update_q = sql_session.query(
        temp_table.c.lightcurve_id,
        func.array_agg(
            aggregate_order_by(
                temp_table.c.quality_flag,
                temp_table.c.cadence.asc()
            )
        ).label('new_qflags')
    ).group_by(temp_table.c.lightcurve_id).cte('updated_qflags')

    update_q = Lightcurve.__table__.update().where(
        Lightcurve.id == lightcurve_update_q.lightcurve_id
    ).values({
        Lightcurve.quality_flags: update_qflag.c.new_qflags
    })

    sql_session.execute(
        update_q
    )

    # Ids in temp table are no longer needed
    sql_session.execute(
        temp_table.delete().where(
            temp_table.c.lightcurve_id.in_(
                lightcurve_iq.subquery('lightcurve_subq')
            )
        )
    )


def set_quality_flags(
        sql_session,
        lightcurve_id_q,
        cadences, 
        quality_flags,
        chunksize=1000,
        temp_buffers_mb=1024*8
        ):
    """
    Assign quality flags en masse to the lightcurve query. Updates are
    performed using the passed cadence and quality flag arrays.

    Arguments
    ---------
    sql_session : sqlalchemy.Session
        The sql session object to perform the update
    lightcurve_id_q : sqlalchemy.query.Query
        A query of lightcurve ids to use.
    cadences : iterable of integers
        The cadences to key by to assign quality flags
    quality_flags : iterable of integers
        The quality flags to assign in relation to the passed ``cadences``
    chunksize : integer
        The number of lightcurves to process at once
    temp_buffers_mb : integer
        Memory in the size of megabytes to set the ``temp_buffers`` to.
        This makes ``postgresql`` allow for larger temporary tables to remain
        in memory. Please set with caution.

    Notes
    -----
    This method utilizes Temporary Tables which SQLAlchemy requires
    a clean session. Any present and uncommitted changes will be
    rolledback and a commit is emitted in order to construct the
    temporary tables.

    Keep in mind that subsequent calls to quality flags will `NOT` change
    the ``temp_buffers`` size.
    """

    sql_session.rollback()
    PSQL_SET = text(
        'SET temp_buffers TO "{}MB"'.format(temp_buffers_mb)
    )
    sql_session.execute(PSQL_SET)
    QMap = declare_lightcurve_cadence_map(
        Column('quality_flag', Integer, nullable=False)
        extend_existing=True,
        keep_existing=False
    )
    QMap.create(
        bind=sql_session.bind,
        checkfirst=True
    )

    for id_chunk in chunkify(lightcurve_id_q.all()):
        q = sql_session.query(
            Lightcurve.id,
            func.unnest(Lightcurve.cadences),
            func.unnest(Lightcurve.quality_flags)
        ).filter(
            Lightcurve.id.in_(id_chunk)
        )
        logger.debug(
            'Updating {} lightcurves with new quality_flags'.format(
                len(id_chunk)
            )
        )
        perform_update(
            sqlsession,
            QMap,
            q,
            cadences,
            quality_flags
        )
        sql_session.commit()
    logger.debug(
        'Done assigning new quality flags'
    )
