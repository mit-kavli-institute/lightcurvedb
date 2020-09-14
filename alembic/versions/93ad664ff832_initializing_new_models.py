"""initializing new models

Revision ID: 93ad664ff832
Revises: 
Create Date: 2020-08-15 02:15:34.989060

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '93ad664ff832'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('apertures',
    sa.Column('created_on', sa.DateTime(), server_default=sa.text(u'now()'), nullable=True),
    sa.Column('name', sa.String(length=64), nullable=False),
    sa.Column('star_radius', sa.Numeric(), nullable=False),
    sa.Column('inner_radius', sa.Numeric(), nullable=False),
    sa.Column('outer_radius', sa.Numeric(), nullable=False),
    sa.CheckConstraint(u'char_length(name) >= 1', name='minimum_name_length'),
    sa.PrimaryKeyConstraint('name'),
    sa.UniqueConstraint('star_radius', 'inner_radius', 'outer_radius')
    )
    op.create_table('frametypes',
    sa.Column('name', sa.String(length=64), nullable=False),
    sa.Column('description', sa.String(), nullable=True),
    sa.Column('created_on', sa.DateTime(), server_default=sa.text(u'now()'), nullable=True),
    sa.PrimaryKeyConstraint('name')
    )
    op.create_table('lightcurvetypes',
    sa.Column('name', sa.String(length=64), nullable=False),
    sa.Column('description', sa.String(), nullable=True),
    sa.Column('created_on', sa.DateTime(), server_default=sa.text(u'now()'), nullable=True),
    sa.PrimaryKeyConstraint('name')
    )
    op.create_table('orbits',
    sa.Column('created_on', sa.DateTime(), server_default=sa.text(u'now()'), nullable=True),
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('orbit_number', sa.Integer(), nullable=False),
    sa.Column('sector', sa.Integer(), nullable=False),
    sa.Column('right_ascension', postgresql.DOUBLE_PRECISION(), nullable=False),
    sa.Column('declination', postgresql.DOUBLE_PRECISION(), nullable=False),
    sa.Column('roll', postgresql.DOUBLE_PRECISION(), nullable=False),
    sa.Column('quaternion_x', postgresql.DOUBLE_PRECISION(), nullable=False),
    sa.Column('quaternion_y', postgresql.DOUBLE_PRECISION(), nullable=False),
    sa.Column('quaternion_z', postgresql.DOUBLE_PRECISION(), nullable=False),
    sa.Column('quaternion_q', postgresql.DOUBLE_PRECISION(), nullable=False),
    sa.Column('crm', sa.Boolean(), nullable=False),
    sa.Column('crm_n', sa.Integer(), nullable=False),
    sa.Column('basename', sa.String(length=256), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('orbit_number')
    )
    op.create_table('qlpprocesses',
    sa.Column('created_on', sa.DateTime(), server_default=sa.text(u'now()'), nullable=True),
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('job_type', sa.String(length=255), nullable=True),
    sa.Column('job_version_major', sa.SmallInteger(), nullable=False),
    sa.Column('job_version_minor', sa.SmallInteger(), nullable=False),
    sa.Column('job_version_revision', sa.Integer(), nullable=False),
    sa.Column('additional_version_info', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_qlpprocesses_additional_version_info'), 'qlpprocesses', ['additional_version_info'], unique=False)
    op.create_index(op.f('ix_qlpprocesses_job_type'), 'qlpprocesses', ['job_type'], unique=False)
    op.create_index(op.f('ix_qlpprocesses_job_version_major'), 'qlpprocesses', ['job_version_major'], unique=False)
    op.create_index(op.f('ix_qlpprocesses_job_version_minor'), 'qlpprocesses', ['job_version_minor'], unique=False)
    op.create_index(op.f('ix_qlpprocesses_job_version_revision'), 'qlpprocesses', ['job_version_revision'], unique=False)
    op.create_table('spacecraftephemeris',
    sa.Column('created_on', sa.DateTime(), server_default=sa.text(u'now()'), nullable=True),
    sa.Column('barycentric_dynamical_time', sa.Float(), nullable=False),
    sa.Column('calendar_date', sa.DateTime(), nullable=True),
    sa.Column('x_coordinate', postgresql.DOUBLE_PRECISION(), nullable=True),
    sa.Column('y_coordinate', postgresql.DOUBLE_PRECISION(), nullable=True),
    sa.Column('z_coordinate', postgresql.DOUBLE_PRECISION(), nullable=True),
    sa.Column('light_travel_time', postgresql.DOUBLE_PRECISION(), nullable=True),
    sa.Column('range_to', postgresql.DOUBLE_PRECISION(), nullable=True),
    sa.Column('range_rate', postgresql.DOUBLE_PRECISION(), nullable=True),
    sa.PrimaryKeyConstraint('barycentric_dynamical_time')
    )
    op.create_index(op.f('ix_spacecraftephemeris_calendar_date'), 'spacecraftephemeris', ['calendar_date'], unique=False)
    op.create_table('best_apertures',
    sa.Column('created_on', sa.DateTime(), server_default=sa.text(u'now()'), nullable=True),
    sa.Column('aperture_id', sa.String(length=64), nullable=False),
    sa.Column('tic_id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['aperture_id'], [u'apertures.name'], onupdate='CASCADE', ondelete='RESTRICT'),
    sa.PrimaryKeyConstraint('aperture_id', 'tic_id'),
    sa.UniqueConstraint('tic_id', name='best_ap_unique_tic')
    )
    op.create_table('frames',
    sa.Column('created_on', sa.DateTime(), server_default=sa.text(u'now()'), nullable=True),
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('cadence_type', sa.SmallInteger(), nullable=False),
    sa.Column('camera', sa.SmallInteger(), nullable=False),
    sa.Column('ccd', sa.SmallInteger(), nullable=True),
    sa.Column('cadence', sa.Integer(), nullable=False),
    sa.Column('gps_time', postgresql.DOUBLE_PRECISION(), nullable=False),
    sa.Column('start_tjd', postgresql.DOUBLE_PRECISION(), nullable=False),
    sa.Column('mid_tjd', postgresql.DOUBLE_PRECISION(), nullable=False),
    sa.Column('end_tjd', postgresql.DOUBLE_PRECISION(), nullable=False),
    sa.Column('exp_time', postgresql.DOUBLE_PRECISION(), nullable=False),
    sa.Column('quality_bit', sa.Boolean(), nullable=False),
    sa.Column('file_path', sa.String(), nullable=False),
    sa.Column('orbit_id', sa.Integer(), nullable=False),
    sa.Column('frame_type_id', sa.String(length=64), nullable=False),
    sa.CheckConstraint(u'(ccd IS NULL) OR (ccd BETWEEN 1 AND 4)', name='physical_ccd_constraint'),
    sa.CheckConstraint(u'camera BETWEEN 1 and 4', name='physical_camera_constraint'),
    sa.ForeignKeyConstraint(['frame_type_id'], ['frametypes.name'], ondelete='RESTRICT'),
    sa.ForeignKeyConstraint(['orbit_id'], ['orbits.id'], ondelete='RESTRICT'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('file_path'),
    sa.UniqueConstraint('frame_type_id', 'orbit_id', 'cadence', 'camera', 'ccd', name='unique_frame')
    )
    op.create_index(op.f('ix_frames_cadence'), 'frames', ['cadence'], unique=False)
    op.create_index(op.f('ix_frames_cadence_type'), 'frames', ['cadence_type'], unique=False)
    op.create_index(op.f('ix_frames_camera'), 'frames', ['camera'], unique=False)
    op.create_index(op.f('ix_frames_ccd'), 'frames', ['ccd'], unique=False)
    op.create_index(op.f('ix_frames_frame_type_id'), 'frames', ['frame_type_id'], unique=False)
    op.create_index(op.f('ix_frames_orbit_id'), 'frames', ['orbit_id'], unique=False)
    op.create_table('lightcurves',
    sa.Column('created_on', sa.DateTime(), server_default=sa.text(u'now()'), nullable=True),
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('tic_id', sa.BigInteger(), nullable=True),
    sa.Column('cadence_type', sa.SmallInteger(), nullable=True),
    sa.Column('lightcurve_type_id', sa.String(length=64), nullable=True),
    sa.Column('aperture_id', sa.String(length=64), nullable=True),
    sa.ForeignKeyConstraint(['aperture_id'], ['apertures.name'], onupdate='CASCADE', ondelete='RESTRICT'),
    sa.ForeignKeyConstraint(['lightcurve_type_id'], ['lightcurvetypes.name'], onupdate='CASCADE', ondelete='RESTRICT'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('lightcurve_type_id', 'aperture_id', 'tic_id', name='unique_lightcurve_constraint')
    )
    op.create_index(op.f('ix_lightcurves_aperture_id'), 'lightcurves', ['aperture_id'], unique=False)
    op.create_index(op.f('ix_lightcurves_cadence_type'), 'lightcurves', ['cadence_type'], unique=False)
    op.create_index(op.f('ix_lightcurves_lightcurve_type_id'), 'lightcurves', ['lightcurve_type_id'], unique=False)
    op.create_index(op.f('ix_lightcurves_tic_id'), 'lightcurves', ['tic_id'], unique=False)
    op.create_table('observations',
    sa.Column('tic_id', sa.BigInteger(), nullable=False),
    sa.Column('camera', sa.SmallInteger(), nullable=False),
    sa.Column('ccd', sa.SmallInteger(), nullable=False),
    sa.Column('orbit_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['orbit_id'], ['orbits.id'], ondelete='RESTRICT'),
    sa.PrimaryKeyConstraint('tic_id', 'orbit_id')
    )
    op.create_index(op.f('ix_observations_camera'), 'observations', ['camera'], unique=False)
    op.create_index(op.f('ix_observations_ccd'), 'observations', ['ccd'], unique=False)
    op.create_table('qlpalterations',
    sa.Column('created_on', sa.DateTime(), server_default=sa.text(u'now()'), nullable=True),
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('process_id', sa.Integer(), nullable=False),
    sa.Column('target_model', sa.String(length=255), nullable=False),
    sa.Column('_alteration_type', sa.String(length=255), nullable=False),
    sa.Column('n_altered_items', sa.BigInteger(), nullable=False),
    sa.Column('est_item_size', sa.BigInteger(), nullable=False),
    sa.Column('time_start', sa.DateTime(), nullable=False),
    sa.Column('time_end', sa.DateTime(), nullable=False),
    sa.Column('_query', sa.Text(), nullable=True),
    sa.ForeignKeyConstraint(['process_id'], [u'qlpprocesses.id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_qlpalterations__alteration_type'), 'qlpalterations', ['_alteration_type'], unique=False)
    op.create_index(op.f('ix_qlpalterations__query'), 'qlpalterations', ['_query'], unique=False)
    op.create_index(op.f('ix_qlpalterations_est_item_size'), 'qlpalterations', ['est_item_size'], unique=False)
    op.create_index(op.f('ix_qlpalterations_n_altered_items'), 'qlpalterations', ['n_altered_items'], unique=False)
    op.create_index(op.f('ix_qlpalterations_target_model'), 'qlpalterations', ['target_model'], unique=False)
    op.create_index(op.f('ix_qlpalterations_time_end'), 'qlpalterations', ['time_end'], unique=False)
    op.create_index(op.f('ix_qlpalterations_time_start'), 'qlpalterations', ['time_start'], unique=False)
    op.create_table('lightcurveframemapping',
    sa.Column('lightcurve_type_id', sa.BigInteger(), nullable=False),
    sa.Column('frame_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['frame_id'], ['frames.id'], ),
    sa.ForeignKeyConstraint(['lightcurve_type_id'], ['lightcurves.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('lightcurve_type_id', 'frame_id')
    )
    op.create_table('lightpoints',
    sa.Column('lightcurve_id', sa.BigInteger(), nullable=False),
    sa.Column('cadence', sa.BigInteger(), nullable=False),
    sa.Column('barycentric_julian_date', postgresql.DOUBLE_PRECISION(), nullable=False),
    sa.Column('data', postgresql.DOUBLE_PRECISION(), nullable=True),
    sa.Column('error', postgresql.DOUBLE_PRECISION(), nullable=True),
    sa.Column('x_centroid', postgresql.DOUBLE_PRECISION(), nullable=True),
    sa.Column('y_centroid', postgresql.DOUBLE_PRECISION(), nullable=True),
    sa.Column('quality_flag', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['lightcurve_id'], ['lightcurves.id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('lightcurve_id', 'cadence'),
    postgresql_partition_by='range(lightcurve_id)'
    )
    op.create_index(op.f('ix_lightpoints_cadence'), 'lightpoints', ['cadence'], unique=False, postgresql_using='brin')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_lightpoints_cadence'), table_name='lightpoints')
    op.drop_table('lightpoints')
    op.drop_table('lightcurveframemapping')
    op.drop_index(op.f('ix_qlpalterations_time_start'), table_name='qlpalterations')
    op.drop_index(op.f('ix_qlpalterations_time_end'), table_name='qlpalterations')
    op.drop_index(op.f('ix_qlpalterations_target_model'), table_name='qlpalterations')
    op.drop_index(op.f('ix_qlpalterations_n_altered_items'), table_name='qlpalterations')
    op.drop_index(op.f('ix_qlpalterations_est_item_size'), table_name='qlpalterations')
    op.drop_index(op.f('ix_qlpalterations__query'), table_name='qlpalterations')
    op.drop_index(op.f('ix_qlpalterations__alteration_type'), table_name='qlpalterations')
    op.drop_table('qlpalterations')
    op.drop_index(op.f('ix_observations_ccd'), table_name='observations')
    op.drop_index(op.f('ix_observations_camera'), table_name='observations')
    op.drop_table('observations')
    op.drop_index(op.f('ix_lightcurves_tic_id'), table_name='lightcurves')
    op.drop_index(op.f('ix_lightcurves_lightcurve_type_id'), table_name='lightcurves')
    op.drop_index(op.f('ix_lightcurves_cadence_type'), table_name='lightcurves')
    op.drop_index(op.f('ix_lightcurves_aperture_id'), table_name='lightcurves')
    op.drop_table('lightcurves')
    op.drop_index(op.f('ix_frames_orbit_id'), table_name='frames')
    op.drop_index(op.f('ix_frames_frame_type_id'), table_name='frames')
    op.drop_index(op.f('ix_frames_ccd'), table_name='frames')
    op.drop_index(op.f('ix_frames_camera'), table_name='frames')
    op.drop_index(op.f('ix_frames_cadence_type'), table_name='frames')
    op.drop_index(op.f('ix_frames_cadence'), table_name='frames')
    op.drop_table('frames')
    op.drop_table('best_apertures')
    op.drop_index(op.f('ix_spacecraftephemeris_calendar_date'), table_name='spacecraftephemeris')
    op.drop_table('spacecraftephemeris')
    op.drop_index(op.f('ix_qlpprocesses_job_version_revision'), table_name='qlpprocesses')
    op.drop_index(op.f('ix_qlpprocesses_job_version_minor'), table_name='qlpprocesses')
    op.drop_index(op.f('ix_qlpprocesses_job_version_major'), table_name='qlpprocesses')
    op.drop_index(op.f('ix_qlpprocesses_job_type'), table_name='qlpprocesses')
    op.drop_index(op.f('ix_qlpprocesses_additional_version_info'), table_name='qlpprocesses')
    op.drop_table('qlpprocesses')
    op.drop_table('orbits')
    op.drop_table('lightcurvetypes')
    op.drop_table('frametypes')
    op.drop_table('apertures')
    # ### end Alembic commands ###