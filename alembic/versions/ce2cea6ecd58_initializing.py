"""initializing

Revision ID: ce2cea6ecd58
Revises: 
Create Date: 2020-02-05 09:19:56.551925

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.schema import Sequence, CreateSequence, DropSequence
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'ce2cea6ecd58'
down_revision = None
branch_labels = None
depends_on = None

def create_sequence(name):
    op.execute(CreateSequence(name))


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###

    # Create sequences
    op.execute(CreateSequence(Sequence('qlpdataproducts_pk_table')))
    op.execute(CreateSequence(Sequence('qlpdatasubtypes_pk_table')))
    op.execute(CreateSequence(Sequence('qlpreferences_pk_table')))

    op.create_table('qlpdataproducts',
    sa.Column('created_on', sa.DateTime(), nullable=True),
    sa.Column('product_type', sa.String(length=255), nullable=True),
    sa.Column('id', sa.BigInteger(), sa.Sequence('qlpdataproducts_pk_table'), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('qlpdatasubtypes',
    sa.Column('created_on', sa.DateTime(), nullable=True),
    sa.Column('name', sa.String(length=64), nullable=False),
    sa.Column('description', sa.String(), nullable=True),
    sa.Column('subtype', sa.String(length=255), nullable=True),
    sa.Column('id', sa.BigInteger(), sa.Sequence('qlpdatasubtypes_pk_table'), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('subtype', 'name')
    )
    op.create_index(op.f('ix_qlpdatasubtypes_name'), 'qlpdatasubtypes', ['name'], unique=False)
    op.create_table('qlpreferences',
    sa.Column('created_on', sa.DateTime(), nullable=True),
    sa.Column('reference_type', sa.String(length=255), nullable=True),
    sa.Column('id', sa.BigInteger(), sa.Sequence('qlpreferences_pk_table'), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('apertures',
    sa.Column('name', sa.String(length=64), nullable=False),
    sa.Column('star_radius', sa.Numeric(), nullable=False),
    sa.Column('inner_radius', sa.Numeric(), nullable=False),
    sa.Column('outer_radius', sa.Numeric(), nullable=False),
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.CheckConstraint('char_length(name) >= 1', name='minimum_name_length'),
    sa.ForeignKeyConstraint(['id'], ['qlpreferences.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('name'),
    sa.UniqueConstraint('star_radius', 'inner_radius', 'outer_radius')
    )
    op.create_table('frametypes',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['id'], ['qlpdatasubtypes.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('lightcurvetypes',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['id'], ['qlpdatasubtypes.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('orbits',
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
    sa.Column('basename', sa.String(length=256), nullable=False),
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['id'], ['qlpreferences.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('orbit_number')
    )
    op.create_table('frames',
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
    sa.Column('orbit_id', sa.Integer(), nullable=True),
    sa.Column('frame_type_id', sa.BigInteger(), nullable=True),
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.CheckConstraint('(ccd IS NULL) OR (ccd BETWEEN 1 AND 4)', name='physical_ccd_constraint'),
    sa.CheckConstraint('camera BETWEEN 1 and 4', name='physical_camera_constraint'),
    sa.ForeignKeyConstraint(['frame_type_id'], ['frametypes.id'], ondelete='RESTRICT'),
    sa.ForeignKeyConstraint(['id'], ['qlpdataproducts.id'], ),
    sa.ForeignKeyConstraint(['orbit_id'], ['orbits.id'], ondelete='RESTRICT'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('file_path'),
    sa.UniqueConstraint('frame_type_id', 'orbit_id', 'cadence', 'camera', 'ccd', name='unique_frame')
    )
    op.create_index(op.f('ix_frames_cadence'), 'frames', ['cadence'], unique=False)
    op.create_index(op.f('ix_frames_cadence_type'), 'frames', ['cadence_type'], unique=False)
    op.create_index(op.f('ix_frames_camera'), 'frames', ['camera'], unique=False)
    op.create_index(op.f('ix_frames_ccd'), 'frames', ['ccd'], unique=False)
    op.create_table('lightcurves',
    sa.Column('tic_id', sa.BigInteger(), nullable=True),
    sa.Column('cadence_type', sa.SmallInteger(), nullable=True),
    sa.Column('cadences', postgresql.ARRAY(sa.Integer(), dimensions=1), nullable=False),
    sa.Column('barycentric_julian_date', postgresql.ARRAY(sa.Integer(), dimensions=1), nullable=False),
    sa.Column('flux', postgresql.ARRAY(postgresql.DOUBLE_PRECISION(), dimensions=1), nullable=False),
    sa.Column('flux_err', postgresql.ARRAY(postgresql.DOUBLE_PRECISION(), dimensions=1), nullable=False),
    sa.Column('x_centroids', postgresql.ARRAY(postgresql.DOUBLE_PRECISION(), dimensions=1), nullable=False),
    sa.Column('y_centroids', postgresql.ARRAY(postgresql.DOUBLE_PRECISION(), dimensions=1), nullable=False),
    sa.Column('meta', postgresql.ARRAY(sa.Integer(), dimensions=1), nullable=False),
    sa.Column('lightcurve_type_id', sa.BigInteger(), nullable=True),
    sa.Column('aperture_id', sa.BigInteger(), nullable=True),
    sa.Column('orbit_id', sa.BigInteger(), nullable=True),
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.CheckConstraint('array_length(barycentric_julian_date, 1) = array_length(cadences, 1)', name='barycentric_julian_date_congruence'),
    sa.CheckConstraint('array_length(flux, 1) = array_length(cadences, 1)', name='flux_congruence'),
    sa.CheckConstraint('array_length(flux_err, 1) = array_length(cadences, 1)', name='flux_err_congruence'),
    sa.CheckConstraint('array_length(meta, 1) = array_length(cadences, 1)', name='meta_congruence'),
    sa.CheckConstraint('array_length(x_centroids, 1) = array_length(cadences, 1)', name='x_centroids_congruence'),
    sa.CheckConstraint('array_length(y_centroids, 1) = array_length(cadences, 1)', name='y_centroids_congruence'),
    sa.ForeignKeyConstraint(['aperture_id'], ['apertures.id'], onupdate='CASCADE', ondelete='RESTRICT'),
    sa.ForeignKeyConstraint(['id'], ['qlpdataproducts.id'], ),
    sa.ForeignKeyConstraint(['lightcurve_type_id'], ['lightcurvetypes.id'], onupdate='CASCADE', ondelete='RESTRICT'),
    sa.ForeignKeyConstraint(['orbit_id'], ['orbits.id'], onupdate='CASCADE', ondelete='RESTRICT'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('cadence_type', 'lightcurve_type_id', 'aperture_id', 'tic_id')
    )
    op.create_index(op.f('ix_lightcurves_aperture_id'), 'lightcurves', ['aperture_id'], unique=False)
    op.create_index(op.f('ix_lightcurves_cadence_type'), 'lightcurves', ['cadence_type'], unique=False)
    op.create_index(op.f('ix_lightcurves_lightcurve_type_id'), 'lightcurves', ['lightcurve_type_id'], unique=False)
    op.create_index(op.f('ix_lightcurves_orbit_id'), 'lightcurves', ['orbit_id'], unique=False)
    op.create_index(op.f('ix_lightcurves_tic_id'), 'lightcurves', ['tic_id'], unique=False)
    op.create_table('lightcurveframemapping',
    sa.Column('created_on', sa.DateTime(), nullable=True),
    sa.Column('lightcurve_type_id', sa.BigInteger(), nullable=False),
    sa.Column('frame_id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['frame_id'], ['frames.id'], ),
    sa.ForeignKeyConstraint(['lightcurve_type_id'], ['lightcurves.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('lightcurve_type_id', 'frame_id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('lightcurveframemapping')
    op.drop_index(op.f('ix_lightcurves_tic_id'), table_name='lightcurves')
    op.drop_index(op.f('ix_lightcurves_orbit_id'), table_name='lightcurves')
    op.drop_index(op.f('ix_lightcurves_lightcurve_type_id'), table_name='lightcurves')
    op.drop_index(op.f('ix_lightcurves_cadence_type'), table_name='lightcurves')
    op.drop_index(op.f('ix_lightcurves_aperture_id'), table_name='lightcurves')
    op.drop_table('lightcurves')
    op.drop_index(op.f('ix_frames_ccd'), table_name='frames')
    op.drop_index(op.f('ix_frames_camera'), table_name='frames')
    op.drop_index(op.f('ix_frames_cadence_type'), table_name='frames')
    op.drop_index(op.f('ix_frames_cadence'), table_name='frames')
    op.drop_table('frames')
    op.drop_table('orbits')
    op.drop_table('lightcurvetypes')
    op.drop_table('frametypes')
    op.drop_table('apertures')
    op.drop_table('qlpreferences')
    op.drop_index(op.f('ix_qlpdatasubtypes_name'), table_name='qlpdatasubtypes')
    op.drop_table('qlpdatasubtypes')
    op.drop_table('qlpdataproducts')

    op.execute(DropSequence('qlpdataproducts_pk_table'))
    op.execute(DropSequence('qlpdatasubtypes_pk_table'))
    op.execute(DropSequence('qlpreferences_pk_table'))

    # ### end Alembic commands ###