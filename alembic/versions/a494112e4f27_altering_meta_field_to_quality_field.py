"""altering meta field to quality field

Revision ID: a494112e4f27
Revises: 75cab39984bb
Create Date: 2020-02-05 15:46:36.234251

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'a494112e4f27'
down_revision = '75cab39984bb'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('lightcurves', sa.Column('quality_flags', postgresql.ARRAY(sa.Integer(), dimensions=1), nullable=False))
    op.create_check_constraint('quality_flags_congruence', 'lightcurves', 'array_length(quality_flags, 1) = array_length(cadences, 1)')
    op.drop_index('ix_lightcurves_orbit_id', table_name='lightcurves')
    op.drop_constraint('lightcurves_orbit_id_fkey', 'lightcurves', type_='foreignkey')
    op.drop_column('lightcurves', 'orbit_id')
    op.drop_column('lightcurves', 'meta')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('lightcurves', sa.Column('meta', postgresql.ARRAY(sa.INTEGER()), autoincrement=False, nullable=False))
    op.add_column('lightcurves', sa.Column('orbit_id', sa.BIGINT(), autoincrement=False, nullable=True))
    op.create_foreign_key('lightcurves_orbit_id_fkey', 'lightcurves', 'orbits', ['orbit_id'], ['id'], onupdate='CASCADE', ondelete='RESTRICT')
    op.create_index('ix_lightcurves_orbit_id', 'lightcurves', ['orbit_id'], unique=False)
    op.drop_constraint('quality_flags_congruence', 'lightcurves')
    op.drop_column('lightcurves', 'quality_flags')
    # ### end Alembic commands ###
