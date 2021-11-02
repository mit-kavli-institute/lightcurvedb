"""adding best orbit lightcurve table

Revision ID: efb12c5fb79c
Revises: ca0d6f4e1d49
Create Date: 2021-10-21 10:27:11.399328

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'efb12c5fb79c'
down_revision = 'ca0d6f4e1d49'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('best_orbit_lightcurves',
    sa.Column('created_on', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('lightcurve_id', sa.BigInteger(), nullable=False),
    sa.Column('orbit_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['lightcurve_id'], ['lightcurves.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['orbit_id'], ['orbits.id'], ondelete='RESTRICT'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('bls_result_lookups',
    sa.Column('created_on', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('bls_id', sa.BigInteger(), nullable=True),
    sa.Column('best_detrending_method_id', sa.BigInteger(), nullable=True),
    sa.ForeignKeyConstraint(['best_detrending_method_id'], ['best_orbit_lightcurves.id'], ),
    sa.ForeignKeyConstraint(['bls_id'], ['bls.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('bls_id', 'best_detrending_method_id')
    )
    op.drop_index('ix_bls_lightcurve_id', table_name='bls')
    op.drop_index('ix_bls_sector', table_name='bls')
    op.drop_index('ix_bls_tce_n', table_name='bls')
    op.drop_constraint('unique_bls_runtime', 'bls', type_='unique')
    op.drop_constraint('bls_lightcurve_id_fkey', 'bls', type_='foreignkey')
    op.drop_column('bls', 'tce_n')
    op.drop_column('bls', 'sector')
    op.drop_column('bls', 'lightcurve_id')
    op.add_column('lightcurvetypes', sa.Column('id', sa.SmallInteger(), nullable=True))

def downgrade():
    op.drop_column('lightcurvetypes', 'id')
    op.add_column('bls', sa.Column('lightcurve_id', sa.BIGINT(), autoincrement=False, nullable=True))
    op.add_column('bls', sa.Column('sector', sa.INTEGER(), autoincrement=False, nullable=False))
    op.add_column('bls', sa.Column('tce_n', sa.SMALLINT(), autoincrement=False, nullable=False))
    op.create_foreign_key('bls_lightcurve_id_fkey', 'bls', 'lightcurves', ['lightcurve_id'], ['id'], onupdate='CASCADE', ondelete='CASCADE')
    op.create_unique_constraint('unique_bls_runtime', 'bls', ['lightcurve_id', 'sector', 'tce_n'])
    op.create_index('ix_bls_tce_n', 'bls', ['tce_n'], unique=False)
    op.create_index('ix_bls_sector', 'bls', ['sector'], unique=False)
    op.create_index('ix_bls_lightcurve_id', 'bls', ['lightcurve_id'], unique=False)
    op.drop_table('bls_result_lookups')
    op.drop_table('best_orbit_lightcurves')
    # ### end Alembic commands ###