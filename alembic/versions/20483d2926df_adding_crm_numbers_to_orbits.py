"""Adding crm numbers to orbits

Revision ID: 20483d2926df
Revises: ce2cea6ecd58
Create Date: 2020-02-05 11:02:16.996623

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20483d2926df'
down_revision = 'ce2cea6ecd58'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('orbits', sa.Column('crm_n', sa.Integer(), nullable=True))
    op.execute('UPDATE orbits SET crm_n = 10')
    op.alter_column('orbits', 'crm_n', nullable=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('orbits', 'crm_n')
    # ### end Alembic commands ###