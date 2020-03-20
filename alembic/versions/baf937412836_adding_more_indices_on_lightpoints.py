"""Adding more indices on lightpoints

Revision ID: baf937412836
Revises: 90c9fd1a4c11
Create Date: 2020-02-17 15:59:09.469792

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'baf937412836'
down_revision = '90c9fd1a4c11'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index(op.f('ix_lightpoints_cadence'), 'lightpoints', ['cadence'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_lightpoints_cadence'), table_name='lightpoints')
    # ### end Alembic commands ###