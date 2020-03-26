"""altering bjd column

Revision ID: 08f0bdc317cf
Revises: 548de58fffac
Create Date: 2020-03-20 15:19:51.421693

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '08f0bdc317cf'
down_revision = '548de58fffac'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        table_name='lightcurve_revisions',
        column_name='barycentric_julian_date',
        type_=postgresql.ARRAY(postgresql.DOUBLE_PRECISION)
    )
    # ### end Alembic commands ###

def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        table_name='lightcurve_revisions',
        column_name='barycentric_julian_date',
        type_=postgresql.ARRAY(postgresql.DOUBLE_PRECISION)
    )
    # ### end Alembic commands ###
