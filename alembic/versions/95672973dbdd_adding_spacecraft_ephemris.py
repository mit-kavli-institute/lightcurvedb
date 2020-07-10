"""adding spacecraft ephemris

Revision ID: 95672973dbdd
Revises: 2ffa104367ee
Create Date: 2020-06-25 13:40:18.586935

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '95672973dbdd'
down_revision = '2ffa104367ee'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
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
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_spacecraftephemeris_calendar_date'), table_name='spacecraftephemeris')
    op.drop_table('spacecraftephemeris')
    # ### end Alembic commands ###
