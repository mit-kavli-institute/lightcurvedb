"""Adding not null constraints on frame foreign keys to orbit and frame type

Revision ID: 75cab39984bb
Revises: 20483d2926df
Create Date: 2020-02-05 12:52:30.414169

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '75cab39984bb'
down_revision = '20483d2926df'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('frames', 'frame_type_id',
               existing_type=sa.BIGINT(),
               nullable=False)
    op.alter_column('frames', 'orbit_id',
               existing_type=sa.INTEGER(),
               nullable=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('frames', 'orbit_id',
               existing_type=sa.INTEGER(),
               nullable=True)
    op.alter_column('frames', 'frame_type_id',
               existing_type=sa.BIGINT(),
               nullable=True)
    # ### end Alembic commands ###
