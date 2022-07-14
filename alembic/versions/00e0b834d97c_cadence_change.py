"""Update Frames to use Cadences defined by seconds

Revision ID: 00e0b834d97c
Revises: efb12c5fb79c, be86fdf5cef3
Create Date: 2022-03-22 14:17:25.824888

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "00e0b834d97c"
down_revision = ("efb12c5fb79c", "be86fdf5cef3")
branch_labels = None
depends_on = None


def upgrade():
    #  Update current values
    op.execute(
        "UPDATE frames SET cadence_type = cadence_type * 60"
    )


def downgrade():
    # WARNING WILL RESULT IN LOSS OF SUB MINUTE RESOLUTION
    # DATA WILL BE LOST
    op.execute(
        "UPDATE frames SET cadence_type = cadence_type / 60"
    )