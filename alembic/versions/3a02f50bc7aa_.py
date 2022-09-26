"""empty message

Revision ID: 3a02f50bc7aa
Revises: c4daf006804e
Create Date: 2022-08-10 10:26:57.860618

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "3a02f50bc7aa"
down_revision = "c4daf006804e"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION timescaledb")
    op.execute(
        "SELECT create_hypertable("
        "'hyper_lightpoints', "
        "'lightcurve_id', "
        "chunk_time_interval => 1000000)"
    )


def downgrade():
    pass
