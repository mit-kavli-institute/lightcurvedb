"""Initializing TimescaleDB

Revision ID: db7da31008c0
Revises:
Create Date: 2022-11-17 16:30:18.472173

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "db7da31008c0"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")


def downgrade():
    op.execute("DROP EXTENSION timescaledb")
