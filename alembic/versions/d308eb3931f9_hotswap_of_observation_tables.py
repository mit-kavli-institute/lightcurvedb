"""hotswap of observation tables

Revision ID: d308eb3931f9
Revises: eee1e61ac19b
Create Date: 2021-02-23 22:31:56.299945

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd308eb3931f9'
down_revision = 'eee1e61ac19b'
branch_labels = None
depends_on = None

LEGACY_OBS_TABLENAME = "observations"
NEW_OBS_TABLENAME = "complete_observations"
SAFETY_TABLE = "SAFETY_TABLE"


def upgrade():
    op.rename_table(LEGACY_OBS_TABLENAME, SAFETY_TABLE)
    op.rename_table(NEW_OBS_TABLENAME, LEGACY_OBS_TABLENAME)


def downgrade():
    op.rename_table(LEGACY_OBS_TABLENAME, NEW_OBS_TABLENAME)
    op.rename_table(SAFETY_TABLE, LEGACY_OBS_TABLENAME)
