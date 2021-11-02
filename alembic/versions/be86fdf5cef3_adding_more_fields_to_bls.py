"""adding more fields to bls

Revision ID: be86fdf5cef3
Revises: 85a99e8a16dd
Create Date: 2021-10-26 13:42:43.629950

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "be86fdf5cef3"
down_revision = "85a99e8a16dd"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("bls", sa.Column("tic_id", sa.BigInteger(), nullable=True))
    op.create_index(op.f("ix_bls_tic_id"), "bls", ["tic_id"], unique=False)


def downgrade():
    op.drop_index(op.f("ix_bls_tic_id"), table_name="bls")
    op.drop_column("bls", "tic_id")
    # ### end Alembic commands ###
