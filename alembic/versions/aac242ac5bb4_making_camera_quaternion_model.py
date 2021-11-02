"""making camera_quaternion model

Revision ID: aac242ac5bb4
Revises: 93ad664ff832
Create Date: 2020-10-28 15:42:43.537263

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "aac242ac5bb4"
down_revision = "93ad664ff832"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "camera_quaternions",
        sa.Column(
            "created_on",
            sa.DateTime(),
            server_default=sa.text(u"now()"),
            nullable=True,
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("date", sa.DateTime(), nullable=False),
        sa.Column("camera", sa.Integer(), nullable=False),
        sa.Column("w", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.Column("x", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.Column("y", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.Column("z", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.CheckConstraint(
            u"camera IN (1, 2, 3, 4)", name="phys_camera_constraint"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("camera", "date"),
    )
    op.create_index(
        op.f("ix_camera_quaternions_camera"),
        "camera_quaternions",
        ["camera"],
        unique=False,
    )
    op.create_index(
        op.f("ix_camera_quaternions_date"),
        "camera_quaternions",
        ["date"],
        unique=False,
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(
        op.f("ix_camera_quaternions_date"), table_name="camera_quaternions"
    )
    op.drop_index(
        op.f("ix_camera_quaternions_camera"), table_name="camera_quaternions"
    )
    op.drop_table("camera_quaternions")
    # ### end Alembic commands ###
