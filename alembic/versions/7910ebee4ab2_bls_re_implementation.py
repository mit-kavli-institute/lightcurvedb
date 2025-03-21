"""BLS re-implementation

Revision ID: 7910ebee4ab2
Revises: c6e1baa7ce54
Create Date: 2022-12-13 11:33:57.385160

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "7910ebee4ab2"
down_revision = "c6e1baa7ce54"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "bls",
        sa.Column(
            "created_on",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("sector", sa.SmallInteger(), nullable=True),
        sa.Column("tic_id", sa.BigInteger(), nullable=True),
        sa.Column("tce_n", sa.SmallInteger(), nullable=False),
        sa.Column(
            "transit_period", postgresql.DOUBLE_PRECISION(), nullable=False
        ),
        sa.Column(
            "transit_depth", postgresql.DOUBLE_PRECISION(), nullable=False
        ),
        sa.Column(
            "transit_duration", postgresql.DOUBLE_PRECISION(), nullable=False
        ),
        sa.Column(
            "planet_radius", postgresql.DOUBLE_PRECISION(), nullable=False
        ),
        sa.Column(
            "planet_radius_error",
            postgresql.DOUBLE_PRECISION(),
            nullable=False,
        ),
        sa.Column("points_pre_transit", sa.Integer(), nullable=False),
        sa.Column("points_in_transit", sa.Integer(), nullable=False),
        sa.Column("points_post_transit", sa.Integer(), nullable=False),
        sa.Column(
            "out_of_transit_magnitude",
            postgresql.DOUBLE_PRECISION(),
            nullable=False,
        ),
        sa.Column("transits", sa.Integer(), nullable=False),
        sa.Column("ingress", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.Column(
            "transit_center", postgresql.DOUBLE_PRECISION(), nullable=False
        ),
        sa.Column("rednoise", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.Column("whitenoise", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.Column(
            "signal_to_noise", postgresql.DOUBLE_PRECISION(), nullable=False
        ),
        sa.Column(
            "signal_to_pinknoise",
            postgresql.DOUBLE_PRECISION(),
            nullable=False,
        ),
        sa.Column(
            "signal_detection_efficiency",
            postgresql.DOUBLE_PRECISION(),
            nullable=False,
        ),
        sa.Column(
            "signal_residual", postgresql.DOUBLE_PRECISION(), nullable=False
        ),
        sa.Column(
            "zero_point_transit", postgresql.DOUBLE_PRECISION(), nullable=False
        ),
        sa.Column(
            "additional_data",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "bls_additional_data_idx",
        "bls",
        ["additional_data"],
        unique=False,
        postgresql_using="gin",
    )
    op.create_index(
        op.f("ix_bls_planet_radius"), "bls", ["planet_radius"], unique=False
    )
    op.create_index(op.f("ix_bls_sector"), "bls", ["sector"], unique=False)
    op.create_index(
        op.f("ix_bls_signal_to_noise"),
        "bls",
        ["signal_to_noise"],
        unique=False,
    )
    op.create_index(op.f("ix_bls_tce_n"), "bls", ["tce_n"], unique=False)
    op.create_index(op.f("ix_bls_tic_id"), "bls", ["tic_id"], unique=False)
    op.create_index(
        op.f("ix_bls_transit_period"), "bls", ["transit_period"], unique=False
    )
    op.create_index(op.f("ix_bls_transits"), "bls", ["transits"], unique=False)
    op.create_table(
        "bls_tags",
        sa.Column(
            "created_on",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column("name", sa.String(length=64), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "bls_association_table",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("bls", sa.BigInteger(), nullable=True),
        sa.Column("tag", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["bls"],
            ["bls.id"],
        ),
        sa.ForeignKeyConstraint(
            ["tag"],
            ["bls_tags.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_unique_constraint(None, "apertures", ["id"])
    # op.drop_index(
    #     "array_orbit_lightcurves_tic_id_idx",
    #     table_name="array_orbit_lightcurves",
    # )
    op.create_unique_constraint(None, "frametypes", ["id"])
    op.create_unique_constraint(None, "lightcurvetypes", ["id"])
    op.create_unique_constraint(None, "qlpstages", ["name"])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, "qlpstages", type_="unique")
    op.drop_constraint(None, "lightcurvetypes", type_="unique")
    op.drop_constraint(None, "frametypes", type_="unique")
    # op.create_index(
    #     "array_orbit_lightcurves_tic_id_idx",
    #     "array_orbit_lightcurves",
    #     ["tic_id"],
    #     unique=False,
    # )
    op.drop_constraint(None, "apertures", type_="unique")
    op.drop_table("bls_association_table")
    op.drop_table("bls_tags")
    op.drop_index(op.f("ix_bls_transits"), table_name="bls")
    op.drop_index(op.f("ix_bls_transit_period"), table_name="bls")
    op.drop_index(op.f("ix_bls_tic_id"), table_name="bls")
    op.drop_index(op.f("ix_bls_tce_n"), table_name="bls")
    op.drop_index(op.f("ix_bls_signal_to_noise"), table_name="bls")
    op.drop_index(op.f("ix_bls_sector"), table_name="bls")
    op.drop_index(op.f("ix_bls_planet_radius"), table_name="bls")
    op.drop_index("bls_additional_data_idx", table_name="bls")
    op.drop_table("bls")
    # ### end Alembic commands ###
