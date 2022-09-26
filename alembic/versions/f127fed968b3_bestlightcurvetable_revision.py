"""bestlightcurvetable_revision

Revision ID: f127fed968b3
Revises: 3a02f50bc7aa
Create Date: 2022-09-06 09:11:46.078629

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "f127fed968b3"
down_revision = "3a02f50bc7aa"
branch_labels = None
depends_on = None
table = "best_orbit_lightcurves"


def upgrade():
    op.alter_column(table, "lightcurve_id", new_column_name="tic_id")
    op.drop_constraint(
        "best_orbit_lightcurves_lightcurve_id_fkey", table, type_="foreignkey"
    )
    op.add_column(
        table,
        sa.Column(
            "aperture_id",
            sa.SmallInteger,
            sa.ForeignKey("apertures.id", ondelete="RESTRICT"),
        ),
    )
    op.add_column(
        table,
        sa.Column(
            "lightcurve_type_id",
            sa.SmallInteger,
            sa.ForeignKey("lightcurvetypes.id", ondelete="RESTRICT"),
        ),
    )
    op.create_unique_constraint(
        "unique_best_lightcurve", table, ["tic_id", "orbit_id"]
    )


def downgrade():
    # WF: I do not see why we would rollback this change. If it comes to it
    # implement on the spot
    raise NotImplementedError
