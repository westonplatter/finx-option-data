"""create option_quotes table

Revision ID: 2c88c92b9b7a
Revises: bdc9c99204bc
Create Date: 2022-06-11 13:46:48.356844

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "2c88c92b9b7a"
down_revision = "bdc9c99204bc"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "option_quotes",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("dt", sa.DateTime(timezone=True), nullable=False),
        sa.Column("symbol", sa.String(50), nullable=False),
        sa.Column(
            "open",
            sa.Numeric(precision=8, scale=2, decimal_return_scale=None, asdecimal=True),
            nullable=True,
        ),
        sa.Column(
            "high",
            sa.Numeric(precision=8, scale=2, decimal_return_scale=None, asdecimal=True),
            nullable=True,
        ),
        sa.Column(
            "low",
            sa.Numeric(precision=8, scale=2, decimal_return_scale=None, asdecimal=True),
            nullable=True,
        ),
        sa.Column(
            "close",
            sa.Numeric(precision=8, scale=2, decimal_return_scale=None, asdecimal=True),
            nullable=True,
        ),
        sa.Column("volume", sa.Integer, nullable=True),
        sa.Column(
            "pre_market",
            sa.Numeric(precision=8, scale=2, decimal_return_scale=None, asdecimal=True),
            nullable=True,
        ),
        sa.Column(
            "after_market",
            sa.Numeric(precision=8, scale=2, decimal_return_scale=None, asdecimal=True),
            nullable=True,
        ),
        sa.Column("fetched", sa.Boolean, default=False, nullable=True),
        sa.Column("exp_date", sa.Date, nullable=True),
        sa.Column("dte", sa.Integer, nullable=True),
        sa.Column(
            "strike",
            sa.Numeric(precision=8, scale=2, decimal_return_scale=None, asdecimal=True),
            nullable=True,
        ),
        sa.Column("ticker", sa.String(50), nullable=True),
        sa.Column(
            "delta",
            sa.Numeric(precision=8, scale=2, decimal_return_scale=None, asdecimal=True),
            nullable=True,
        ),
        sa.Column(
            "theta",
            sa.Numeric(precision=8, scale=2, decimal_return_scale=None, asdecimal=True),
            nullable=True,
        ),
        sa.Column(
            "vega",
            sa.Numeric(precision=8, scale=2, decimal_return_scale=None, asdecimal=True),
            nullable=True,
        ),
        sa.Column(
            "gamma",
            sa.Numeric(precision=8, scale=2, decimal_return_scale=None, asdecimal=True),
            nullable=True,
        ),
        sa.Column(
            "iv",
            sa.Numeric(precision=8, scale=2, decimal_return_scale=None, asdecimal=True),
            nullable=True,
        ),
    )


def downgrade():
    op.drop_table("option_quotes")
