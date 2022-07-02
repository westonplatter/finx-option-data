"""create stock_quotes table

Revision ID: bdc9c99204bc
Revises: 
Create Date: 2022-06-11 13:07:09.353121

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "bdc9c99204bc"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "stock_quotes",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("dt", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ticker", sa.String(50), nullable=False),
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
        sa.Column("fetched", sa.Boolean, default=False, nullable=True),
    )


def downgrade():
    op.drop_table("stock_quotes")
