"""add diagonal_strategies_table

Revision ID: 45ceec6e82b7
Revises: 96625cfec7df
Create Date: 2022-06-18 13:35:57.684650

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '45ceec6e82b7'
down_revision = '96625cfec7df'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "strategy_timespreads",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("dt", sa.DateTime(timezone=True), nullable=False),
        sa.Column("desc", sa.String(50), nullable=False),
        sa.Column("id_f", sa.Integer, nullable=False),
        sa.Column("id_b", sa.Integer, nullable=False),
        sa.Column("ticker_f", sa.String(50), nullable=False),
        sa.Column("ticker_b", sa.String(50), nullable=False),
    )

    
def downgrade():
    op.drop_table("strategy_timespreads")
