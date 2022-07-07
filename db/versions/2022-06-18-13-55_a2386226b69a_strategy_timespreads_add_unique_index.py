"""strategy_timespreads add unique index

Revision ID: a2386226b69a
Revises: 45ceec6e82b7
Create Date: 2022-06-18 13:55:45.031176

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a2386226b69a'
down_revision = '45ceec6e82b7'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        op.f('ix_strategy_timespreads_dt_desc_ticker_f_ticker_b'),
        'strategy_timespreads',
        ["dt", "desc", "ticker_f", "ticker_b"],
        unique=True
    )


def downgrade():
    op.drop_index('ix_strategy_timespreads_dt_desc_ticker_f_ticker_b')
