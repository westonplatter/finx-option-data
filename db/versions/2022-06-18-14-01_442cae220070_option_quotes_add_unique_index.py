"""option_quotes add unique index

Revision ID: 442cae220070
Revises: a2386226b69a
Create Date: 2022-06-18 14:01:27.636466

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '442cae220070'
down_revision = 'a2386226b69a'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        op.f(
            'ix_option_quotes_dt_underlying_ticker_exp_date_option_type_strike'),
            'option_quotes', [
                "dt", "underlying_ticker", "exp_date", "option_type", "strike"
            ],
        unique=True
    )
    

def downgrade():
    op.drop_index("ix_option_quotes_dt_underlying_ticker_exp_date_option_type_strike")
