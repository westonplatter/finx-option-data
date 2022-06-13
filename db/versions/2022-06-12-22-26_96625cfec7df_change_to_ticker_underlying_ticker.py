"""change to ticker/underlying_ticker

Revision ID: 96625cfec7df
Revises: 32e63e2cf4e2
Create Date: 2022-06-12 22:26:16.440590

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '96625cfec7df'
down_revision = '32e63e2cf4e2'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('option_quotes', 'ticker', nullable=False, new_column_name='underlying_ticker')
    op.alter_column('option_quotes', 'symbol', nullable=False, new_column_name='ticker')
    


def downgrade():
    op.alter_column('option_quotes', 'ticker', nullable=False, new_column_name='symbol')
    op.alter_column('option_quotes', 'underlying_ticker', nullable=False, new_column_name='ticker')
    
