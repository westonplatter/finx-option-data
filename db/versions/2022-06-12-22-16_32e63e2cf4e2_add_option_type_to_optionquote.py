"""add option_type to OptionQuote

Revision ID: 32e63e2cf4e2
Revises: 2c88c92b9b7a
Create Date: 2022-06-12 22:16:55.793362

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '32e63e2cf4e2'
down_revision = '2c88c92b9b7a'
branch_labels = None
depends_on = None


def upgrade():
    # option_type = Column(String(10))
    op.add_column('option_quotes', sa.Column('option_type', sa.String(10)))


def downgrade():
    op.remove_column('option_quotes', 'option_type')
