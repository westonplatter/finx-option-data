"""option_quotes add scale

Revision ID: 1e7e52da4a34
Revises: 442cae220070
Create Date: 2022-06-25 13:55:51.462336

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1e7e52da4a34'
down_revision = '442cae220070'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('ALTER TABLE option_quotes ALTER COLUMN iv TYPE numeric(12,4)')
    

def downgrade():
    op.execute('ALTER TABLE option_quotes ALTER COLUMN iv TYPE numeric(8,2)')
