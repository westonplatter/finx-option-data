"""option_quotes add weekday column

Revision ID: 3098c2bcdbcc
Revises: 1e7e52da4a34
Create Date: 2022-07-01 20:40:46.004561

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3098c2bcdbcc'
down_revision = '1e7e52da4a34'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("option_quotes", sa.Column("weekday", sa.Integer(), nullable=True))


def downgrade():
    op.drop_column("option_quotes", "weekday")
