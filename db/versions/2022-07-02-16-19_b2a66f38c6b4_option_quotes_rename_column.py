"""option_quotes rename column

Revision ID: b2a66f38c6b4
Revises: 3098c2bcdbcc
Create Date: 2022-07-02 16:19:19.081170

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b2a66f38c6b4"
down_revision = "3098c2bcdbcc"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column("option_quotes", "weekday", new_column_name="exp_date_weekday")


def downgrade():
    op.alter_column("option_quotes", "exp_date_weekday", new_column_name="weekday")
