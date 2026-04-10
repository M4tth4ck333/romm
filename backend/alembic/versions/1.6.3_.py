"""update to 1.6.3

Revision ID: 1.6.3
Revises: 1.6.2
Create Date: 2023-04-10 23:13:43.591414

"""

import sqlalchemy as sa
from alembic import op

from utils.database import CustomJSON

# revision identifiers, used by Alembic.
revision = "1.6.3"
down_revision = "1.6.2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("roms") as batch_op:
        batch_op.add_column(
            sa.Column("multi", sa.Boolean(), nullable=True), if_not_exists=True
        )
        batch_op.add_column(
            sa.Column("files", CustomJSON(), nullable=True), if_not_exists=True
        )


def downgrade() -> None:
    with op.batch_alter_table("roms") as batch_op:
        batch_op.drop_column("files", if_exists=True)
        batch_op.drop_column("multi", if_exists=True)
