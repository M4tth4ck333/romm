"""update to 1.8.1

Revision ID: 1.8.1
Revises: 1.8
Create Date: 2023-04-17 12:03:19.163501

"""

import sqlalchemy as sa
from alembic import op

from utils.database import CustomJSON, is_postgresql

# revision identifiers, used by Alembic.
revision = "1.8.1"
down_revision = "1.8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()

    json_array_build_func = (
        "jsonb_build_array()" if is_postgresql(connection) else "JSON_ARRAY()"
    )

    with op.batch_alter_table("roms") as batch_op:
        batch_op.add_column(
            sa.Column(
                "url_screenshots",
                CustomJSON(),
                nullable=False,
                server_default=sa.text(f"({json_array_build_func})"),
            ),
            if_not_exists=True,
        )
        batch_op.add_column(
            sa.Column(
                "path_screenshots",
                CustomJSON(),
                nullable=False,
                server_default=sa.text(f"({json_array_build_func})"),
            ),
            if_not_exists=True,
        )


def downgrade() -> None:
    with op.batch_alter_table("roms") as batch_op:
        batch_op.drop_column("url_screenshots", if_exists=True)
        batch_op.drop_column("path_screenshots", if_exists=True)
