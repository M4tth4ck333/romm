"""Fix age_ratings extraction in roms_metadata view for MySQL/MariaDB

MySQL/MariaDB returns a scalar instead of a single-element array when using
JSON_EXTRACT with a [*] wildcard path on an array with a single element.
This migration wraps the extracted value in JSON_ARRAY when it's not already
an array, ensuring consistent list[str] output.

Revision ID: 0073_fix_age_ratings_array
Revises: 0072_client_tokens
Create Date: 2026-04-05 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

from utils.database import is_postgresql

# revision identifiers, used by Alembic.
revision = "0073_fix_age_ratings_array"
down_revision = "0072_client_tokens"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    if not is_postgresql(connection):
        connection.execute(sa.text("""
                CREATE OR REPLACE VIEW roms_metadata AS
                    SELECT
                        r.id as rom_id,
                        NOW() AS created_at,
                        NOW() AS updated_at,
                        COALESCE(
                            JSON_EXTRACT(r.manual_metadata, '$.genres'),
                            JSON_EXTRACT(r.igdb_metadata, '$.genres'),
                            JSON_EXTRACT(r.moby_metadata, '$.genres'),
                            JSON_EXTRACT(r.ss_metadata, '$.genres'),
                            JSON_EXTRACT(r.launchbox_metadata, '$.genres'),
                            JSON_EXTRACT(r.ra_metadata, '$.genres'),
                            JSON_EXTRACT(r.flashpoint_metadata, '$.genres'),
                            JSON_EXTRACT(r.gamelist_metadata, '$.genres'),
                            JSON_ARRAY()
                        ) AS genres,

                        COALESCE(
                            JSON_EXTRACT(r.manual_metadata, '$.franchises'),
                            JSON_EXTRACT(r.igdb_metadata, '$.franchises'),
                            JSON_EXTRACT(r.ss_metadata, '$.franchises'),
                            JSON_EXTRACT(r.flashpoint_metadata, '$.franchises'),
                            JSON_EXTRACT(r.gamelist_metadata, '$.franchises'),
                            JSON_ARRAY()
                        ) AS franchises,

                        COALESCE(
                            JSON_EXTRACT(r.igdb_metadata, '$.collections'),
                            JSON_ARRAY()
                        ) AS collections,

                        COALESCE(
                            JSON_EXTRACT(r.manual_metadata, '$.companies'),
                            JSON_EXTRACT(r.igdb_metadata, '$.companies'),
                            JSON_EXTRACT(r.ss_metadata, '$.companies'),
                            JSON_EXTRACT(r.ra_metadata, '$.companies'),
                            JSON_EXTRACT(r.launchbox_metadata, '$.companies'),
                            JSON_EXTRACT(r.flashpoint_metadata, '$.companies'),
                            JSON_EXTRACT(r.gamelist_metadata, '$.companies'),
                            JSON_ARRAY()
                        ) AS companies,

                        COALESCE(
                            JSON_EXTRACT(r.manual_metadata, '$.game_modes'),
                            JSON_EXTRACT(r.igdb_metadata, '$.game_modes'),
                            JSON_EXTRACT(r.ss_metadata, '$.game_modes'),
                            JSON_EXTRACT(r.flashpoint_metadata, '$.game_modes'),
                            JSON_ARRAY()
                        ) AS game_modes,

                        COALESCE(
                            JSON_EXTRACT(r.manual_metadata, '$.age_ratings'),
                            CASE
                                WHEN JSON_CONTAINS_PATH(r.igdb_metadata, 'one', '$.age_ratings')
                                    AND JSON_LENGTH(JSON_EXTRACT(r.igdb_metadata, '$.age_ratings')) > 0
                                THEN
                                    IF(
                                        JSON_TYPE(JSON_EXTRACT(r.igdb_metadata, '$.age_ratings[*].rating')) = 'ARRAY',
                                        JSON_EXTRACT(r.igdb_metadata, '$.age_ratings[*].rating'),
                                        JSON_ARRAY(JSON_UNQUOTE(JSON_EXTRACT(r.igdb_metadata, '$.age_ratings[*].rating')))
                                    )
                                ELSE
                                    NULL
                            END,
                            CASE
                                WHEN JSON_CONTAINS_PATH(r.ss_metadata, 'one', '$.age_ratings')
                                    AND JSON_LENGTH(JSON_EXTRACT(r.ss_metadata, '$.age_ratings')) > 0
                                THEN
                                    IF(
                                        JSON_TYPE(JSON_EXTRACT(r.ss_metadata, '$.age_ratings[*].rating')) = 'ARRAY',
                                        JSON_EXTRACT(r.ss_metadata, '$.age_ratings[*].rating'),
                                        JSON_ARRAY(JSON_UNQUOTE(JSON_EXTRACT(r.ss_metadata, '$.age_ratings[*].rating')))
                                    )
                                ELSE
                                    NULL
                            END,
                            CASE
                                WHEN JSON_CONTAINS_PATH(r.launchbox_metadata, 'one', '$.esrb')
                                    AND JSON_EXTRACT(r.launchbox_metadata, '$.esrb') IS NOT NULL
                                    AND JSON_EXTRACT(r.launchbox_metadata, '$.esrb') != ''
                                THEN
                                    JSON_ARRAY(JSON_EXTRACT(r.launchbox_metadata, '$.esrb'))
                                ELSE
                                    NULL
                            END,
                            JSON_ARRAY()
                        ) AS age_ratings,

                        CASE
                            WHEN JSON_CONTAINS_PATH(r.manual_metadata, 'one', '$.first_release_date') AND
                                JSON_UNQUOTE(JSON_EXTRACT(r.manual_metadata, '$.first_release_date')) NOT IN ('null', 'None', '0', '0.0') AND
                                JSON_UNQUOTE(JSON_EXTRACT(r.manual_metadata, '$.first_release_date')) REGEXP '^[0-9]+$'
                            THEN CAST(JSON_EXTRACT(r.manual_metadata, '$.first_release_date') AS SIGNED)

                            WHEN JSON_CONTAINS_PATH(r.igdb_metadata, 'one', '$.first_release_date') AND
                                JSON_UNQUOTE(JSON_EXTRACT(r.igdb_metadata, '$.first_release_date')) NOT IN ('null', 'None', '0', '0.0') AND
                                JSON_UNQUOTE(JSON_EXTRACT(r.igdb_metadata, '$.first_release_date')) REGEXP '^[0-9]+$'
                            THEN CAST(JSON_EXTRACT(r.igdb_metadata, '$.first_release_date') AS SIGNED) * 1000

                            WHEN JSON_CONTAINS_PATH(r.ss_metadata, 'one', '$.first_release_date') AND
                                JSON_UNQUOTE(JSON_EXTRACT(r.ss_metadata, '$.first_release_date')) NOT IN ('null', 'None', '0', '0.0') AND
                                JSON_UNQUOTE(JSON_EXTRACT(r.ss_metadata, '$.first_release_date')) REGEXP '^[0-9]+$'
                            THEN CAST(JSON_EXTRACT(r.ss_metadata, '$.first_release_date') AS SIGNED) * 1000

                            WHEN JSON_CONTAINS_PATH(r.ra_metadata, 'one', '$.first_release_date') AND
                                JSON_UNQUOTE(JSON_EXTRACT(r.ra_metadata, '$.first_release_date')) NOT IN ('null', 'None', '0', '0.0') AND
                                JSON_UNQUOTE(JSON_EXTRACT(r.ra_metadata, '$.first_release_date')) REGEXP '^[0-9]+$'
                            THEN CAST(JSON_EXTRACT(r.ra_metadata, '$.first_release_date') AS SIGNED) * 1000

                            WHEN JSON_CONTAINS_PATH(r.launchbox_metadata, 'one', '$.first_release_date') AND
                                JSON_UNQUOTE(JSON_EXTRACT(r.launchbox_metadata, '$.first_release_date')) NOT IN ('null', 'None', '0', '0.0') AND
                                JSON_UNQUOTE(JSON_EXTRACT(r.launchbox_metadata, '$.first_release_date')) REGEXP '^[0-9]+$'
                            THEN CAST(JSON_EXTRACT(r.launchbox_metadata, '$.first_release_date') AS SIGNED) * 1000

                            WHEN JSON_CONTAINS_PATH(r.flashpoint_metadata, 'one', '$.first_release_date') AND
                                JSON_UNQUOTE(JSON_EXTRACT(r.flashpoint_metadata, '$.first_release_date')) NOT IN ('null', 'None', '0', '0.0') AND
                                JSON_UNQUOTE(JSON_EXTRACT(r.flashpoint_metadata, '$.first_release_date')) REGEXP '^[0-9]+$'
                            THEN CAST(JSON_EXTRACT(r.flashpoint_metadata, '$.first_release_date') AS SIGNED) * 1000

                            WHEN JSON_CONTAINS_PATH(r.gamelist_metadata, 'one', '$.first_release_date')
                                AND JSON_UNQUOTE(JSON_EXTRACT(r.gamelist_metadata, '$.first_release_date')) NOT IN ('null', 'None', '0', '0.0')
                                AND JSON_UNQUOTE(JSON_EXTRACT(r.gamelist_metadata, '$.first_release_date')) REGEXP '^[0-9]{8}T[0-9]{6}$'
                            THEN UNIX_TIMESTAMP(
                                STR_TO_DATE(
                                JSON_UNQUOTE(JSON_EXTRACT(r.gamelist_metadata, '$.first_release_date')),
                                '%Y%m%dT%H%i%S'
                                )
                            ) * 1000

                            ELSE NULL
                        END AS first_release_date,

                        CASE
                            WHEN (igdb_rating IS NOT NULL OR moby_rating IS NOT NULL OR ss_rating IS NOT NULL OR launchbox_rating IS NOT NULL OR gamelist_rating IS NOT NULL) THEN
                                (COALESCE(igdb_rating, 0) + COALESCE(moby_rating, 0) + COALESCE(ss_rating, 0) + COALESCE(launchbox_rating, 0) + COALESCE(gamelist_rating, 0)) /
                                (CASE WHEN igdb_rating IS NOT NULL THEN 1 ELSE 0 END +
                                CASE WHEN moby_rating IS NOT NULL THEN 1 ELSE 0 END +
                                CASE WHEN ss_rating IS NOT NULL THEN 1 ELSE 0 END +
                                CASE WHEN launchbox_rating IS NOT NULL THEN 1 ELSE 0 END +
                                CASE WHEN gamelist_rating IS NOT NULL THEN 1 ELSE 0 END)
                            ELSE NULL
                        END AS average_rating,

                        COALESCE(
                            NULLIF(JSON_UNQUOTE(JSON_EXTRACT(r.manual_metadata, '$.player_count')), '1'),
                            NULLIF(JSON_UNQUOTE(JSON_EXTRACT(r.ss_metadata, '$.player_count')), '1'),
                            NULLIF(JSON_UNQUOTE(JSON_EXTRACT(r.igdb_metadata, '$.player_count')), '1'),
                            NULLIF(JSON_UNQUOTE(JSON_EXTRACT(r.gamelist_metadata, '$.player_count')), '1'),
                            '1'
                        ) AS player_count
                    FROM (
                        SELECT
                            id,
                            manual_metadata,
                            igdb_metadata,
                            moby_metadata,
                            ss_metadata,
                            ra_metadata,
                            launchbox_metadata,
                            flashpoint_metadata,
                            gamelist_metadata,
                            CASE
                                WHEN JSON_CONTAINS_PATH(igdb_metadata, 'one', '$.total_rating') AND
                                    JSON_UNQUOTE(JSON_EXTRACT(igdb_metadata, '$.total_rating')) NOT IN ('null', 'None', '0', '0.0') AND
                                    JSON_UNQUOTE(JSON_EXTRACT(igdb_metadata, '$.total_rating')) REGEXP '^[0-9]+(\\\\.[0-9]+)?$'
                                THEN CAST(JSON_EXTRACT(igdb_metadata, '$.total_rating') AS DECIMAL(10,2))
                                ELSE NULL
                            END AS igdb_rating,
                            CASE
                                WHEN JSON_CONTAINS_PATH(moby_metadata, 'one', '$.moby_score') AND
                                    JSON_UNQUOTE(JSON_EXTRACT(moby_metadata, '$.moby_score')) NOT IN ('null', 'None', '0', '0.0') AND
                                    JSON_UNQUOTE(JSON_EXTRACT(moby_metadata, '$.moby_score')) REGEXP '^[0-9]+(\\\\.[0-9]+)?$'
                                THEN CAST(JSON_EXTRACT(moby_metadata, '$.moby_score') AS DECIMAL(10,2)) * 10
                                ELSE NULL
                            END AS moby_rating,
                            CASE
                                WHEN JSON_CONTAINS_PATH(ss_metadata, 'one', '$.ss_score') AND
                                    JSON_UNQUOTE(JSON_EXTRACT(ss_metadata, '$.ss_score')) NOT IN ('null', 'None', '0', '0.0') AND
                                    JSON_UNQUOTE(JSON_EXTRACT(ss_metadata, '$.ss_score')) REGEXP '^[0-9]+(\\\\.[0-9]+)?$'
                                THEN CAST(JSON_EXTRACT(ss_metadata, '$.ss_score') AS DECIMAL(10,2)) * 10
                                ELSE NULL
                            END AS ss_rating,
                            CASE
                                WHEN JSON_CONTAINS_PATH(launchbox_metadata, 'one', '$.community_rating') AND
                                    JSON_UNQUOTE(JSON_EXTRACT(launchbox_metadata, '$.community_rating')) NOT IN ('null', 'None', '0', '0.0') AND
                                    JSON_UNQUOTE(JSON_EXTRACT(launchbox_metadata, '$.community_rating')) REGEXP '^[0-9]+(\\\\.[0-9]+)?$'
                                THEN CAST(JSON_EXTRACT(launchbox_metadata, '$.community_rating') AS DECIMAL(10,2)) * 20
                                ELSE NULL
                            END AS launchbox_rating,
                            CASE
                                WHEN JSON_CONTAINS_PATH(gamelist_metadata, 'one', '$.rating') AND
                                    JSON_UNQUOTE(JSON_EXTRACT(gamelist_metadata, '$.rating')) NOT IN ('null', 'None', '0', '0.0') AND
                                    JSON_UNQUOTE(JSON_EXTRACT(gamelist_metadata, '$.rating')) REGEXP '^[0-9]+(\\\\.[0-9]+)?$'
                                THEN CAST(JSON_EXTRACT(gamelist_metadata, '$.rating') AS DECIMAL(10,2)) * 100
                                ELSE NULL
                            END AS gamelist_rating
                        FROM roms
                    ) AS r;
                """))


def downgrade():
    op.execute("DROP VIEW IF EXISTS roms_metadata")
