import json
from enum import Enum
from typing import NotRequired, TypedDict

import httpx
import yarl
from fastapi import HTTPException, status

from config import PLAYMATCH_API_ENABLED
from handler.metadata.base_handler import MetadataHandler
from logger.logger import log
from models.rom import Rom, RomFile
from utils import get_version
from utils.context import ctx_httpx_client


class PlaymatchProvider(str, Enum):
    IGDB = "IGDB"


# (rom attribute, provider tag). Playmatch drops unknown tags server-side.
_PLAYMATCH_PROVIDER_TAGS: tuple[tuple[str, str], ...] = (
    ("igdb_id", "IGDB"),
    ("moby_id", "MOBY_GAMES"),
    ("ss_id", "SCREENSCRAPER"),
    ("ra_id", "RETRO_ACHIEVEMENTS"),
    ("launchbox_id", "LAUNCHBOX"),
    ("hasheous_id", "HASHEOUS"),
    ("tgdb_id", "TGDB"),
    ("flashpoint_id", "FLASHPOINT"),
    ("hltb_id", "HOWLONGTOBEAT"),
    ("libretro_id", "LIBRETRO"),
    ("sgdb_id", "STEAMGRIDDB"),
    ("gamelist_id", "GAMELIST"),
)


class GameMatchType(str, Enum):
    SHA256 = "SHA256"
    SHA1 = "SHA1"
    MD5 = "MD5"
    FileNameAndSize = "FileNameAndSize"
    NoMatch = "NoMatch"


class PlaymatchExternalMetadata(TypedDict):
    automaticMatchReason: NotRequired[str]
    comment: NotRequired[str]
    failedMatchReason: NotRequired[str]
    manualMatchType: NotRequired[str]
    matchType: NotRequired[str]
    providerId: NotRequired[str]
    providerName: NotRequired[str]


class PlaymatchRomMatch(TypedDict):
    igdb_id: int | None


class PlaymatchHandler(MetadataHandler):
    """
    Handler for [Playmatch](https://github.com/RetroRealm/playmatch), a service for matching ROMs by Hashes.
    """

    def __init__(self):
        self.base_url = "https://playmatch.retrorealm.dev/api"
        self.identify_url = f"{self.base_url}/identify/ids"
        self.healthcheck_url = f"{self.base_url}/health"
        self.suggestion_url = f"{self.base_url}/suggestion/external/game"

    @classmethod
    def is_enabled(cls) -> bool:
        return PLAYMATCH_API_ENABLED

    async def heartbeat(self) -> bool:
        if not self.is_enabled():
            return False

        try:
            response = await self._request(self.healthcheck_url, {})
        except Exception as e:
            log.error("Error checking Playmatch API: %s", e)
            return False

        return bool(response)

    async def _request(self, url: str, query: dict) -> dict:
        """
        Sends a Request to Playmatch API.

        :param url: The API endpoint URL.
        :param query: A dictionary containing the query parameters.
        :return: A dictionary with the json result.
        :raises HTTPException: If the request fails or the service is unavailable.
        """
        httpx_client = ctx_httpx_client.get()

        filtered_query = {
            key: value
            for key, value in query.items()
            if value is not None and value != ""  # drop None and ""
        }

        url_with_query = yarl.URL(url).update_query(**filtered_query)

        log.debug(
            "API request: URL=%s, Timeout=%s",
            url_with_query,
            60,
        )

        headers = {"user-agent": f"RomM/{get_version()}"}

        try:
            res = await httpx_client.get(
                str(url_with_query), headers=headers, timeout=60
            )
            res.raise_for_status()
            return res.json()
        except (httpx.HTTPStatusError, httpx.ConnectError, httpx.ReadTimeout) as exc:
            log.warning("Connection error: can't connect to Playmatch", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Can't connect to Playmatch, check your internet connection",
            ) from exc
        except json.JSONDecodeError as exc:
            log.error("Error decoding JSON response from ScreenScraper: %s", exc)
            return {}

    async def lookup_rom(self, files: list[RomFile]) -> PlaymatchRomMatch:
        """
        Identify a ROM file using Playmatch API.

        :param rom_attrs: A dictionary containing the ROM attributes.
        :return: A PlaymatchRomMatch objects containing the matched ROM information.
        :raises HTTPException: If the request fails or the service is unavailable.
        """
        if not self.is_enabled():
            return PlaymatchRomMatch(igdb_id=None)

        first_file = next(
            (file for file in files if file.file_size_bytes > 0),
            None,
        )
        if first_file is None:
            return PlaymatchRomMatch(igdb_id=None)

        try:
            response = await self._request(
                self.identify_url,
                {
                    "fileName": first_file.file_name,
                    "fileSize": first_file.file_size_bytes,
                    "md5": first_file.md5_hash,
                    "sha1": first_file.sha1_hash,
                },
            )
        except httpx.HTTPStatusError:
            # We silently fail if the service is unavailable as this should not block the rest of RomM.
            return PlaymatchRomMatch(igdb_id=None)

        game_match_type = response.get("gameMatchType", None)
        if game_match_type == GameMatchType.NoMatch:
            log.debug("No match found for the provided ROM file.")
            return PlaymatchRomMatch(igdb_id=None)

        externalMetadata = response.get("externalMetadata", [])
        if len(externalMetadata) == 0:
            log.debug("No external metadata found for the matched ROM file.")
            return PlaymatchRomMatch(igdb_id=None)

        igdb_id = None

        for metadata in externalMetadata:
            provider_name = metadata.get("providerName", None)
            provider_game_id = metadata.get("providerId", None)
            if provider_name == PlaymatchProvider.IGDB and provider_game_id is not None:
                log.debug(
                    "Playmatch found IGDB match with IGDB ID: %s", provider_game_id
                )
                igdb_id = int(provider_game_id)

        return PlaymatchRomMatch(igdb_id=igdb_id)

    async def submit_manual_match_suggestion(self, rom: Rom) -> None:
        """Fire-and-forget suggestion POST. No-ops if disabled or no provider IDs are set; never raises."""
        try:
            if not self.is_enabled():
                return

            mappings = [
                {"provider": tag, "providerId": str(getattr(rom, attr))}
                for attr, tag in _PLAYMATCH_PROVIDER_TAGS
                if getattr(rom, attr, None)
            ]
            if not mappings:
                return

            first_file = next(
                (f for f in rom.files if f.file_size_bytes > 0),
                None,
            )
            if first_file is not None:
                md5 = first_file.md5_hash
                sha1 = first_file.sha1_hash
                file_name = first_file.file_name
                file_size: int | None = first_file.file_size_bytes
            else:
                md5 = rom.md5_hash
                sha1 = rom.sha1_hash
                file_name = rom.fs_name
                file_size = rom.fs_size_bytes or None

            payload = {
                "md5": md5,
                "sha1": sha1,
                "sha256": None,
                "fileName": file_name,
                "fileSize": file_size,
                "mappings": mappings,
            }

            httpx_client = ctx_httpx_client.get()
            res = await httpx_client.post(
                self.suggestion_url,
                json=payload,
                headers={"user-agent": f"RomM/{get_version()}"},
                timeout=30,
            )
            res.raise_for_status()
        except Exception:
            log.debug("Playmatch match suggestion failed (ignored)", exc_info=True)
