# RomM Backend Architecture

Comprehensive documentation of the RomM backend: a FastAPI-based server powering the self-hosted retro gaming platform.

---

## Table of Contents

1. [Overview](#1-overview)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Directory Structure](#3-directory-structure)
4. [Application Lifecycle](#4-application-lifecycle)
5. [Database Layer](#5-database-layer)
6. [API Endpoints](#6-api-endpoints)
7. [Authentication & Authorization](#7-authentication--authorization)
8. [Business Logic (Handlers)](#8-business-logic-handlers)
9. [External Integrations (Adapters)](#9-external-integrations-adapters)
10. [Real-Time Communication (WebSockets)](#10-real-time-communication-websockets)
11. [Background Tasks & Scheduling](#11-background-tasks--scheduling)
12. [File System Management](#12-file-system-management)
13. [Caching (Redis)](#13-caching-redis)
14. [Configuration](#14-configuration)
15. [Error Handling](#15-error-handling)
16. [Logging](#16-logging)
17. [Testing](#17-testing)

---

## 1. Overview

| Property           | Value                            |
| ------------------ | -------------------------------- |
| **Framework**      | FastAPI 0.121.1                  |
| **Language**       | Python 3.13+                     |
| **ORM**            | SQLAlchemy 2.0                   |
| **Migrations**     | Alembic                          |
| **Databases**      | MariaDB, MySQL, PostgreSQL       |
| **Cache/Queue**    | Redis (via RQ)                   |
| **Real-time**      | Socket.IO (python-socketio)      |
| **Auth**           | OAuth2 + Basic + OIDC + Sessions |
| **ASGI Server**    | Uvicorn / Gunicorn               |
| **Error Tracking** | Sentry                           |

RomM's backend is responsible for:

- **Library scanning** — detecting platforms and ROMs from the filesystem
- **Metadata enrichment** — pulling game info from 10+ external providers
- **User management** — roles, authentication, per-user game tracking
- **Asset management** — saves, save states, screenshots, firmware/BIOS
- **Device sync** — cross-device save synchronization
- **Netplay** — real-time multiplayer room coordination
- **Feed generation** — Tinfoil, WebRcade, PKGi, and other custom formats

---

## 2. High-Level Architecture

```
                                 +---------------------+
                                 |     Nginx / CDN     |
                                 |  (reverse proxy,    |
                                 |   X-Accel-Redirect) |
                                 +----------+----------+
                                            |
                                            v
                              +-------------+--------------+
                              |     FastAPI Application     |
                              |        (main.py)            |
                              +-------------+--------------+
                                            |
              +-----------------------------+------------------------------+
              |                             |                              |
     +--------v--------+         +---------v----------+         +---------v--------+
     |   Middleware     |         |   API Routers      |         |   WebSockets     |
     |  Stack (5 layers)|         |  (20 routers)      |         |  /ws  /netplay   |
     +---------+--------+         +---------+----------+         +---------+--------+
               |                            |                              |
               v                            v                              v
     +-------------------+      +-----------+-----------+       +----------+---------+
     | CORS              |      |    Endpoint Layer     |       |  Socket.IO Server  |
     | CSRF              |      | (request validation,  |       |  (scan progress,   |
     | Authentication    |      |  response schemas)    |       |   netplay rooms)   |
     | Session (Redis)   |      +-----------+-----------+       +----------+---------+
     | Context Vars      |                  |                              |
     +-------------------+                  v                              |
                              +-------------+--------------+               |
                              |     Handler Layer          |               |
                              | (business logic, CRUD,     |<--------------+
                              |  metadata, filesystem)     |
                              +------+-------+------+------+
                                     |       |      |
                  +------------------+       |      +------------------+
                  |                          |                         |
         +--------v--------+     +----------v----------+    +---------v---------+
         |   Database       |     |   External APIs     |    |   File System     |
         |  (SQLAlchemy)    |     |  (IGDB, MobyGames,  |    |  (ROM library,    |
         |                  |     |   ScreenScraper,     |    |   assets, BIOS)   |
         | MariaDB/MySQL/PG |     |   SteamGridDB, RA,   |    |                   |
         +---------+--------+     |   LaunchBox, HLTB,   |    +-------------------+
                   |              |   Hasheous, TGDB,    |
                   v              |   Flashpoint)        |
         +-------------------+    +---------------------+
         |     Redis          |
         | (sessions, cache,  |
         |  job queues, rooms)|
         +-------------------+
```

### Layered Architecture

```
+-----------------------------------------------------------------------+
|                         PRESENTATION LAYER                             |
|  endpoints/          API route handlers, request/response schemas      |
|  endpoints/sockets/  WebSocket event handlers                          |
+-----------------------------------------------------------------------+
|                         BUSINESS LOGIC LAYER                           |
|  handler/auth/       Authentication & authorization                    |
|  handler/database/   CRUD operations per entity                        |
|  handler/metadata/   Metadata fetching & normalization                 |
|  handler/filesystem/ File system operations                            |
|  handler/            Scan orchestration, netplay, socket management    |
+-----------------------------------------------------------------------+
|                         DATA ACCESS LAYER                              |
|  models/             SQLAlchemy ORM model definitions                  |
|  adapters/services/  External API client wrappers                      |
|  handler/redis_handler.py  Cache & queue operations                    |
+-----------------------------------------------------------------------+
|                         INFRASTRUCTURE LAYER                           |
|  config/             Environment variables & YAML config               |
|  decorators/         Auth & DB session decorators                      |
|  exceptions/         Custom exception hierarchy                        |
|  logger/             Structured logging                                |
|  utils/              Shared helpers (hashing, context, validation)     |
|  tasks/              Background job definitions & scheduling           |
+-----------------------------------------------------------------------+
```

---

## 3. Directory Structure

```
backend/
├── main.py                    # FastAPI app creation, middleware, routers
├── startup.py                 # Pre-startup: cache init, scheduled tasks
├── watcher.py                 # Filesystem change detection (watchfiles)
├── __version__.py             # Version placeholder
├── alembic.ini                # Alembic migration configuration
├── pytest.ini                 # Test configuration
├── .coveragerc                # Code coverage settings
│
├── adapters/                  # External API client wrappers
│   └── services/
│       ├── igdb.py            # IGDB (Internet Game Database)
│       ├── mobygames.py       # MobyGames
│       ├── screenscraper.py   # ScreenScraper
│       ├── steamgriddb.py     # SteamGridDB
│       ├── retroachievements.py # RetroAchievements
│       ├── rahasher.py        # RA hash computation
│       └── *_types.py         # Type definitions per adapter
│
├── alembic/                   # Database migrations
│   ├── env.py                 # Migration environment setup
│   └── versions/              # 72+ migration scripts
│
├── config/                    # Configuration system
│   ├── __init__.py            # Env var loading (100+ variables)
│   └── config_manager.py      # YAML config manager (singleton)
│
├── decorators/                # Function decorators
│   ├── auth.py                # @protected_route, OAuth setup
│   └── database.py            # @begin_session (DB session injection)
│
├── endpoints/                 # API route handlers
│   ├── auth.py                # Login, logout, token, OIDC
│   ├── user.py                # User CRUD, invite links
│   ├── client_tokens.py       # API token management
│   ├── platform.py            # Platform CRUD
│   ├── collections.py         # Collection management
│   ├── configs.py             # App configuration
│   ├── device.py              # Device registration
│   ├── feeds.py               # Tinfoil, WebRcade, PKGi feeds
│   ├── firmware.py            # BIOS/firmware management
│   ├── gamelist.py            # ES-DE gamelist export
│   ├── heartbeat.py           # Health check + setup wizard
│   ├── netplay.py             # Netplay room listing
│   ├── raw.py                 # Raw asset file serving
│   ├── saves.py               # Save file management
│   ├── screenshots.py         # Screenshot management
│   ├── search.py              # Cross-provider metadata search
│   ├── states.py              # Save state management
│   ├── stats.py               # Library statistics
│   ├── tasks.py               # Task monitoring & triggering
│   ├── roms/                  # ROM-specific endpoints
│   │   ├── __init__.py        # ROM CRUD, download, bulk ops
│   │   ├── upload.py          # Chunked upload system
│   │   ├── files.py           # File download (nginx redirect)
│   │   ├── manual.py          # Manual metadata entry
│   │   └── notes.py           # ROM notes/comments
│   ├── sockets/               # WebSocket handlers
│   │   ├── scan.py            # Scan progress events
│   │   └── netplay.py         # Netplay room events
│   ├── forms/                 # Request body models
│   │   └── identity.py        # Auth form schemas
│   └── responses/             # Pydantic response schemas
│       ├── base.py            # Base response classes
│       ├── rom.py             # SimpleRomSchema, DetailedRomSchema
│       ├── platform.py        # PlatformSchema
│       ├── identity.py        # UserSchema, TokenResponse
│       └── ...                # 15+ more response schemas
│
├── exceptions/                # Custom exception classes
│   ├── auth_exceptions.py     # AuthCredentialsException, etc.
│   ├── endpoint_exceptions.py # NotFound, Permission, Conflict
│   ├── fs_exceptions.py       # Filesystem errors
│   ├── config_exceptions.py   # Config write errors
│   ├── task_exceptions.py     # Scheduler errors
│   └── socket_exceptions.py   # Scan stopped
│
├── handler/                   # Business logic layer
│   ├── scan_handler.py        # Library scan orchestration
│   ├── socket_handler.py      # Socket.IO server management
│   ├── netplay_handler.py     # Netplay room state
│   ├── redis_handler.py       # Redis clients & queues
│   ├── auth/                  # Authentication subsystem
│   │   ├── base_handler.py    # Auth, OAuth, OIDC handlers
│   │   ├── hybrid_auth.py     # Multi-method auth backend
│   │   ├── constants.py       # Scopes, role mappings
│   │   └── middleware/        # CSRF, session middleware
│   ├── database/              # Per-entity CRUD handlers
│   │   ├── base_handler.py    # Engine, session factory
│   │   ├── roms_handler.py    # ROM queries & mutations
│   │   ├── platforms_handler.py
│   │   ├── users_handler.py
│   │   ├── saves_handler.py
│   │   ├── states_handler.py
│   │   ├── screenshots_handler.py
│   │   ├── firmware_handler.py
│   │   ├── collections_handler.py
│   │   ├── devices_handler.py
│   │   ├── device_save_sync_handler.py
│   │   ├── client_tokens_handler.py
│   │   └── stats_handler.py
│   ├── filesystem/            # File I/O operations
│   │   ├── base_handler.py
│   │   ├── roms_handler.py    # ROM file reading, hashing
│   │   ├── assets_handler.py  # User assets storage
│   │   ├── firmware_handler.py
│   │   ├── platforms_handler.py
│   │   └── resources_handler.py # Artwork caching
│   └── metadata/              # Metadata provider handlers
│       ├── base_handler.py    # Base metadata handler
│       ├── igdb_handler.py
│       ├── moby_handler.py
│       ├── ss_handler.py      # ScreenScraper
│       ├── sgdb_handler.py    # SteamGridDB
│       ├── ra_handler.py      # RetroAchievements
│       ├── hltb_handler.py    # HowLongToBeat
│       ├── hasheous_handler.py
│       ├── tgdb_handler.py    # TheGamesDB
│       ├── flashpoint_handler.py
│       ├── gamelist_handler.py # gamelist.xml parser
│       ├── playmatch_handler.py
│       ├── launchbox_handler/ # LaunchBox (local + remote)
│       └── fixtures/          # Static metadata indexes
│           ├── mame_index.json
│           ├── scummvm_index.json
│           ├── ps1_serial_index.json
│           ├── ps2_serial_index.json
│           ├── ps2_opl_index.json
│           ├── psp_serial_index.json
│           └── known_bios_files.json
│
├── logger/                    # Logging setup
│   ├── logger.py              # Logger instance ("romm")
│   └── formatter.py           # Colored formatter, LOGGING_CONFIG
│
├── models/                    # SQLAlchemy ORM models
│   ├── base.py                # BaseModel (created_at, updated_at)
│   ├── user.py                # User, Role enum
│   ├── platform.py            # Platform
│   ├── rom.py                 # Rom, RomFile, RomMetadata, RomUser, RomNote
│   ├── collection.py          # Collection, SmartCollection, VirtualCollection
│   ├── assets.py              # Save, State, Screenshot
│   ├── device.py              # Device, SyncMode enum
│   ├── device_save_sync.py    # DeviceSaveSync
│   ├── firmware.py            # Firmware
│   ├── client_token.py        # ClientToken
│   └── fixtures/              # Seed data
│       └── known_bios_files.json
│
├── tasks/                     # Background job system
│   ├── tasks.py               # Base Task, PeriodicTask classes
│   ├── scheduled/             # Cron-scheduled tasks
│   │   ├── scan_library.py
│   │   ├── sync_retroachievements_progress.py
│   │   ├── update_switch_titledb.py
│   │   ├── update_launchbox_metadata.py
│   │   ├── convert_images_to_webp.py
│   │   └── cleanup_netplay.py
│   └── manual/                # On-demand tasks
│       ├── cleanup_missing_roms.py
│       └── cleanup_orphaned_resources.py
│
├── utils/                     # Shared helpers
│   ├── __init__.py            # get_version()
│   ├── cache.py               # Redis fixture loading
│   ├── hashing.py             # CRC32, file hashing
│   ├── context.py             # Async context vars (aiohttp, httpx)
│   ├── database.py            # JSON/JSONB helpers, DB detection
│   ├── filesystem.py          # Path sanitization
│   ├── validation.py          # Input validation
│   ├── client_tokens.py       # Token generation
│   ├── datetime.py            # UTC helpers
│   ├── json_module.py         # Custom JSON encoder
│   ├── nginx.py               # X-Accel-Redirect responses
│   ├── router.py              # Custom APIRouter
│   ├── gamelist_exporter.py   # ES-DE gamelist.xml generation
│   ├── archive_7zip.py        # 7-Zip archive handling
│   ├── platforms.py           # Platform management
│   └── emoji.py               # Emoji utilities
│
├── tools/                     # Development utilities
│   └── xml_diagnostics.py     # XML diagnostic tool
│
├── tests/                     # Test suite
│   ├── conftest.py            # Pytest fixtures
│   └── ...                    # Mirrors backend structure
│
└── romm_test/                 # Test fixtures & data
    ├── assets/users/          # Test user saves
    ├── config/                # Test configuration
    ├── library/               # Test ROM library
    └── resources/roms/        # Test ROM resources
```

---

## 4. Application Lifecycle

### Startup Sequence

```
1. alembic upgrade head          # Run database migrations
2. startup.main()                # Async startup tasks
   ├── Initialize scheduled jobs (RQ Scheduler)
   │   ├── cleanup_netplay
   │   ├── scan_library (if ENABLE_SCHEDULED_RESCAN)
   │   ├── update_switch_titledb
   │   ├── update_launchbox_metadata
   │   ├── convert_images_to_webp
   │   └── sync_retroachievements_progress
   └── Load fixture caches into Redis
       ├── mame_index.json
       ├── scummvm_index.json
       ├── ps1/ps2/psp serial indexes
       └── known_bios_files.json
3. uvicorn.run("main:app")       # Start ASGI server
   └── FastAPI lifespan
       ├── Create aiohttp.ClientSession
       ├── Create httpx.AsyncClient
       └── Store in app.state + context vars
```

### Middleware Stack (execution order, outside-in)

```
Request →  CORS → CSRF → Authentication → Session (Redis) → Context Vars → Endpoint
Response ← CORS ← CSRF ← Authentication ← Session (Redis) ← Context Vars ← Endpoint
```

| Layer | Middleware                 | Purpose                                            |
| ----- | -------------------------- | -------------------------------------------------- |
| 1     | `CORSMiddleware`           | Allow cross-origin requests (all origins)          |
| 2     | `CSRFMiddleware`           | Token-based CSRF protection (cookie + header)      |
| 3     | `AuthenticationMiddleware` | `HybridAuthBackend` — Basic, Bearer, Session, OIDC |
| 4     | `RedisSessionMiddleware`   | Cookie-based sessions stored in Redis              |
| 5     | `set_context_middleware`   | Inject aiohttp/httpx clients into context vars     |

### Request Flow

```
HTTP Request
    │
    ├─ Middleware processes request (auth, session, CSRF)
    │
    ├─ FastAPI routes to endpoint handler
    │   └─ @protected_route checks scopes
    │
    ├─ Endpoint calls handler layer
    │   ├─ handler/database/*   → SQLAlchemy queries
    │   ├─ handler/metadata/*   → External API calls
    │   ├─ handler/filesystem/* → File I/O
    │   └─ handler/auth/*       → Token operations
    │
    ├─ Response schema (Pydantic) serializes output
    │
    └─ HTTP Response
```

---

## 5. Database Layer

### Supported Databases

| Database      | Driver                | Status    |
| ------------- | --------------------- | --------- |
| MariaDB 10.5+ | `mariadb+pymysql`     | Default   |
| MySQL 8.0+    | `mysql+pymysql`       | Supported |
| PostgreSQL    | `postgresql+psycopg2` | Supported |

### Engine & Session Setup

**Location:** `handler/database/base_handler.py`

```python
sync_engine = create_engine(
    ConfigManager.get_db_engine(),
    pool_pre_ping=True,  # Connection health check
    echo=False,          # SQL logging (DEV_SQL_ECHO overrides)
)
sync_session = sessionmaker(bind=sync_engine, expire_on_commit=False)
```

Sessions are injected via the `@begin_session` decorator, which wraps handlers in a transaction context.

### Base Model

**Location:** `models/base.py`

All models inherit `BaseModel`, providing:

| Column       | Type                 | Behavior                     |
| ------------ | -------------------- | ---------------------------- |
| `created_at` | `TIMESTAMP(tz=True)` | Auto-set to UTC on creation  |
| `updated_at` | `TIMESTAMP(tz=True)` | Auto-updated on modification |

Constants: `FILE_NAME_MAX_LENGTH=450`, `FILE_PATH_MAX_LENGTH=1000`, `FILE_EXTENSION_MAX_LENGTH=100`

### Entity-Relationship Diagram

```
                                    ┌──────────────┐
                          ┌────────>│  client_tokens│
                          │         └──────────────┘
                          │
                          │         ┌──────────────┐
                          ├────────>│   devices     │──────┐
                          │         └──────────────┘      │
                          │                                v
┌──────────┐              │         ┌──────────────┐    ┌──────────────────┐
│  users   │──────────────┼────────>│    saves      │<───│ device_save_sync │
└──────────┘              │         └──────────────┘    └──────────────────┘
     │                    │
     │                    │         ┌──────────────┐
     │                    ├────────>│   states      │
     │                    │         └──────────────┘
     │                    │
     │                    │         ┌──────────────┐
     │                    ├────────>│ screenshots   │
     │                    │         └──────────────┘
     │                    │
     │                    │         ┌──────────────┐
     │                    ├────────>│  rom_user     │
     │                    │         └──────────────┘
     │                    │              │
     │                    │              │
     │                    │         ┌──────────────┐      ┌──────────────┐
     │                    │    ┌───>│    roms       │<─────│  platforms   │
     │                    │    │    └──────────────┘      └──────────────┘
     │                    │    │         │                      │
     │                    │    │         ├────> rom_files       ├────> firmware
     │                    │    │         ├────> roms_metadata   │
     │                    │    │         ├────> rom_notes       │
     │                    │    │         ├────> saves           │
     │                    │    │         ├────> states          │
     │                    │    │         ├────> screenshots     │
     │                    │    │         └────> sibling_roms (self M:M)
     │                    │    │
     │         ┌──────────┴────┴──┐
     └────────>│   collections    │  (M:M via collections_roms)
               ├──────────────────┤
               │ smart_collections│  (filter-based, dynamic)
               ├──────────────────┤
               │virtual_collections│  (DB view, read-only)
               └──────────────────┘
```

### Model Definitions

#### Users

**Table:** `users`

| Column            | Type                              | Notes                      |
| ----------------- | --------------------------------- | -------------------------- |
| `id`              | Integer                           | PK, autoincrement          |
| `username`        | String(255)                       | Unique, indexed            |
| `hashed_password` | String(255)                       | Nullable (OIDC users)      |
| `email`           | String(255)                       | Unique, indexed, nullable  |
| `enabled`         | Boolean                           | Default `True`             |
| `role`            | Enum(`VIEWER`, `EDITOR`, `ADMIN`) | Default `VIEWER`           |
| `avatar_path`     | String(255)                       | Default `""`               |
| `last_login`      | Timestamp                         | Nullable                   |
| `last_active`     | Timestamp                         | Nullable                   |
| `ra_username`     | String(255)                       | RetroAchievements username |
| `ra_progression`  | JSON                              | RetroAchievements data     |
| `ui_settings`     | JSON                              | User preferences           |

**Relationships:** saves (1:M), states (1:M), screenshots (1:M), rom_users (1:M), notes (1:M), collections (1:M), smart_collections (1:M), devices (1:M, cascade), client_tokens (1:M, cascade)

---

#### Platforms

**Table:** `platforms`

| Column                                                                                                       | Type         | Notes                         |
| ------------------------------------------------------------------------------------------------------------ | ------------ | ----------------------------- |
| `id`                                                                                                         | Integer      | PK                            |
| `slug`                                                                                                       | String(100)  | Indexed, canonical identifier |
| `fs_slug`                                                                                                    | String(100)  | Filesystem folder name        |
| `name`                                                                                                       | String(400)  | Display name                  |
| `custom_name`                                                                                                | String(400)  | User override                 |
| `igdb_id`, `sgdb_id`, `moby_id`, `ss_id`, `ra_id`, `launchbox_id`, `hasheous_id`, `tgdb_id`, `flashpoint_id` | Integer      | External provider IDs         |
| `category`                                                                                                   | String(100)  | Platform category             |
| `generation`                                                                                                 | Integer      | Console generation            |
| `family_name` / `family_slug`                                                                                | String(1000) | Platform family               |
| `aspect_ratio`                                                                                               | String(10)   | Default `"2 / 3"`             |
| `missing_from_fs`                                                                                            | Boolean      | Default `False`               |

**Computed properties:** `rom_count` (subquery), `fs_size_bytes` (sum of ROM sizes)

**Relationships:** roms (1:M), firmware (1:M)

---

#### ROMs

**Table:** `roms` — the central entity

| Column Group          | Columns                                                                                                                                                                                   | Notes                   |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------- |
| **Identity**          | `id`, `platform_id` (FK)                                                                                                                                                                  | Core identifiers        |
| **External IDs**      | `igdb_id`, `sgdb_id`, `moby_id`, `ss_id`, `ra_id`, `launchbox_id`, `hasheous_id`, `tgdb_id`, `flashpoint_id`, `hltb_id`, `gamelist_id`                                                    | All indexed             |
| **Filesystem**        | `fs_name`, `fs_name_no_tags`, `fs_name_no_ext`, `fs_extension`, `fs_path`, `fs_size_bytes`                                                                                                | File info               |
| **Display**           | `name`, `slug`, `summary`                                                                                                                                                                 | Game metadata           |
| **Provider metadata** | `igdb_metadata`, `moby_metadata`, `ss_metadata`, `ra_metadata`, `launchbox_metadata`, `hasheous_metadata`, `flashpoint_metadata`, `hltb_metadata`, `gamelist_metadata`, `manual_metadata` | JSON blobs per provider |
| **Media**             | `path_cover_s`, `path_cover_l`, `url_cover`, `path_manual`, `url_manual`, `path_screenshots`, `url_screenshots`                                                                           | Cover art & screenshots |
| **Classification**    | `revision`, `version`, `regions`, `languages`, `tags`                                                                                                                                     | Game attributes         |
| **Hashes**            | `crc_hash`, `md5_hash`, `sha1_hash`, `ra_hash`                                                                                                                                            | File integrity          |
| **State**             | `missing_from_fs`                                                                                                                                                                         | Filesystem sync         |

**Relationships:** platform (M:1), files (1:M), saves (1:M), states (1:M), screenshots (1:M), rom_users (1:M), notes (1:M), metadatum (1:1), sibling_roms (M:M self-referential), collections (M:M)

---

#### ROM Files

**Table:** `rom_files`

Tracks individual files within a ROM (archives can contain multiple files).

| Column                                         | Type        | Notes                                                                                                  |
| ---------------------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------ |
| `id`                                           | Integer     | PK                                                                                                     |
| `rom_id`                                       | Integer     | FK → roms                                                                                              |
| `file_name`, `file_path`                       | String      | File identity                                                                                          |
| `file_size_bytes`                              | BigInteger  | Size                                                                                                   |
| `crc_hash`, `md5_hash`, `sha1_hash`, `ra_hash` | String(100) | Hashes                                                                                                 |
| `category`                                     | Enum        | `GAME`, `DLC`, `HACK`, `MANUAL`, `PATCH`, `UPDATE`, `MOD`, `DEMO`, `TRANSLATION`, `PROTOTYPE`, `CHEAT` |
| `missing_from_fs`                              | Boolean     | Sync state                                                                                             |

---

#### ROM Metadata (Aggregated)

**Table:** `roms_metadata` — aggregated metadata from all providers

| Column               | Type                        |
| -------------------- | --------------------------- |
| `rom_id`             | Integer (PK, FK → roms)     |
| `genres`             | JSON                        |
| `franchises`         | JSON                        |
| `collections`        | JSON                        |
| `companies`          | JSON                        |
| `game_modes`         | JSON                        |
| `age_ratings`        | JSON                        |
| `player_count`       | String(100)                 |
| `first_release_date` | BigInteger (UNIX timestamp) |
| `average_rating`     | Float                       |

---

#### ROM User Data

**Table:** `rom_user` — per-user, per-ROM tracking

| Column               | Type         | Notes                                                                 |
| -------------------- | ------------ | --------------------------------------------------------------------- |
| `rom_id` + `user_id` | FK composite | Unique constraint                                                     |
| `is_main_sibling`    | Boolean      | Primary version flag                                                  |
| `last_played`        | Timestamp    |                                                                       |
| `backlogged`         | Boolean      |                                                                       |
| `now_playing`        | Boolean      |                                                                       |
| `hidden`             | Boolean      |                                                                       |
| `rating`             | Integer      | 0-5                                                                   |
| `difficulty`         | Integer      |                                                                       |
| `completion`         | Integer      | Percentage                                                            |
| `status`             | Enum         | `INCOMPLETE`, `FINISHED`, `COMPLETED_100`, `RETIRED`, `NEVER_PLAYING` |

---

#### ROM Notes

**Table:** `rom_notes`

| Column                         | Type        | Notes             |
| ------------------------------ | ----------- | ----------------- |
| `id`                           | Integer     | PK                |
| `rom_id` + `user_id` + `title` |             | Unique constraint |
| `title`                        | String(400) |                   |
| `content`                      | Text        |                   |
| `is_public`                    | Boolean     | Default `False`   |
| `tags`                         | JSON        |                   |

---

#### Collections

**Table:** `collections` — manually curated ROM lists

| Column                        | Type        | Notes     |
| ----------------------------- | ----------- | --------- |
| `id`                          | Integer     | PK        |
| `user_id`                     | FK → users  |           |
| `name`                        | String(400) |           |
| `description`                 | Text        |           |
| `is_public`                   | Boolean     |           |
| `is_favorite`                 | Boolean     |           |
| `path_cover_s/l`, `url_cover` | Text        | Cover art |

Linked to ROMs via `collections_roms` join table (M:M).

**Table:** `smart_collections` — dynamic, filter-based

| Column            | Type    | Notes                   |
| ----------------- | ------- | ----------------------- |
| `filter_criteria` | JSON    | Query definition        |
| `rom_ids`         | JSON    | Cached matching ROM IDs |
| `rom_count`       | Integer | Cached count            |

**View:** `virtual_collections` — database view, read-only, excluded from migrations.

---

#### Assets (Saves, States, Screenshots)

All three share a similar structure:

| Table         | Extra Columns                      | Notes               |
| ------------- | ---------------------------------- | ------------------- |
| `saves`       | `emulator`, `slot`, `content_hash` | Device sync support |
| `states`      | `emulator`                         | Save states         |
| `screenshots` | —                                  | In-game captures    |

Common columns: `id`, `rom_id` (FK), `user_id` (FK), `file_name`, `file_path`, `file_size_bytes`, `missing_from_fs`

Saves additionally link to `device_save_sync` for cross-device tracking.

---

#### Devices & Sync

**Table:** `devices`

| Column                                         | Type        | Notes                               |
| ---------------------------------------------- | ----------- | ----------------------------------- |
| `id`                                           | String(255) | UUID, PK                            |
| `user_id`                                      | FK → users  |                                     |
| `name`, `platform`, `client`, `client_version` | String      | Device info                         |
| `sync_mode`                                    | Enum        | `API`, `FILE_TRANSFER`, `PUSH_PULL` |
| `sync_enabled`                                 | Boolean     |                                     |
| `last_seen`                                    | Timestamp   |                                     |

**Table:** `device_save_sync` — tracks per-device, per-save sync state

| Column                  | Type         |
| ----------------------- | ------------ |
| `device_id` + `save_id` | Composite PK |
| `last_synced_at`        | Timestamp    |
| `is_untracked`          | Boolean      |

---

#### Client Tokens

**Table:** `client_tokens` — long-lived API tokens

| Column         | Type         | Notes                        |
| -------------- | ------------ | ---------------------------- |
| `id`           | Integer      | PK                           |
| `user_id`      | FK → users   |                              |
| `name`         | String(255)  | Display name                 |
| `hashed_token` | String(64)   | SHA-256 hash, unique         |
| `scopes`       | String(1000) | Space-separated OAuth scopes |
| `expires_at`   | Timestamp    | Nullable                     |
| `last_used_at` | Timestamp    |                              |

Token format: `rmm_` + 64 hex chars (32-byte random)

---

#### Firmware

**Table:** `firmware`

| Column                              | Type           | Notes                         |
| ----------------------------------- | -------------- | ----------------------------- |
| `id`                                | Integer        | PK                            |
| `platform_id`                       | FK → platforms |                               |
| `file_name`, `file_path`            | String         |                               |
| `crc_hash`, `md5_hash`, `sha1_hash` | String         | Integrity                     |
| `is_verified`                       | Boolean        | Matches known_bios_files.json |

---

### Alembic Migrations

72+ migration scripts in `alembic/versions/`. Key milestones:

| Migration      | Description                               |
| -------------- | ----------------------------------------- |
| `0009`         | Models refactor                           |
| `0014`, `0019` | Asset filesystem refactoring              |
| `0020`         | Added created_at/updated_at to all tables |
| `0021`         | ROM user associations                     |
| `0022`         | Collection system                         |
| `0023`         | Column nullability constraints            |
| `0024`         | Sibling ROM database views                |
| `0025`         | ROM hash tracking                         |
| `0064`         | Performance indexes on updated_at         |
| `0068`         | Device + device_save_sync tables          |
| `0072`         | Client tokens table                       |

Migrations support batch mode for SQLite and DB-specific SQL for MariaDB/MySQL/PostgreSQL.

---

## 6. API Endpoints

**Base URL:** `/api`
**Documentation:** Swagger UI at `/api/docs`, ReDoc at `/api/redoc`
**Pagination:** `fastapi-pagination` with `LimitOffsetParams` (`limit`, `offset`, `total`)

### 6.1 Authentication (`/api`)

| Method | Path               | Auth | Description                                    |
| ------ | ------------------ | ---- | ---------------------------------------------- |
| POST   | `/login`           | No   | Session login (HTTP Basic)                     |
| POST   | `/logout`          | No   | Logout (returns OIDC logout URL if configured) |
| POST   | `/token`           | No   | OAuth2 token (password, refresh_token grants)  |
| GET    | `/login/openid`    | No   | OIDC login redirect                            |
| GET    | `/oauth/openid`    | No   | OIDC callback                                  |
| POST   | `/forgot-password` | No   | Request password reset                         |
| POST   | `/reset-password`  | No   | Reset password with token                      |

### 6.2 Users (`/api/users`)

| Method | Path               | Scope                  | Description                      |
| ------ | ------------------ | ---------------------- | -------------------------------- |
| POST   | `/`                | ME_WRITE / USERS_WRITE | Create user (first user = admin) |
| POST   | `/invite-link`     | USERS_WRITE            | Generate invite token            |
| POST   | `/register`        | None                   | Register with invite token       |
| GET    | `/`                | USERS_READ             | List all users                   |
| GET    | `/identifiers`     | USERS_READ             | Get user IDs                     |
| GET    | `/me`              | ME_READ                | Current user profile             |
| GET    | `/{id}`            | USERS_READ             | Get user by ID                   |
| PUT    | `/{id}`            | ME_WRITE               | Update user                      |
| DELETE | `/{id}`            | USERS_WRITE            | Delete user                      |
| POST   | `/{id}/ra/refresh` | ME_WRITE               | Refresh RetroAchievements data   |

### 6.3 Client Tokens (`/api/client-tokens`)

| Method | Path                  | Scope       | Description                  |
| ------ | --------------------- | ----------- | ---------------------------- |
| POST   | `/`                   | ME_WRITE    | Create token                 |
| GET    | `/`                   | ME_READ     | List user's tokens           |
| DELETE | `/{id}`               | ME_WRITE    | Delete token                 |
| PUT    | `/{id}/regenerate`    | ME_WRITE    | Regenerate token             |
| POST   | `/{id}/pair`          | ME_WRITE    | Generate pair code           |
| GET    | `/pair/{code}/status` | None        | Check pair status            |
| POST   | `/exchange`           | None        | Exchange pair code for token |
| GET    | `/all`                | USERS_READ  | Admin: list all tokens       |
| DELETE | `/{id}/admin`         | USERS_WRITE | Admin: delete any token      |

### 6.4 Platforms (`/api/platforms`)

| Method | Path           | Scope           | Description                                  |
| ------ | -------------- | --------------- | -------------------------------------------- |
| POST   | `/`            | PLATFORMS_WRITE | Create platform                              |
| GET    | `/`            | PLATFORMS_READ  | List platforms (with `updated_after` filter) |
| GET    | `/identifiers` | PLATFORMS_READ  | Get platform IDs                             |
| GET    | `/supported`   | PLATFORMS_READ  | List supported platforms                     |
| GET    | `/{id}`        | PLATFORMS_READ  | Get platform                                 |
| PUT    | `/{id}`        | PLATFORMS_WRITE | Update platform                              |
| DELETE | `/{id}`        | PLATFORMS_WRITE | Delete platform                              |

### 6.5 ROMs (`/api/roms`)

| Method | Path                         | Scope      | Description                       |
| ------ | ---------------------------- | ---------- | --------------------------------- |
| GET    | `/`                          | ROMS_READ  | List ROMs (paginated, filterable) |
| GET    | `/identifiers`               | ROMS_READ  | Get ROM IDs                       |
| GET    | `/{id}`                      | ROMS_READ  | Get ROM details                   |
| PUT    | `/{id}`                      | ROMS_WRITE | Update ROM metadata               |
| PUT    | `/{id}/user`                 | ME_WRITE   | Update user-specific ROM data     |
| DELETE | `/{id}`                      | ROMS_WRITE | Delete ROM                        |
| POST   | `/delete`                    | ROMS_WRITE | Bulk delete                       |
| POST   | `/download/{id}/{file_name}` | ROMS_READ  | Download ROM                      |
| POST   | `/unidentified`              | ROMS_READ  | Get unidentified ROMs             |

#### ROM Upload (Chunked)

| Method | Path                    | Scope      | Description               |
| ------ | ----------------------- | ---------- | ------------------------- |
| POST   | `/upload/init`          | ROMS_WRITE | Initialize upload session |
| POST   | `/upload/{id}/chunk`    | ROMS_WRITE | Upload chunk (max 64MB)   |
| POST   | `/upload/{id}/complete` | ROMS_WRITE | Finalize upload           |
| GET    | `/upload/{id}/session`  | ROMS_WRITE | Check session status      |
| DELETE | `/upload/{id}`          | ROMS_WRITE | Cancel upload             |

#### ROM Files

| Method | Path                         | Scope     | Description                             |
| ------ | ---------------------------- | --------- | --------------------------------------- |
| GET    | `/{id}/files`                | ROMS_READ | Get ROM file metadata                   |
| GET    | `/{id}/files/content/{name}` | ROMS_READ | Download file (nginx X-Accel or direct) |

### 6.6 Search (`/api/search`)

| Method | Path     | Scope     | Description                          |
| ------ | -------- | --------- | ------------------------------------ |
| GET    | `/roms`  | ROMS_READ | Search metadata across all providers |
| GET    | `/cover` | ROMS_READ | Search SteamGridDB for cover art     |

### 6.7 Saves (`/api/saves`)

| Method | Path               | Scope         | Description                             |
| ------ | ------------------ | ------------- | --------------------------------------- |
| POST   | `/`                | ASSETS_WRITE  | Upload save (with optional device sync) |
| GET    | `/`                | ASSETS_READ   | List saves (with device_id filter)      |
| GET    | `/identifiers`     | ASSETS_READ   | Get save IDs                            |
| GET    | `/summary`         | ASSETS_READ   | Saves grouped by slot                   |
| GET    | `/{id}`            | ASSETS_READ   | Get save                                |
| GET    | `/{id}/content`    | ASSETS_READ   | Download save file                      |
| POST   | `/{id}/downloaded` | DEVICES_WRITE | Confirm download (device sync)          |
| PUT    | `/{id}`            | ASSETS_WRITE  | Update save                             |
| POST   | `/delete`          | ASSETS_WRITE  | Bulk delete                             |
| POST   | `/{id}/track`      | DEVICES_WRITE | Re-enable sync tracking                 |
| POST   | `/{id}/untrack`    | DEVICES_WRITE | Disable sync tracking                   |

### 6.8 States (`/api/states`)

| Method | Path           | Scope        | Description   |
| ------ | -------------- | ------------ | ------------- |
| POST   | `/`            | ASSETS_WRITE | Upload state  |
| GET    | `/`            | ASSETS_READ  | List states   |
| GET    | `/identifiers` | ASSETS_READ  | Get state IDs |
| GET    | `/{id}`        | ASSETS_READ  | Get state     |
| PUT    | `/{id}`        | ASSETS_WRITE | Update state  |
| POST   | `/delete`      | ASSETS_WRITE | Bulk delete   |

### 6.9 Screenshots (`/api/screenshots`)

| Method | Path           | Scope        | Description        |
| ------ | -------------- | ------------ | ------------------ |
| POST   | `/`            | ASSETS_WRITE | Upload screenshot  |
| GET    | `/`            | ASSETS_READ  | List screenshots   |
| GET    | `/identifiers` | ASSETS_READ  | Get screenshot IDs |
| GET    | `/{id}`        | ASSETS_READ  | Get screenshot     |
| PUT    | `/{id}`        | ASSETS_WRITE | Update screenshot  |
| POST   | `/delete`      | ASSETS_WRITE | Bulk delete        |

### 6.10 Devices (`/api/devices`)

| Method | Path    | Scope         | Description                         |
| ------ | ------- | ------------- | ----------------------------------- |
| POST   | `/`     | DEVICES_WRITE | Register device (fingerprint dedup) |
| GET    | `/`     | DEVICES_READ  | List devices                        |
| GET    | `/{id}` | DEVICES_READ  | Get device                          |
| PUT    | `/{id}` | DEVICES_WRITE | Update device                       |
| DELETE | `/{id}` | DEVICES_WRITE | Delete device                       |

### 6.11 Collections (`/api/collections`)

| Method | Path                  | Scope             | Description           |
| ------ | --------------------- | ----------------- | --------------------- |
| POST   | `/`                   | COLLECTIONS_WRITE | Create collection     |
| GET    | `/`                   | COLLECTIONS_READ  | List collections      |
| GET    | `/identifiers`        | COLLECTIONS_READ  | Get collection IDs    |
| GET    | `/{id}`               | COLLECTIONS_READ  | Get collection        |
| PUT    | `/{id}`               | COLLECTIONS_WRITE | Update collection     |
| DELETE | `/{id}`               | COLLECTIONS_WRITE | Delete collection     |
| POST   | `/{id}/roms`          | COLLECTIONS_WRITE | Add ROM to collection |
| DELETE | `/{id}/roms/{rom_id}` | COLLECTIONS_WRITE | Remove ROM            |

### 6.12 Feeds (`/api/feeds`)

| Method | Path                  | Description                   |
| ------ | --------------------- | ----------------------------- |
| GET    | `/webrcade`           | WebRcade feed format          |
| GET    | `/tinfoil`            | Tinfoil custom index (Switch) |
| GET    | `/pkgi/ps3/{type}`    | PKGi PS3 database             |
| GET    | `/pkgi/psvita/{type}` | PKGi PS Vita database         |
| GET    | `/pkgi/psp/{type}`    | PKGi PSP database             |
| GET    | `/fpkgi/{platform}`   | FPKGi (PS4/PS5) format        |
| GET    | `/kekatsu/{platform}` | Kekatsu DS format             |
| GET    | `/pkgj/psp/games`     | PKGj PSP games                |
| GET    | `/pkgj/psp/dlc`       | PKGj PSP DLC                  |
| GET    | `/pkgj/psvita/games`  | PKGj PS Vita games            |
| GET    | `/pkgj/psvita/dlc`    | PKGj PS Vita DLC              |
| GET    | `/pkgj/psx/games`     | PKGj PSX games                |

### 6.13 Configuration (`/api/config`)

| Method | Path                       | Scope           | Description              |
| ------ | -------------------------- | --------------- | ------------------------ |
| GET    | `/`                        | None            | Get RomM configuration   |
| POST   | `/system/platforms`        | PLATFORMS_WRITE | Add platform binding     |
| DELETE | `/system/platforms/{slug}` | PLATFORMS_WRITE | Remove platform binding  |
| POST   | `/system/versions`         | PLATFORMS_WRITE | Add version mapping      |
| DELETE | `/system/versions/{slug}`  | PLATFORMS_WRITE | Remove version mapping   |
| POST   | `/system/exclusions`       | PLATFORMS_WRITE | Add exclusion pattern    |
| DELETE | `/system/exclusions`       | PLATFORMS_WRITE | Remove exclusion pattern |

### 6.14 Tasks (`/api/tasks`)

| Method | Path          | Scope     | Description              |
| ------ | ------------- | --------- | ------------------------ |
| GET    | `/`           | TASKS_RUN | List all available tasks |
| GET    | `/status`     | TASKS_RUN | Status of all tasks      |
| GET    | `/{id}`       | TASKS_RUN | Status of specific task  |
| POST   | `/run/{name}` | TASKS_RUN | Trigger task execution   |

### 6.15 Other Endpoints

| Router    | Path                                   | Description                            |
| --------- | -------------------------------------- | -------------------------------------- |
| Heartbeat | `GET /api/heartbeat`                   | System info, version, metadata sources |
| Heartbeat | `GET /api/heartbeat/metadata/{source}` | Check metadata provider health         |
| Heartbeat | `GET /api/setup/library`               | Library structure info (wizard)        |
| Heartbeat | `POST /api/setup/platforms`            | Create platform folders (wizard)       |
| Stats     | `GET /api/stats`                       | Library statistics                     |
| Raw       | `HEAD /api/raw/assets/{path}`          | Check asset existence                  |
| Raw       | `GET /api/raw/assets/{path}`           | Serve raw asset file                   |
| Firmware  | Standard CRUD                          | BIOS file management                   |
| Gamelist  | `POST /api/gamelist/export`            | Export gamelist.xml                    |
| Netplay   | `GET /api/netplay/list`                | List netplay rooms                     |

**Total: 110+ endpoints**

---

## 7. Authentication & Authorization

### Authentication Methods

```
┌──────────────────────────────────────────────────────────────┐
│                    HybridAuthBackend                          │
│                                                              │
│  1. Check session cookie (romm_session)                      │
│     └─ Redis lookup → user from session["sub"]               │
│                                                              │
│  2. Check Authorization header                               │
│     ├─ "Basic ..." → bcrypt password verify                  │
│     ├─ "Bearer ..." → JWT validation (HS256)                 │
│     └─ "Bearer rmm_..." → Client API token (SHA-256 lookup)  │
│                                                              │
│  3. OIDC (if enabled)                                        │
│     └─ Token from OIDC provider → email match → user         │
│                                                              │
│  4. Kiosk mode (if enabled)                                  │
│     └─ Anonymous access with read-only scopes                │
│                                                              │
│  Falls through all methods → 401 Unauthorized                │
└──────────────────────────────────────────────────────────────┘
```

### Token Types

| Token            | Format                | Lifetime               | Storage                 |
| ---------------- | --------------------- | ---------------------- | ----------------------- |
| Access Token     | JWT (HS256)           | 30 min (configurable)  | Client-side             |
| Refresh Token    | JWT with JTI          | 7 days (configurable)  | JTI in Redis            |
| Session          | Cookie `romm_session` | 14 days (configurable) | Redis                   |
| Client API Token | `rmm_` + 64 hex chars | Configurable / never   | SHA-256 hash in DB      |
| CSRF Token       | Signed cookie         | Session lifetime       | Cookie + header         |
| Password Reset   | JWT with JTI          | 10 minutes             | JTI in Redis (one-time) |
| Invite Link      | JWT with JTI          | 10 minutes             | JTI in Redis (one-time) |

### Role-Based Access Control

| Role     | Scopes                                   | Description     |
| -------- | ---------------------------------------- | --------------- |
| `VIEWER` | Read all + write own profile             | Default role    |
| `EDITOR` | VIEWER + write ROMs, platforms, assets   | Content manager |
| `ADMIN`  | EDITOR + user management, task execution | Full access     |

### Scope Definitions

```
me.read / me.write           — Own profile
roms.read / roms.write       — ROM data
platforms.read / platforms.write — Platform data
assets.read / assets.write   — Saves, states, screenshots
devices.read / devices.write — Device management
firmware.read / firmware.write — BIOS files
collections.read / collections.write — Collections
users.read / users.write     — User management (admin)
tasks.run                    — Task execution
```

### CSRF Protection

- Cookie: `romm_csrftoken` (signed with `itsdangerous`)
- Header: `x-csrftoken`
- Both must match and contain the authenticated user's ID
- Exempt: `/api/token`, `/api/client-tokens/exchange`, `/api/client-tokens/pair/*/status`, `/ws`, `/netplay`
- Skipped for requests with `Authorization: Bearer` or `Authorization: Basic` headers

### Session Management

- Redis keys: `session:{session_id}`, `user_sessions:{username}`
- Cookie: `romm_session` (httponly, samesite=lax/strict)
- `clear_user_sessions(user_id)` on password change clears all sessions

---

## 8. Business Logic (Handlers)

### 8.1 Scan Handler (`handler/scan_handler.py`)

The core of RomM — orchestrates library scanning and metadata enrichment.

**Scan Types:**

| Type            | Behavior                         |
| --------------- | -------------------------------- |
| `NEW_PLATFORMS` | Detect new platform folders only |
| `QUICK`         | Scan new/unscanned ROMs          |
| `UPDATE`        | Rescan already-identified ROMs   |
| `UNMATCHED`     | Rescan ROMs without metadata     |
| `COMPLETE`      | Full rescan of everything        |
| `HASHES`        | Recalculate all file hashes      |

**Scan Flow:**

```
1. Detect platform folders in LIBRARY_BASE_PATH
2. For each platform:
   ├── Map filesystem slug to canonical platform (via config bindings)
   ├── Query metadata providers for platform info
   └── Create/update platform in DB
3. For each ROM file in platform:
   ├── Parse filename (extract name, tags, region, version)
   ├── Calculate file hashes (CRC32, MD5, SHA1)
   ├── Search metadata providers (in priority order):
   │   ├── IGDB
   │   ├── MobyGames
   │   ├── ScreenScraper
   │   ├── LaunchBox
   │   ├── RetroAchievements
   │   ├── Hasheous (hash-based matching)
   │   ├── Flashpoint
   │   ├── HLTB
   │   └── TheGamesDB
   ├── Download cover art and screenshots
   ├── Build aggregated metadata (RomMetadata)
   └── Create/update ROM in DB
4. Emit real-time progress via Socket.IO
5. Mark missing ROMs (files no longer on filesystem)
```

**Search Term Normalization:**

- Remove articles ("The", "A", "An")
- Strip punctuation and special characters
- Unicode normalization (NFKD)
- Jaro-Winkler similarity matching for fuzzy results

### 8.2 Database Handlers (`handler/database/`)

Each entity has a dedicated handler providing CRUD operations:

| Handler                       | Key Operations                                                                     |
| ----------------------------- | ---------------------------------------------------------------------------------- |
| `db_roms_handler`             | Advanced filtering (platform, genre, region, status), pagination, file association |
| `db_platform_handler`         | Slug mapping, ROM count aggregation                                                |
| `db_users_handler`            | Role management, credential storage                                                |
| `db_saves_handler`            | Slot-based grouping, device sync tracking                                          |
| `db_collections_handler`      | ROM association, smart collection evaluation                                       |
| `db_stats_handler`            | Platform/ROM counts, storage usage, metadata coverage                              |
| `db_devices_handler`          | Fingerprint deduplication                                                          |
| `db_device_save_sync_handler` | Cross-device sync state                                                            |
| `db_client_tokens_handler`    | Hash-based lookup, scope management                                                |

### 8.3 Metadata Handlers (`handler/metadata/`)

Each external provider has a handler that normalizes data into a common format:

| Handler              | Provider          | Key Data                                   |
| -------------------- | ----------------- | ------------------------------------------ |
| `igdb_handler`       | IGDB              | Game info, covers, screenshots, franchises |
| `moby_handler`       | MobyGames         | Publisher, genre classification            |
| `ss_handler`         | ScreenScraper     | Regional metadata, box art, manuals        |
| `sgdb_handler`       | SteamGridDB       | Grid artwork, logos, icons                 |
| `ra_handler`         | RetroAchievements | Achievements, user progression             |
| `hltb_handler`       | HowLongToBeat     | Playtime estimates                         |
| `hasheous_handler`   | Hasheous          | Hash-based ROM identification              |
| `tgdb_handler`       | TheGamesDB        | Alternative metadata                       |
| `flashpoint_handler` | Flashpoint        | Browser game archive                       |
| `gamelist_handler`   | gamelist.xml      | ES-DE format parser                        |
| `playmatch_handler`  | PlayMatch         | Game matching algorithm                    |
| `launchbox_handler/` | LaunchBox         | Local + remote database, media             |

**Priority system:** Metadata sources are queried in configurable priority order. First match wins for each field, with manual overrides taking highest priority.

### 8.4 Filesystem Handlers (`handler/filesystem/`)

| Handler             | Responsibility                                     |
| ------------------- | -------------------------------------------------- |
| `roms_handler`      | Read ROM files, calculate hashes, extract archives |
| `assets_handler`    | Store/retrieve saves, states, screenshots          |
| `firmware_handler`  | BIOS file management, verification                 |
| `platforms_handler` | Platform folder creation and detection             |
| `resources_handler` | Download and cache artwork                         |

**Supported archive formats:** ZIP, 7Z, TAR, GZIP, BZ2, RAR

**Special hash handling:**

- CHD (Compressed Hunks of Data) v5: SHA1 extracted from header
- PICO-8 cartridges (.p8.png): special handling
- RetroAchievements hash (`ra_hash`): platform-specific algorithm via `rahasher.py`
- Non-hashable platforms (Switch, PS3/4/5): hashing skipped

### 8.5 Netplay Handler (`handler/netplay_handler.py`)

Manages real-time multiplayer rooms stored in Redis:

```python
NetplayRoom:
    owner: str              # User ID
    players: dict           # sid → NetplayPlayerInfo
    peers: list[str]        # Peer IDs
    room_name: str
    game_id: str            # ROM ID
    domain: Optional[str]
    password: Optional[str]
    max_players: int
```

### 8.6 Socket Handler (`handler/socket_handler.py`)

Manages two Socket.IO servers:

| Server                   | Mount      | Purpose                              |
| ------------------------ | ---------- | ------------------------------------ |
| `socket_handler`         | `/ws`      | Scan progress, general notifications |
| `netplay_socket_handler` | `/netplay` | Netplay room management              |

Both use Redis as the message queue backend for horizontal scaling.

**Scan Progress Events:**

```python
ScanStats:
    total_platforms, scanned_platforms, new_platforms
    total_roms, scanned_roms, new_roms, identified_roms
    scanned_firmware, new_firmware
```

---

## 9. External Integrations (Adapters)

**Location:** `adapters/services/`

Each adapter wraps an external API with authentication, retry logic, and type safety.

### IGDB (Internet Game Database)

| Property        | Value                                                                   |
| --------------- | ----------------------------------------------------------------------- |
| **Auth**        | Twitch OAuth2 (client_id + client_secret → bearer token)                |
| **Data**        | Game metadata, covers, screenshots, age ratings, franchises, game modes |
| **Rate limits** | Retry logic with backoff                                                |
| **Config vars** | `IGDB_CLIENT_ID`, `IGDB_CLIENT_SECRET`                                  |

### MobyGames

| Property       | Value                                               |
| -------------- | --------------------------------------------------- |
| **Auth**       | API key                                             |
| **Data**       | Game metadata, publisher info, genre classification |
| **Config var** | `MOBYGAMES_API_KEY`                                 |

### ScreenScraper

| Property        | Value                                                         |
| --------------- | ------------------------------------------------------------- |
| **Auth**        | Device ID + user credentials                                  |
| **Data**        | Regional game metadata, box art, screenshots, manuals, bezels |
| **Media types** | Box 2D/3D, screenshot, video, manual, marquee, bezel          |
| **Config vars** | `SCREENSCRAPER_USER`, `SCREENSCRAPER_PASSWORD`                |

### SteamGridDB

| Property       | Value                                                    |
| -------------- | -------------------------------------------------------- |
| **Auth**       | Bearer token                                             |
| **Data**       | Grid artwork, logos, icons in multiple dimensions/styles |
| **Filters**    | Style, dimension, MIME type                              |
| **Config var** | `STEAMGRIDDB_API_KEY`                                    |

### RetroAchievements

| Property       | Value                                             |
| -------------- | ------------------------------------------------- |
| **Auth**       | API key (query parameter)                         |
| **Data**       | Game achievements, user progression, award badges |
| **Hash**       | Platform-specific hash via `rahasher.py`          |
| **Config var** | `RETROACHIEVEMENTS_API_KEY`                       |

### Additional Providers

| Provider      | Handler              | Description                                       |
| ------------- | -------------------- | ------------------------------------------------- |
| LaunchBox     | `launchbox_handler/` | Local XML database + remote API, platform mapping |
| HowLongToBeat | `hltb_handler`       | Game playtime estimates                           |
| Hasheous      | `hasheous_handler`   | Hash-based ROM identification                     |
| TheGamesDB    | `tgdb_handler`       | Alternative game metadata                         |
| Flashpoint    | `flashpoint_handler` | Browser game archive database                     |
| PlayMatch     | `playmatch_handler`  | Game matching algorithm                           |

### Static Fixture Data

Cached in Redis at startup from JSON files in `handler/metadata/fixtures/`:

| Fixture                 | Purpose                          |
| ----------------------- | -------------------------------- |
| `mame_index.json`       | MAME ROM name → game info        |
| `scummvm_index.json`    | ScummVM game identification      |
| `ps1_serial_index.json` | PS1 serial code → game mapping   |
| `ps2_serial_index.json` | PS2 serial code → game mapping   |
| `ps2_opl_index.json`    | PS2 OPL serial codes             |
| `psp_serial_index.json` | PSP serial code → game mapping   |
| `known_bios_files.json` | Verified BIOS file hashes (66KB) |

---

## 10. Real-Time Communication (WebSockets)

### Architecture

```
Client  ←──Socket.IO──→  FastAPI (python-socketio)  ←──Redis PubSub──→  Workers
```

### Scan Progress (`/ws`)

**Events emitted to clients:**

| Event               | Payload            | When                        |
| ------------------- | ------------------ | --------------------------- |
| `scan:update_stats` | `ScanStats` object | Each ROM/platform processed |
| `scan:log`          | Log message        | Scan log entries            |
| `scan:stop`         | —                  | Scan completed or cancelled |

### Netplay (`/netplay`)

**Events:**

| Event           | Direction       | Description             |
| --------------- | --------------- | ----------------------- |
| `open-room`     | Client → Server | Create netplay room     |
| `join-room`     | Client → Server | Join existing room      |
| `users-updated` | Server → Client | Player list changed     |
| Message relay   | Bidirectional   | Game data between peers |

Redis-backed for horizontal scaling across multiple server instances.

---

## 11. Background Tasks & Scheduling

### Job Queue System

**Technology:** RQ (Redis Queue)

**Priority Queues:**

| Queue             | Use Case                    |
| ----------------- | --------------------------- |
| `high_prio_queue` | Urgent operations           |
| `default_queue`   | Standard background work    |
| `low_prio_queue`  | Long-running scans, cleanup |

### Scheduled Tasks

Configured via environment variables and managed by RQ Scheduler:

| Task                              | Env Toggle                                         | Default Cron       | Description            |
| --------------------------------- | -------------------------------------------------- | ------------------ | ---------------------- |
| `scan_library`                    | `ENABLE_SCHEDULED_RESCAN`                          | `0 3 * * *` (3 AM) | Full library rescan    |
| `update_switch_titledb`           | `ENABLE_SCHEDULED_UPDATE_SWITCH_TITLEDB`           | `0 4 * * *`        | Update Switch game DB  |
| `update_launchbox_metadata`       | `ENABLE_SCHEDULED_UPDATE_LAUNCHBOX_METADATA`       | `0 4 * * *`        | Refresh LaunchBox data |
| `convert_images_to_webp`          | `ENABLE_SCHEDULED_CONVERT_IMAGES_TO_WEBP`          | `0 4 * * *`        | Image optimization     |
| `sync_retroachievements_progress` | `ENABLE_SCHEDULED_RETROACHIEVEMENTS_PROGRESS_SYNC` | `0 4 * * *`        | Sync RA user progress  |
| `cleanup_netplay`                 | Always enabled                                     | Periodic           | Clean stale rooms      |

### Manual Tasks

Triggered via `POST /api/tasks/run/{task_name}`:

| Task                         | Description                                   |
| ---------------------------- | --------------------------------------------- |
| `cleanup_missing_roms`       | Remove DB entries for files no longer on disk |
| `cleanup_orphaned_resources` | Remove unused artwork/resource files          |

### Filesystem Watcher

**File:** `watcher.py`

Uses `watchfiles` to monitor the library directory for changes. When enabled (`ENABLE_RESCAN_ON_FILESYSTEM_CHANGE`), triggers a rescan after a configurable delay (`RESCAN_ON_FILESYSTEM_CHANGE_DELAY`, default 5 minutes).

---

## 12. File System Management

### Directory Layout

```
{ROMM_BASE_PATH}/                    # Default: /romm
├── library/                         # ROM files, organized by platform
│   ├── n64/
│   │   └── roms/
│   │       ├── Game1.z64
│   │       └── Game2.z64
│   ├── psx/
│   │   └── roms/
│   │       └── Game.bin
│   └── {platform_slug}/
│       ├── roms/                    # Configurable folder name
│       └── bios/                    # Firmware/BIOS files
│
├── resources/                       # Cached metadata assets
│   └── roms/
│       └── {rom_id}/
│           ├── cover_s.webp         # Small cover
│           ├── cover_l.webp         # Large cover
│           └── screenshots/
│
├── assets/                          # User-generated assets
│   └── users/
│       └── {user_id}/
│           └── {rom_id}/
│               ├── saves/
│               ├── states/
│               └── screenshots/
│
└── config/
    └── config.yml                   # YAML configuration
```

### File Serving

| Mode        | Mechanism                | When            |
| ----------- | ------------------------ | --------------- |
| Development | `FileResponse` (direct)  | `DEV_MODE=true` |
| Production  | Nginx `X-Accel-Redirect` | Default         |

Nginx receives an internal redirect header and efficiently serves the file from disk without passing bytes through the Python process.

### Hash Calculation

| Algorithm | Used For                                                       |
| --------- | -------------------------------------------------------------- |
| CRC32     | Quick integrity check, standard ROM identification             |
| MD5       | Content deduplication (saves), BIOS verification               |
| SHA1      | ROM identification, BIOS verification                          |
| RA Hash   | RetroAchievements-specific hash (platform-dependent algorithm) |

Hashing can be disabled per-installation via `skip_hash_calculation` in config.yml.

---

## 13. Caching (Redis)

### Architecture

Two Redis client instances:

| Client         | Type                    | Purpose                        |
| -------------- | ----------------------- | ------------------------------ |
| `redis_client` | Sync (with auto-decode) | Cache queries, session lookups |
| `async_cache`  | Async                   | Async operations               |

Falls back to `FakeRedis` in test mode.

### Cache Key Patterns

| Pattern                    | TTL             | Content                         |
| -------------------------- | --------------- | ------------------------------- |
| `session:{id}`             | 14 days         | Session JSON                    |
| `user_sessions:{username}` | 14 days         | Set of session IDs              |
| `reset-jti:{jti}`          | 10 min          | Password reset token (one-time) |
| `invite-jti:{jti}`         | 10 min          | Invite token (one-time)         |
| `refresh-jti:{jti}`        | 7 days          | Refresh token validation        |
| `romm:mame_index`          | Permanent       | MAME game index                 |
| `romm:scummvm_index`       | Permanent       | ScummVM game index              |
| `romm:ps1_serials`         | Permanent       | PS1 serial codes                |
| `romm:ps2_serials`         | Permanent       | PS2 serial codes                |
| `romm:psp_serials`         | Permanent       | PSP serial codes                |
| `romm:switch_titledb`      | Refreshed daily | Switch TitleDB                  |
| `romm:known_bios`          | Permanent       | Verified BIOS hashes            |
| Upload sessions            | 24 hours        | Chunked upload state            |
| Netplay rooms              | Dynamic         | Active room state               |

---

## 14. Configuration

### Environment Variables

#### Core

| Variable         | Default          | Description          |
| ---------------- | ---------------- | -------------------- |
| `ROMM_BASE_PATH` | `/romm`          | Base data directory  |
| `ROMM_BASE_URL`  | `http://0.0.0.0` | Application base URL |
| `ROMM_PORT`      | `8080`           | Server port          |
| `DEV_MODE`       | `false`          | Development mode     |
| `LOGLEVEL`       | `INFO`           | Log level            |

#### Database

| Variable         | Default   | Description                         |
| ---------------- | --------- | ----------------------------------- |
| `ROMM_DB_DRIVER` | `mariadb` | `mariadb`, `mysql`, or `postgresql` |
| `DB_HOST`        | —         | Database host                       |
| `DB_PORT`        | `3306`    | Database port                       |
| `DB_USER`        | —         | Database user                       |
| `DB_PASSWD`      | —         | Database password                   |
| `DB_NAME`        | `romm`    | Database name                       |

#### Redis

| Variable         | Default     | Description           |
| ---------------- | ----------- | --------------------- |
| `REDIS_HOST`     | `127.0.0.1` | Redis host            |
| `REDIS_PORT`     | `6379`      | Redis port            |
| `REDIS_PASSWORD` | —           | Redis password        |
| `REDIS_DB`       | `0`         | Redis database number |
| `REDIS_SSL`      | `false`     | Enable SSL            |

#### Authentication

| Variable                             | Default   | Description                           |
| ------------------------------------ | --------- | ------------------------------------- |
| `ROMM_AUTH_SECRET_KEY`               | —         | **Required.** JWT/session signing key |
| `OAUTH_ACCESS_TOKEN_EXPIRE_SECONDS`  | `1800`    | 30 minutes                            |
| `OAUTH_REFRESH_TOKEN_EXPIRE_SECONDS` | `604800`  | 7 days                                |
| `SESSION_MAX_AGE_SECONDS`            | `1209600` | 14 days                               |
| `DISABLE_CSRF_PROTECTION`            | `false`   | Disable CSRF                          |
| `DISABLE_DOWNLOAD_ENDPOINT_AUTH`     | `false`   | Allow unauthenticated downloads       |
| `DISABLE_USERPASS_LOGIN`             | `false`   | Disable password login                |
| `KIOSK_MODE`                         | `false`   | Read-only anonymous access            |

#### OIDC

| Variable                        | Default              | Description           |
| ------------------------------- | -------------------- | --------------------- |
| `OIDC_ENABLED`                  | `false`              | Enable OpenID Connect |
| `OIDC_PROVIDER`                 | —                    | Provider URL          |
| `OIDC_CLIENT_ID`                | —                    | Client ID             |
| `OIDC_CLIENT_SECRET`            | —                    | Client secret         |
| `OIDC_REDIRECT_URI`             | —                    | Redirect URI          |
| `OIDC_USERNAME_ATTRIBUTE`       | `preferred_username` | Username claim        |
| `OIDC_CLAIM_ROLES`              | —                    | Roles claim name      |
| `OIDC_ROLE_VIEWER/EDITOR/ADMIN` | —                    | Role mappings         |

#### API Keys

| Variable                                        | Description       |
| ----------------------------------------------- | ----------------- |
| `IGDB_CLIENT_ID` + `IGDB_CLIENT_SECRET`         | IGDB (via Twitch) |
| `MOBYGAMES_API_KEY`                             | MobyGames         |
| `SCREENSCRAPER_USER` + `SCREENSCRAPER_PASSWORD` | ScreenScraper     |
| `STEAMGRIDDB_API_KEY`                           | SteamGridDB       |
| `RETROACHIEVEMENTS_API_KEY`                     | RetroAchievements |

#### Feature Toggles

| Variable                 | Default | Description             |
| ------------------------ | ------- | ----------------------- |
| `LAUNCHBOX_API_ENABLED`  | `false` | LaunchBox metadata      |
| `PLAYMATCH_API_ENABLED`  | `false` | PlayMatch matching      |
| `HASHEOUS_API_ENABLED`   | `false` | Hasheous identification |
| `TGDB_API_ENABLED`       | `false` | TheGamesDB              |
| `FLASHPOINT_API_ENABLED` | `false` | Flashpoint archive      |
| `HLTB_API_ENABLED`       | `false` | HowLongToBeat           |

#### Task Scheduling

| Variable                             | Default     | Description              |
| ------------------------------------ | ----------- | ------------------------ |
| `SCAN_TIMEOUT`                       | `14400`     | 4-hour scan timeout      |
| `SCAN_WORKERS`                       | `1`         | Concurrent scan workers  |
| `ENABLE_SCHEDULED_RESCAN`            | `false`     | Auto library rescan      |
| `SCHEDULED_RESCAN_CRON`              | `0 3 * * *` | Rescan schedule          |
| `ENABLE_RESCAN_ON_FILESYSTEM_CHANGE` | `false`     | Watch for file changes   |
| `RESCAN_ON_FILESYSTEM_CHANGE_DELAY`  | `5`         | Debounce delay (minutes) |

### YAML Configuration (`config.yml`)

```yaml
exclude:
  platforms: ["arcade"]
  roms:
    single_file:
      extensions: [".txt", ".nfo"]
      names: ["readme"]
    multi_file:
      names: ["__MACOSX"]

filesystem:
  roms_folder: "roms" # Subfolder name for ROMs
  firmware_folder: "bios" # Subfolder name for BIOS
  skip_hash_calculation: false

system:
  platforms:
    snes: "snes" # fs_slug → canonical slug mappings
  versions:
    snes: "pal" # Platform version overrides

scan:
  priority:
    metadata: ["igdb", "moby", "ss"] # Provider priority
    artwork: ["sgdb", "igdb", "ss"]
    region: ["us", "eu", "jp"]
    language: ["en", "es", "ja"]
  media: ["box2d", "screenshot", "manual"]
  export_gamelist: false

emulatorjs:
  debug: false
  netplay:
    enabled: false
    ice_servers:
      - urls: "stun:stun.l.google.com:19302"
  settings:
    nes:
      option_name: option_value
  controls:
    nes:
      0: { 0: { value: "x" } }
```

Managed by `ConfigManager` (singleton pattern) which reads, validates, and writes the YAML file.

---

## 15. Error Handling

### Exception Hierarchy

```
Exception
├── AuthCredentialsException          # 401 — Incorrect credentials
├── AuthenticationSchemeException      # 401 — Invalid auth scheme
├── UserDisabledException             # 401 — Account disabled
├── OAuthCredentialsException         # 401 — Invalid OAuth token
├── OIDCDisabledException             # 500 — OIDC not configured
├── OIDCNotConfiguredException        # 500 — OIDC feature disabled
│
├── PlatformNotFoundInDatabaseException    # 404
├── RomNotFoundInDatabaseException         # 404
├── CollectionNotFoundInDatabaseException  # 404
├── CollectionPermissionError              # 403
├── CollectionAlreadyExistsException       # 500
├── RomNotFoundInRetroAchievementsException # 404
├── SGDBInvalidAPIKeyException             # 401
│
├── FolderStructureNotMatchException  # Invalid library layout
├── PlatformNotFoundException         # Platform not found in FS
├── PlatformAlreadyExistsException    # Duplicate platform
├── RomsNotFoundException             # No ROMs for platform
├── RomAlreadyExistsException         # Duplicate ROM
├── FirmwareNotFoundException         # Firmware not found
│
├── ConfigNotWritableException        # Config file not writable
├── SchedulerException                # Task scheduling error
└── ScanStoppedException              # Scan cancelled
```

### HTTP Status Codes

| Code | Meaning               |
| ---- | --------------------- |
| 200  | Success (GET, PUT)    |
| 201  | Created (POST)        |
| 204  | No Content (DELETE)   |
| 400  | Bad Request           |
| 401  | Unauthorized          |
| 403  | Forbidden             |
| 404  | Not Found             |
| 409  | Conflict (duplicate)  |
| 500  | Internal Server Error |

---

## 16. Logging

### Setup

**Logger name:** `"romm"`
**Level:** Configurable via `LOGLEVEL` env var (default `INFO`)

### Format

```
[LEVEL]: [RomM][module] [timestamp] message
```

### Color Coding

| Level    | Color         |
| -------- | ------------- |
| DEBUG    | Light Magenta |
| INFO     | Green         |
| WARNING  | Yellow        |
| ERROR    | Light Red     |
| CRITICAL | Red           |

Color behavior:

- `FORCE_COLOR=true` → Always use colors
- `NO_COLOR=true` → Strip ANSI codes
- Default → Colors enabled

### Monitoring

- **Sentry** integration via `SENTRY_DSN` environment variable
- Release tagged as `romm@{version}`

---

## 17. Testing

### Configuration

**File:** `pytest.ini`

- Async mode enabled
- Test database: `romm_test`
- Mock API keys pre-configured
- OIDC disabled for tests
- Log level: DEBUG

### Test Structure

Tests mirror the backend directory structure under `tests/`:

```
tests/
├── conftest.py                    # Shared fixtures
├── adapters/services/             # API adapter tests
│   └── cassettes/                 # VCR recorded API responses
├── config/                        # Configuration tests
├── endpoints/                     # Endpoint integration tests
│   ├── test_auth.py
│   ├── test_collections.py
│   ├── roms/
│   └── sockets/
├── handler/                       # Handler unit tests
│   ├── auth/
│   ├── database/
│   ├── filesystem/
│   └── metadata/
├── logger/                        # Logger tests
├── models/                        # Model tests
├── tasks/                         # Task tests
└── utils/                         # Utility tests
```

### Test Fixtures

Located in `romm_test/`:

- Test ROM library with real directory structure (n64, ps3, psp, psvita, psx)
- User asset fixtures
- Configuration fixtures
- VCR cassettes for external API responses (pre-recorded)

### Key Patterns

- **VCR cassettes** for external API tests (reproducible without network)
- **Test database** (separate `romm_test` DB)
- **FastAPI TestClient** for endpoint integration tests
- **Mock Redis** via FakeRedis

---

## Appendix: Key Design Patterns

| Pattern                     | Where                                | Purpose                           |
| --------------------------- | ------------------------------------ | --------------------------------- |
| **Three-tier architecture** | Endpoints → Handlers → Models        | Separation of concerns            |
| **Singleton**               | `ConfigManager`                      | Single config instance            |
| **Adapter pattern**         | `adapters/services/`                 | Normalize external APIs           |
| **Decorator pattern**       | `@protected_route`, `@begin_session` | Cross-cutting concerns            |
| **Context variables**       | `utils/context.py`                   | Request-scoped state (async-safe) |
| **Repository pattern**      | `handler/database/`                  | Encapsulate data access           |
| **Observer pattern**        | Socket.IO events                     | Real-time updates                 |
| **Priority queue**          | RQ with 3 priority levels            | Task scheduling                   |
| **Chunked upload**          | `endpoints/roms/upload.py`           | Large file handling               |
| **X-Accel-Redirect**        | `utils/nginx.py`                     | Efficient file serving            |
