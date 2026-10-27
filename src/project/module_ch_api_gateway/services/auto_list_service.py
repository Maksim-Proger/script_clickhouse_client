import asyncio
import logging
from datetime import datetime

import asyncpg

from project.module_ch_api_gateway.models.filters import ReputationFilters
from project.module_ch_api_gateway.services.feed_list_service import FeedListService, check_source_size

logger = logging.getLogger("ch-api-gateway.auto_lists")

MIN_SCORE_SETTING = "feed_lists.min_score"
UNKNOWN_COUNTRY = "ZZ"
DEFAULT_MIN_SCORE = 50.0
DEFAULT_CREATED_BY = "system:auto-lists"
DEFAULT_INTERVAL = 60
DEFAULT_CIS = ["RU", "BY", "AM", "KZ", "GE", "AZ", "KG", "UZ", "TJ", "MD"]


class AutoListService:
    def __init__(self, feed_service: FeedListService, reputation_service, cfg: dict):
        self.feed_service = feed_service
        self.repo = feed_service.repo
        self.reputation_service = reputation_service

        names = cfg.get("names", {})
        cis = cfg.get("cis_countries", DEFAULT_CIS)
        self.lists = [
            (names.get("all", "all"), []),
            (names.get("not_ru", "not-ru"), ["RU", UNKNOWN_COUNTRY]),
            (names.get("not_cis", "not-cis"), list(cis) + [UNKNOWN_COUNTRY]),
        ]
        self.names = {name for name, _ in self.lists}
        self.min_score = float(cfg.get("min_score", DEFAULT_MIN_SCORE))
        self.created_by = cfg.get("created_by", DEFAULT_CREATED_BY)
        self.interval = cfg.get("interval_sec", DEFAULT_INTERVAL)

    @property
    def is_available(self) -> bool:
        return self.feed_service.is_available

    async def current_min_score(self) -> float:
        raw = await self.repo.get_setting(MIN_SCORE_SETTING)
        if raw is None:
            return self.min_score
        try:
            return float(raw)
        except ValueError:
            logger.error("action=auto_lists_bad_setting key=%s value=%s", MIN_SCORE_SETTING, raw)
            return self.min_score

    async def check(self) -> None:
        snapshot = await self.reputation_service.latest_snapshot()
        if snapshot is None:
            return
        run_id = snapshot["run_id"]
        computed_at = datetime.strptime(snapshot["computed_at"], "%Y-%m-%d %H:%M:%S")
        min_score = await self.current_min_score()

        for name, countries in self.lists:
            try:
                await self._check_list(name, countries, run_id, computed_at, min_score)
            except Exception as e:
                logger.error("action=auto_list_check_failed name=%s run_id=%s error=%s", name, run_id, str(e))

    async def _check_list(self,
                          name: str,
                          countries: list[str],
                          run_id: str,
                          computed_at: datetime,
                          min_score: float) -> None:
        list_id = None
        card = await self.repo.get_catalog_row_by_name(name)
        if card is not None:
            if not card["is_system"]:
                logger.error("action=auto_list_name_taken name=%s id=%d", name, card["id"])
                return
            if card["pending_version"] is not None:
                return
            list_id = card["id"]
            if card["current_version"] is not None:
                current = await self.repo.get_version(list_id, card["current_version"])
                if current["snapshot_run_id"] == run_id and current["min_score"] == min_score:
                    return

        filters = ReputationFilters(score_from=min_score, country=countries, country_exclude=bool(countries))
        total = await self.reputation_service.count_snapshot(filters, run_id=run_id)
        if total == 0:
            logger.info("action=auto_list_skipped name=%s run_id=%s min_score=%s reason=empty",
                        name, run_id, min_score)
            return
        check_source_size(total)

        clean = {"score_from": min_score}
        if countries:
            clean["country"] = countries
            clean["country_exclude"] = True
        source_filters = {"source": "reputation", "filters": clean}

        async def build(lid, version):
            await self.feed_service.build_from_reputation_snapshot(
                lid, version, self.reputation_service, filters, run_id=run_id,
            )

        try:
            list_id, version = await self.feed_service.start_auto_version(
                list_id, name, self.created_by, source_filters, run_id, computed_at, min_score, build,
            )
        except asyncpg.UniqueViolationError:
            logger.warning("action=auto_list_busy name=%s", name)
            return

        logger.info(
            "action=auto_list_version_started name=%s id=%d version=%d run_id=%s min_score=%s rows=%d",
            name, list_id, version, run_id, min_score, total,
        )


async def auto_lists_loop(service: AutoListService) -> None:
    while True:
        try:
            await asyncio.sleep(service.interval)
            if not service.is_available:
                continue
            await service.check()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("action=auto_lists_loop_error error=%s", str(e))
