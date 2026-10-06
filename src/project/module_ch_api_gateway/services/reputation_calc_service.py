import asyncio
import logging
from datetime import timedelta
from typing import Optional

import asyncpg

from project.module_ch_api_gateway.infrastructure.reputation_calc_client import ReputationCalcClient
from project.module_ch_api_gateway.infrastructure.reputation_calc_repo import ReputationCalcRepository

logger = logging.getLogger("ch-api-gateway.reputation_calcs")

CALC_PERIOD_DAYS = 7
COUNT_RETRY_DELAY = 2.0


class CalcBusyError(Exception):
    pass


class CalcCountError(Exception):
    pass


class ReputationCalcService:
    def __init__(self, repo: ReputationCalcRepository, ch: ReputationCalcClient):
        self.repo = repo
        self.ch = ch
        self._background_tasks: set[asyncio.Task] = set()

    @property
    def is_available(self) -> bool:
        return self.repo.is_connected

    async def start_calc(self, source: str, profile: Optional[str], created_by: str) -> dict:
        try:
            row = await self.repo.create_calc(source, profile, created_by)
        except asyncpg.UniqueViolationError as e:
            if e.constraint_name == "idx_reputation_calcs_one_per_user":
                raise CalcBusyError("У вас уже идёт расчёт, дождитесь его завершения")
            raise CalcBusyError("Расчёт по этому источнику и профилю уже идёт, дождитесь его завершения")

        logger.info(
            "action=reputation_calc_started id=%d source=%s profile=%s user=%s",
            row["id"], source, profile, created_by,
        )
        task = asyncio.create_task(self._run(row["id"], source, profile))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return dict(row)

    async def _run(self, calc_id: int, source: str, profile: Optional[str]) -> None:
        try:
            period_to = await self.ch.now()
            period_from = period_to - timedelta(days=CALC_PERIOD_DAYS)
            await self.repo.set_period(calc_id, period_from, period_to)

            written = await self.ch.calc(calc_id, period_to, source, profile)
            count = await self.ch.count(calc_id)
            if count != written:
                await asyncio.sleep(COUNT_RETRY_DELAY)
                count = await self.ch.count(calc_id)
            if count != written:
                raise CalcCountError(f"Результат записан не полностью: рассчитано {written}, в ClickHouse {count}")

            await self.repo.finish_calc(calc_id, count)
            logger.info(
                "action=reputation_calc_done id=%d source=%s profile=%s rows=%d",
                calc_id, source, profile, count,
            )
        except CalcCountError as e:
            logger.error("action=reputation_calc_failed id=%d error=%s", calc_id, str(e))
            await self._fail(calc_id, str(e))
        except Exception as e:
            logger.error("action=reputation_calc_failed id=%d error=%s", calc_id, str(e))
            await self._fail(calc_id, "Ошибка расчёта, подробности в логе")

    async def _fail(self, calc_id: int, error: str) -> None:
        try:
            await self.ch.clear(calc_id)
        except Exception as e:
            logger.error("action=reputation_calc_clear_failed id=%d error=%s", calc_id, str(e))
        try:
            await self.repo.fail_calc(calc_id, error)
        except Exception as e:
            logger.error("action=reputation_calc_fail_save_failed id=%d error=%s", calc_id, str(e))

    async def fail_stale_calcs(self) -> None:
        for row in await self.repo.get_building_calcs():
            logger.warning("action=reputation_calc_interrupted id=%d", row["id"])
            await self._fail(row["id"], "Расчёт прерван перезапуском сервиса")
