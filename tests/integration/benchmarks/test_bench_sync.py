from contextlib import nullcontext

import pytest
from pytest_codspeed import BenchmarkFixture

from odmantic import AIOEngine, SyncEngine

from .models import VALID_LEVELS, SmallJournal


@pytest.mark.parametrize("count", [10, 50, 100])
@pytest.mark.parametrize(
    "use_session", [pytest.param(True, id="session"), pytest.param(False, id="direct")]
)
def test_insert_small_single(
    benchmark, aio_engine: AIOEngine, count: int, use_session: bool
):
    instances = list(SmallJournal.get_random_instances("test_write_small", count))

    @benchmark
    async def _():
        with aio_engine.session() if use_session else nullcontext(aio_engine) as engine:
            for instance in instances:
                engine.save(instance)


@pytest.mark.parametrize("count", [10, 50, 100])
@pytest.mark.parametrize(
    "use_session", [pytest.param(True, id="session"), pytest.param(False, id="direct")]
)
def test_write_small_bulk(
    benchmark, sync_engine: SyncEngine, count: int, use_session: bool
):
    instances = list(SmallJournal.get_random_instances("test_write_small", count))

    @benchmark
    def _():
        with sync_engine.session() if use_session else nullcontext(
            sync_engine
        ) as engine:
            engine.save_all(instances)


@pytest.mark.parametrize("count", [10, 50, 100])
def test_filter_by_level_small(benchmark, sync_engine: SyncEngine, count: int):
    instances = list(SmallJournal.get_random_instances("test_write_small", count))
    sync_engine.save_all(instances)

    @benchmark
    def _():
        total = 0
        for level in VALID_LEVELS:
            total += len(
                list(sync_engine.find(SmallJournal, SmallJournal.level == level))
            )


@pytest.mark.parametrize("count", [10, 50, 100])
def test_filter_limit_skip_by_level_small(
    benchmark, sync_engine: SyncEngine, count: int
):
    instances = list(SmallJournal.get_random_instances("test_write_small", count))
    sync_engine.save_all(instances)

    @benchmark
    def _():
        total = 0
        for level in VALID_LEVELS:
            total += len(
                list(
                    sync_engine.find(
                        SmallJournal, SmallJournal.level == level, limit=20, skip=20
                    )
                )
            )


@pytest.mark.parametrize("count", [10, 50, 100])
def test_find_one_by_id(benchmark, sync_engine: SyncEngine, count: int):
    instances = list(SmallJournal.get_random_instances("test_write_small", count))
    sync_engine.save_all(instances)
    ids = [instance.id for instance in instances]

    @benchmark
    def _():
        for id_ in ids:
            sync_engine.find_one(SmallJournal, SmallJournal.id == id_)
