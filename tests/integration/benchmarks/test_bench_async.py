from contextlib import nullcontext

import pytest
from pytest_codspeed import BenchmarkFixture

from odmantic import AIOEngine, SyncEngine

from .models import VALID_LEVELS, SmallJournal

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("count", [10, 50, 100])
async def test_insert_small_single(
    benchmark, aio_engine: AIOEngine, count: int, use_session: bool
):
    instances = list(SmallJournal.get_random_instances("test_write_small", count))

    @benchmark
    async def _():
        async with aio_engine.session() as session:
            for instance in instances:
                await session.save(instance)
