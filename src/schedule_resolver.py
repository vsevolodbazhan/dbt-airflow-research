from typing import Iterable

import attrs
import pendulum
import pytest
from croniter import croniter, croniter_range


class FailedToResolveDependencyLogicalDate(Exception):
    pass


def min_or_none(iterable):
    try:
        return min(iterable)
    except ValueError:
        return None


def max_or_none(iterable):
    try:
        return max(iterable)
    except ValueError:
        return None


def resolve_upstream_logical_dates(
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
    upstream_schedule: str,
) -> list[pendulum.DateTime]:
    try:
        # We try to find the latest logical dates within the data interval
        # that satisfies the upstream schedule.
        result = list(
            croniter_range(
                # We want to include the data interval start in the search
                # but exclude the data interval end hence the -1 second.
                start=data_interval_start,
                stop=data_interval_end - pendulum.duration(seconds=1),
                expr_format=upstream_schedule,
                ret_type=pendulum.DateTime,
                exclude_ends=False,
            )
            # If we can't find any logical date within the data interval
            # that satisfies the upstream schedule, we try to find the latest
            # logical date up to the end of the data interval.
        ) or [
            max(
                croniter_range(
                    start=data_interval_start - pendulum.duration(years=1),
                    stop=data_interval_end,
                    expr_format=upstream_schedule,
                    ret_type=pendulum.DateTime,
                    exclude_ends=True,
                )
            )
        ]
        return result
    except Exception as error:
        raise FailedToResolveDependencyLogicalDate from error


@attrs.frozen
class DagRun:
    data_interval_start: pendulum.DateTime = attrs.field(converter=pendulum.parse)  # type: ignore
    data_interval_end: pendulum.DateTime = attrs.field(converter=pendulum.parse)  # type: ignore
    schedule: str = attrs.field()

    @schedule.validator  # type: ignore
    def data_interval_corresponds_to_schedule(self, attribute, value):
        for _datetime in [self.data_interval_start, self.data_interval_end]:
            assert croniter.match(
                value, _datetime
            ), f"'{_datetime}' does not match schedule '{value}'."


@pytest.mark.parametrize(
    [
        'downstream_run',
        'upstream_schedule',
        'expected_upstream_logical_dates',
    ],
    [
        pytest.param(
            DagRun(
                '2024-01-01 00:00:00',
                '2024-01-01 01:00:00',
                '0 * * * *',
            ),
            '0 * * * *',
            ['2024-01-01 00:00:00'],
            id='downstream_has_same_schedule_as_upstream',
        ),
        pytest.param(
            DagRun(
                '2024-01-01 00:00:00',
                '2024-01-01 01:00:00',
                '0 * * * *',
            ),
            '30 0 * * *',
            ['2024-01-01 00:30:00'],
            id='downstream_hourly_and_upstream_is_daily',
        ),
        pytest.param(
            DagRun(
                '2024-01-01 00:00:00',
                '2024-01-01 01:00:00',
                '0 * * * *',
            ),
            '30 0 * * 5',
            ['2023-12-29 00:30:00'],
            id='downstream_hourly_and_upstream_is_weekly',
        ),
        pytest.param(
            DagRun(
                '2024-01-01 00:00:00',
                '2024-01-01 01:00:00',
                '0 * * * *',
            ),
            '30 0 5 * *',
            ['2023-12-05 00:30:00'],
            id='downstream_hourly_and_upstream_is_monthly',
        ),
        pytest.param(
            DagRun(
                '2024-01-01 00:00:00',
                '2024-01-01 01:00:00',
                '0 * * * *',
            ),
            '30 0 5 10 *',
            ['2023-10-05 00:30:00'],
            id='downstream_hourly_and_upstream_is_yearly',
        ),
        pytest.param(
            DagRun(
                '2024-01-01 00:00:00',
                '2024-01-02 00:00:00',
                '0 0 * * *',
            ),
            '30 * * * *',
            (
                logical_date.isoformat()
                for logical_date in pendulum.interval(
                    pendulum.parse('2024-01-01 00:30:00'),  # type: ignore
                    pendulum.parse('2024-01-01 23:30:00'),  # type: ignore
                ).range(unit='hours')
            ),
            id='downstream_daily_and_upstream_is_hourly',
        ),
        pytest.param(
            DagRun(
                '2024-01-01 00:00:00',
                '2024-01-02 00:00:00',
                '0 0 * * *',
            ),
            '0 0 * * *',
            ['2024-01-01 00:00:00'],
            id='downstream_daily_and_upstream_is_daily',
        ),
        pytest.param(
            DagRun(
                '2024-01-01 00:00:00',
                '2024-01-02 00:00:00',
                '0 0 * * *',
            ),
            '30 0 * * 5',
            ['2023-12-29 00:30:00'],
            id='downstream_daily_and_upstream_is_weekly',
        ),
        pytest.param(
            DagRun(
                '2024-01-01 00:00:00',
                '2024-01-02 00:00:00',
                '0 0 * * *',
            ),
            '30 0 5 * *',
            ['2023-12-05 00:30:00'],
            id='downstream_daily_and_upstream_is_monthly',
        ),
        pytest.param(
            DagRun(
                '2024-01-01 00:00:00',
                '2024-01-02 00:00:00',
                '0 0 * * *',
            ),
            '30 0 5 10 *',
            ['2023-10-05 00:30:00'],
            id='downstream_daily_and_upstream_is_yearly',
        ),
        pytest.param(
            DagRun(
                '2023-12-29 00:00:00',
                '2024-01-05 00:00:00',
                '0 0 * * 5',
            ),
            '30 * * * *',
            (
                logical_date.isoformat()
                for logical_date in pendulum.interval(
                    pendulum.parse('2023-12-29 00:30:00'),  # type: ignore
                    pendulum.parse('2024-01-04 23:30:00'),  # type: ignore
                ).range(unit='hours')
            ),
            id='downstream_weekly_and_upstream_is_hourly',
        ),
    ],
)
def test_upstream_logical_date_resolves_correctly(
    downstream_run: DagRun,
    upstream_schedule: str,
    expected_upstream_logical_dates: Iterable[str],
):
    resolved_upstream_logical_dates = map(
        lambda logical_date: logical_date.isoformat(),
        resolve_upstream_logical_dates(
            data_interval_start=downstream_run.data_interval_start,
            data_interval_end=downstream_run.data_interval_end,
            upstream_schedule=upstream_schedule,
        ),
    )
    _expected_upstream_logical_dates = map(
        lambda logical_date: pendulum.parse(logical_date).isoformat(),  # type: ignore
        expected_upstream_logical_dates,
    )
    assert list(resolved_upstream_logical_dates) == list(
        _expected_upstream_logical_dates
    )
