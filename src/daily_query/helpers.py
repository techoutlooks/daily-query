import os
import datetime
import unicodedata

import six
from ordered_set import OrderedSet

from daily_query.constants import FOREVER


__all__ = (
    'isiterable',
    'parse_dates', 'mk_datetime', 'mk_date'
)


DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = f'{DATE_FORMAT} %H:%M:%S'


mk_date = lambda x=None: mk_datetime(x, True)


def mk_datetime(input=None, date_only=False) -> datetime.datetime:
    """
    Create a datetime obj from anything.
    ::param input: `str` date/time, `datetime.date` or `datetime.datetime`
    ::returns corresponding `datetime` obj,
              or the current datetime if no input was passed in.
    """
    date_time = input
    if not input:
        date_time = datetime.datetime.now()  # careful, now() is both datetime and date!
    if isinstance(date_time, datetime.date):
        if not isinstance(date_time, datetime.datetime):
            date_time = str(date_time)
    if isinstance(date_time, str):
        if len(date_time.split()) == 1:
            date_time = f"{str(date_time)} 00:00:00"
        date_time = str(date_time).split('.')[0]
        date_time = datetime.datetime.strptime(date_time, DATETIME_FORMAT)
    if date_only:
        date_time = date_time.date()
    return date_time


def parse_dates(days=None, days_from=None, days_to=None, reverse=True) \
        -> [datetime.date]:
    """
    Compile given date range and days into a list of days
    sorted by date descending (default) / ascending date.

    `days_from`, `days_to` params expected as '%Y-%m-%d' string or datetime
        cf. `utils.DATE_FORMAT`
    """

    days_range = []

    # guard: converts ['None', 'None] => [None, None]
    days_from, days_to = list(
        map(lambda x: None if x == 'None' else x, [days_from, days_to]))

    if (days_from or days_to) or not days:
        start_date, end_date = mk_date(days_from or FOREVER), mk_date(days_to)
        assert start_date <= end_date, \
            f"Must have: `days_to ({days_to}) > days_from({days_from})`"

        days_range = [start_date + datetime.timedelta(x)
                      for x in range((end_date - start_date).days + 1)]

    days = [mk_date(d) for d in days] if days else []
    all_days = OrderedSet([*days_range, *days])
    all_days.items.sort(reverse=reverse)

    return all_days


def isiterable(obj):
    """ Iterable, but not: string, dict """
    return hasattr(obj, '__iter__') and \
        not isinstance(obj, six.string_types)
