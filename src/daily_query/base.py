import abc


class NoSQLDaily(abc.ABC):
    """
    Contract for NoSQL engines to provide helpers for
    opinionated data management that assumes that daily data
    is stored under a distinct collection named after that date.
    """

    @abc.abstractmethod
    def search(self, flatten=False, **kwargs):
        pass

    @abc.abstractmethod
    def find(self, from_date=None, to_date=None, first=None, filter=None,
            fields=None, exclude=None):
        pass

    @abc.abstractmethod
    def get_collections(self, from_date=None, to_date=None):
        pass


