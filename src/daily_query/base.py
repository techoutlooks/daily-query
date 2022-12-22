import abc
import collections
from typing import Union, Tuple, Type


class Collection(abc.ABC):

    @property
    @abc.abstractmethod
    def name(self):
        pass

    @property
    @abc.abstractmethod
    def database(self):
        """ An alias for `self.db` set by parent class Pymongo. """
    pass

    @property
    @abc.abstractmethod
    def collection(self):
        """ Returns the cached collection object """
        pass

    @abc.abstractmethod
    def count(self):
        pass

    @abc.abstractmethod
    def find(self, match=None, projection=None):
        pass

    @abc.abstractmethod
    def find_one(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def update_one(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def insert_many(self, docs, **kwargs, ):
        pass

    @abc.abstractmethod
    def update_or_create(self, defaults: dict, **kwargs):
        """ Similar to Django's `.update_or_create()`, tries to fetch an object
        from the database based on **kwargs** match. If mached, uses **defaults**
        to update the object found, else to create a new object .
        """
        pass

    @abc.abstractmethod
    def aggregate(self, *args, **kwargs):
        pass


class Doc(collections.UserDict):
    """ Collection-aware document  """

    def __init__(self, collection, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collection = collection


class NoSQLDaily(abc.ABC):
    """
    Contract for NoSQL engines to provide helpers for
    opinionated data management that assumes that daily data
    is stored under a distinct collection named after that date.
    """

    @abc.abstractmethod
    def search(self, flatten=False, **kwargs) -> Union[Doc, Tuple[Type[Collection], Type[dict]]]:
        pass

    @abc.abstractmethod
    def find(self, from_date=None, to_date=None, first=None, filter=None,
            fields=None, exclude=None):
        pass

    @abc.abstractmethod
    def get_collections(self, from_date=None, to_date=None):
        pass


