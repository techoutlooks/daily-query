import datetime
import pymongo

from bson import ObjectId
from daily_query.base import NoSQLDaily
from daily_query.helpers import mk_date, parse_dates
from ordered_set import OrderedSet

from .constants import *


__all__ = ['PyMongo', 'Collection', 'MongoDaily']


class PyMongo:
    """
    Initializes a MongoDB using pymongo
    """

    db = None

    def __init__(self, db_or_uri):

        if not isinstance(db_or_uri, str):
            self.db = db_or_uri
        else:
            mongo_client = pymongo.MongoClient(db_or_uri)

            # better error formatting than **mongoclient.get_default_database()**
            db_name = pymongo.uri_parser.parse_uri(db_or_uri).get('database') \
                if db_or_uri else None
            if not db_name:
                raise pymongo.errors.ConfigurationError(
                    f"Wrong mongo uri - no database found: {db_or_uri}.\n"
                    "Scheme: mongodb://[username:password@]host[:port][/[default_db][?options]]"
                )
            self.db = mongo_client[db_name]

    def __del__(self):
        if self.db:
            self.db.client.close()
            del self.db


class Collection(PyMongo):
    """
    Raw access to MongoDB  collection enhanced.
    Wraps a `pymongo.collection.Collection`.

    Use cases:

    - initialize a collection from either a name+db (db obj or db uri),
        or a `pymongo.collection.Collection` object
    - quickly changing collection without changing the database

    Examples:

        # Initialize the db engine with a unique set of params that
        # allow running queries accross all collections (most obvious usage).

        >>> collection = Collection('2021-06-22', 'mongodb://localhost:27017/default_db')
        >>> collection('2021-06-23')

        # perform queries
        >>> collection.db.list_collection_names()
        >>> list(collection.find())

    #TODO inherit pymongo.Collection
    """

    # cached
    _collection = None

    def __init__(self, collection, db_or_uri=None):
        """
        Initializes a collection from str or Collection object.

        :param str or pymongo.collection.Collection collection: collection
        :param str or pymongo.database.Database db_or_uri: database object or connection uri
            required if `collection` is str type.
        """
        if isinstance(collection, str):
            assert db_or_uri, \
                "if given as a string, collection requires valid `db_or_uri`"

        # case `Collection`: we're already set
        if isinstance(collection, (pymongo.collection.Collection, Collection)):
            self.collection = collection
            self.db = self.collection.database
        # case `pymongo.database.Database` or MongoClient uri: can be handled
        # by `PyMongo.__init__()` which will be further set `self.db`
        else:
            super().__init__(db_or_uri)
            self.collection = self.db[collection]

    def __call__(self, collection):
        """ For quickly switching collection without changing of databse.

        :param collection: collection
        :type collection: str or pymongo.collection.Collection
        """
        if isinstance(collection, str):
            self.collection = self.db[collection]
        self.collection = collection
        return self

    def __delete__(self, owner):
        if self._collection:
            self._collection.drop()     # FIXME: test it !
            del self._collection        # restores to None (by class prop)

    def __str__(self):
        """ Yields the underlying collection name, eg. '2022-05-22' 
        Required by ancestors. """
        return self.collection.name

    @property
    def name(self):
        return self.collection.name

    @property
    def database(self):
        """ An alias for `self.db` set by parent class Pymongo. """
        return self.db

    @property
    def collection(self) -> pymongo.collection.Collection:
        """ Returns the cached collection object 
        """
        return self._collection

    @collection.setter
    def collection(self, collection: pymongo.collection.Collection or str):
        """ Saves a valid collection object locally awa. underlying database.
        If str, assumes a collection change on the same database """

        assert isinstance(collection, (str, pymongo.collection.Collection, Collection)), \
            "`collection` must be `str` or `pymongo.collection.Collection`, "\
            f"passed: {type(collection)}"

        self._collection = self.db[collection] if \
            isinstance(collection, str) else (
                collection.collection if
                isinstance(collection, Collection) 
                else collection # <- if pymongo.collection.Collection
            )

        self.db = self._collection.database          # if daily_query.mongo.Collection

    def count(self):
        return self.collection.count_documents({})

    def find(self, filter=None, projection=None):
        return self.collection.find(filter, projection)

    def find_one(self, *args, **kwargs):
        return self.collection.find_one(*args, **kwargs)

    def update_one(self, *args, **kwargs):
        return self.collection.update_one(*args, **kwargs)

    def insert_many(self, docs, **kwargs,):
        return self.collection.insert_many(docs, **kwargs)
    
    def update_or_create(self, defaults: dict, **kwargs):
        """ Similar to Django's `.update_or_create()`, tries to fetch an object
        from the database based on **kwargs** filter. If mached, uses **defaults**
        to update the object found, else to create a new object .
        """

        filter = {k: {'$eq': v} for k,v in kwargs.items() }
        return self.collection.find_one_and_update(
            filter, {"$set": defaults}, upsert=True)


class MongoDaily(PyMongo, NoSQLDaily):
    """
    Query daily data seamlessly with MongoDB.
    Enables database queries across several days.

    """

    db = None  # set by ancestor `PyMongo`

    def search(self, flatten=False, **kwargs):
        """
        Wrapper around `find()` that returns with objects/lists
        instead of raw db cursors

        :param flatten: yields [doc, ...] instead of [(doc, col), ...]
        :return: yields **(collection, doc)**
             set flatten=True to yield **doc**
             **doc**: found doc, **col_name**: collection found doc belongs to.
        """
        for cursor, col in list(self.find(**kwargs)):
            for doc in list(cursor):
                yield (doc, col) \
                    if not flatten else doc

    def find(self, days=None, days_from=FOREVER, days_to=None,
             first=None, filter=None, fields=None, exclude=None, flatten=False):
        """
        Find matching items across several collections (days).
        Has defaults values for all kwargs.
        Nota: items are gathered per collections, one day at a time.

        :param days_from: only items published since `days_from` (default: `FOREVER`)
        :param days_to: only items published before `days_to` (default: **today**)

        :param first: only first N documents across ALL filtered collections .
        :param filter: MongoDB document filter. Applied to each of the filtered collections.
        :param fields: fields to include (added to MongoDB projection).
        :param exclude: fields to exclude (stripped from MongoDB projection)

        :return: yields **(col, cursor)**
                 **cursor**: found docs,
                 **col_name**: collection where found doc belongs.

        #FIXME: search in reverse order from `days_to` to `days_from`.
        """

        limit = first or FETCH_BATCH
        projection = self._mk_projection(fields, exclude)

        collections, docs_count = self.get_collections(
            days=days, days_from=days_from, days_to=days_to)
        limit = min(docs_count, limit)

        for collection in collections:
            cursor = collection.find(filter, projection).limit(limit)  # fix: this tak
            yield (cursor, collection) \
                if not flatten else cursor

            # optionally limit the results
            # limit -= cursor.count()   method removed in MongDB v4
            # cursor.retrieved == 0     since until cursor is retrieved, can't use
            limit -= len(list(cursor.clone()))
            if limit <= 0:
                break

    def get_collections(self, days=[], days_from=None, days_to=None,
                        existing_only=True) -> [[Collection], int]:
        """
        Get existing collections matching the given date range,
        sorted by reverse date order.

        `days_from`, `days_to` params expected as '%Y-%m-%d' string or datetime
            cf. `utils.DATE_FORMAT`
        """

        # parse valid dates as str
        all_days = OrderedSet([str(d) for d in parse_dates(
            days=days, days_from=days_from, days_to=days_to
        )])
        if existing_only:
            all_days.intersection_update(self.db.list_collection_names())

        # get (collections, total docs count) matching given days
        collections = []
        docs_count = 0
        for day in all_days:
            collection = Collection(day, self.db)
            collections += [collection]
            docs_count += collection.count()

        return collections, docs_count

    def _mk_projection(self, fields=None, exclude=None):
        """ Make MongoDB projection from field names """
        if not (fields or exclude):
            return
        fields_map = {f: 0 if f in exclude else 1 for f in fields}
        return fields_map


