import pymongo

from daily_query import base
from daily_query.helpers import parse_dates, isiterable
from ordered_set import OrderedSet

from .constants import \
    FOREVER, FETCH_BATCH, DEFAULT_COLLECTION


__all__ = ['PyMongo', 'Collection', 'MongoDaily']


class PyMongo:
    """
    Initializes a MongoDB using pymongo

    """
    # FIXME: implement looping cursor/roow using with context manager
    #   => refector funcs: distinct(), aggregate(), pipeline_exec(), etc.
    #   https://preshing.com/20110920/the-python-with-statement-by-example/

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
        if self.db is not None:
            # FIXME: raise InvalidOperation("Cannot use MongoClient after close")
            # self.db.client.close()
            del self.db


class Collection(PyMongo, base.Collection):
    """
    Per-collection raw queries to MongoDB enhanced.
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

    strict = True

    # cached collection object
    _collection = None

    def __init__(self, collection=None, db_or_uri=None, strict=True):
        """
        Initializes a collection from str or Collection object.

        :param str or pymongo.collection.Collection collection: collection
        :param str or pymongo.database.Database db_or_uri: database object or connection uri
            required if `collection` is str type.
        """

        if isinstance(collection, str):
            assert db_or_uri is not None, \
                "collection requires valid `db_or_uri` when specified as a string!"

        # case `Collection`: we're already set
        if isinstance(collection, (pymongo.collection.Collection, Collection)):
            self.collection = collection
            self.db = self.collection.database

        # case `pymongo.database.Database` or MongoClient uri: can be handled
        # by `PyMongo.__init__()` which will further set `self.db`
        else:
            super().__init__(db_or_uri)
            self.collection = self.db[collection or DEFAULT_COLLECTION]

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
        """ Returns the cached collection object  """
        # if self.strict:
        #     assert self._collection.name != DEFAULT_COLLECTION, \
        #         "strict=True forbids intializing a `Collection` with, " \
        #         "`collection` keyword argument set to `None` !"
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

    def find(self, match=None, projection=None):
        return self.collection.find(match, projection)

    def find_one(self, *args, **kwargs):
        return self.collection.find_one(*args, **kwargs)

    def update_one(self, *args, **kwargs):
        return self.collection.update_one(*args, **kwargs)

    def insert_many(self, docs, **kwargs,):
        return self.collection.insert_many(docs, **kwargs)
    
    def update_or_create(self, defaults: dict, **kwargs):
        """ Similar to Django's `.update_or_create()`, tries to fetch an object
        from the database based on **kwargs** match. If mached, uses **defaults**
        to update the object found, else to create a new object .
        """

        match = {k: {'$eq': v} for k,v in kwargs.items() }
        return self.collection.find_one_and_update(
            match, {"$set": defaults}, upsert=True)

    def aggregate(self, *args, **kwargs):
        return self.collection.aggregate(*args, **kwargs)


class MongoDaily(PyMongo, base.NoSQLDaily):
    """
    Query daily data seamlessly with MongoDB.
    Enables database queries across several day-collections.
    """

    db = None  # set by ancestor `PyMongo`

    def distinct(self, field, **kwargs):
        """
        Emulate compound `db.*.distinct(field)` across all db collections.
        Yields unique values for field
        """

        # pipeline_exec -> [{'_id': 'Education'}, ...]
        pipeline = [{"$group": {"_id": f"${field}"}}]
        r = self.pipeline_exec(pipeline, flatten=True, **kwargs)

        # yield unique values for field
        # breaks down iterable values
        values = set()
        for cursor in r:
            for doc in cursor:
                # assume value is always iterable, if not, make it so,
                # that eases working with both situations
                value = doc.pop("_id")
                if not isiterable(value):
                    value = value,
                for v in value:
                    if v not in values:
                        values.add(v)
                        yield v

    def search(self, flatten=False, **kwargs):
        """
        Wrapper around `find()` that yields documents instead of raw db cursors

        :param bool flatten: yields [doc, ...] instead of [(doc, col), ...]
        :return yields **(collection, doc)**
             set flatten=True to yield **doc**
             **doc**: found doc, **col_name**: collection found doc belongs to.
        """
        for cursor, cursor_len, collection in list(self.find(**kwargs)):
            for doc in list(cursor):
                yield (doc, collection) \
                    if not flatten else base.Doc(collection, doc)

    def aggregate(self, *args, flatten=False, **kwargs):
        """
        Wrapper around `pipeline_exec()` that yields rows
        instead of raw db cursors
        :return yields (doc, collection)'s, or flattened docs if flatten==True
        """

        limit = kwargs.get('limit', FETCH_BATCH)
        for cursor, cursor_len, collection in self.pipeline_exec(*args, **kwargs):
            for row in list(cursor):
                yield (row, collection) \
                    if not flatten else row
                limit -= 1
                if limit <= 0:
                    break

    def find(self, match=None, flatten=False, limit=None, fields=None, exclude=None,
            days=None, days_from=FOREVER, days_to=None):
        """
        Find matching items across collections (days), yield cursors.
        Limit applied on entire collections set, NOT individual ones.

        :param bool flatten: yields [doc, ...] instead of [(doc, col), ...]
        :param limit: only first N documents across ALL filtered collections .
        :param match: MongoDB document match. Applied to each of the filtered collections.
        :param fields: fields to include (added to MongoDB projection).
        :param exclude: fields to exclude (stripped from MongoDB projection)

        """
        pipeline = [{"$match": match or {}}]

        projection = self._mk_projection(fields, exclude)
        if projection:
            pipeline += [{"$project": projection}]

        return self.pipeline_exec(pipeline, flatten=flatten, limit=limit,
                                  days=None, days_from=FOREVER, days_to=None)

    def pipeline_exec(self, pipeline, *args, flatten=False, limit=FETCH_BATCH,
                      days=None, days_from=FOREVER, days_to=None, **kwargs):
        """
        Run across several collections (days), yield entire cursors.
        Has defaults values for all kwargs.
        Nota: items are gathered per collections, one day at a time.

        :param list or collections.abc.Iterable pipeline: mongo pipeline
        :param bool flatten: yields [doc, ...] instead of [(doc, col), ...]
        :param days: only items published on individual days
        :param days_from: only items published since `days_from` (default: `FOREVER`)
        :param days_to: only items published before `days_to` (default: **today**)
        :param limit: only first N documents across ALL filtered collections.

        :return: yields **(col, cursor)**
                 **cursor**: found docs,
                 **col_name**: collection where found doc belongs.

        #FIXME: search in reverse order from `days_to` to `days_from`.
        """

        collections, total_docs_count = self.get_collections(
            days=days, days_from=days_from, days_to=days_to)
        _limit = min(total_docs_count, limit or FETCH_BATCH)

        for collection in collections:

            pipe = pipeline(collection) if \
                not isiterable(pipeline) else pipeline
            cursor = collection.aggregate(pipe, *args, **kwargs)
            cursor_len = len(cursor._CommandCursor__data)           # FIXME: dirty hack
            yield (cursor, cursor_len, collection) \
                if not flatten else cursor

            # ==[ guard only to ensure that is not yielded  ]==
            # ==[ more cursors than imposed by `limit`      ]==
            # optionally limit the results
            # _limit -= cursor.count()               method removed in MongDB v4
            # cursor.retrieved == 0                 can't use, since stays 0 till cursor is enumerated,
            # _limit -= len(list(cursor.clone()))    works NOT with CommandCursor returned by `.aggregate()`
            _limit -= cursor_len
            if _limit <= 0:
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
        """ 
        Make MongoDB projection from field names 
        """
        if not (fields or exclude):
            return
        fields_map = {f: 0 if f in exclude else 1 for f in fields}
        return fields_map


