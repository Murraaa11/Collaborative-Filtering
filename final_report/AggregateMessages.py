from pyspark import SparkContext
from pyspark.sql import DataFrame, functions as sqlfunctions


def _java_api(jsc):
    javaClassName = "org.graphframes.GraphFramePythonAPI"
    return jsc._jvm.Thread.currentThread().getContextClassLoader().loadClass(javaClassName) \
            .newInstance()


class _ClassProperty(object):
    """Custom read-only class property descriptor.

    The underlying method should take the class as the sole argument.
    """

    def __init__(self, f):
        self.f = f
        self.__doc__ = f.__doc__

    def __get__(self, instance, owner):
        return self.f(owner)


class AM(object):
    """Collection of utilities usable with :meth:`graphframes.GraphFrame.aggregateMessages()`."""

    @_ClassProperty
    def src(cls):
        """Reference for source column, used for specifying messages."""
        jvm_gf_api = _java_api(SparkContext)
        return sqlfunctions.col(jvm_gf_api.SRC())

    @_ClassProperty
    def dst(cls):
        """Reference for destination column, used for specifying messages."""
        jvm_gf_api = _java_api(SparkContext)
        return sqlfunctions.col(jvm_gf_api.DST())

    @_ClassProperty
    def edge(cls):
        """Reference for edge column, used for specifying messages."""
        jvm_gf_api = _java_api(SparkContext)
        return sqlfunctions.col(jvm_gf_api.EDGE())

    @_ClassProperty
    def msg(cls):
        """Reference for message column, used for specifying aggregation function."""
        jvm_gf_api = _java_api(SparkContext)
        return sqlfunctions.col(jvm_gf_api.aggregateMessages().MSG_COL_NAME())

    @staticmethod
    def getCachedDataFrame(df):
        """
        Create a new cached copy of a DataFrame.

        This utility method is useful for iterative DataFrame-based algorithms. See Scala
        documentation for more details.

        WARNING: This is NOT the same as `DataFrame.cache()`.
                 The original DataFrame will NOT be cached.
        """
        sqlContext = df.sql_ctx
        jvm_gf_api = _java_api(sqlContext._sc)
        jdf = jvm_gf_api.aggregateMessages().getCachedDataFrame(df._jdf)
        return DataFrame(jdf, sqlContext)



        