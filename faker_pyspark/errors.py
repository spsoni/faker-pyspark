"""Exceptions raised by faker-pyspark."""


class FakerPySparkError(Exception):
    """Base class for every error this package raises."""


class UnsupportedTypeError(FakerPySparkError, TypeError):
    """A column has a Spark type that no value can be generated for.

    Give that column an entry in ``overrides`` to supply the values yourself.
    """


class GeneratorSpecError(FakerPySparkError, ValueError):
    """An override, a ``faker`` metadata entry or an option is not valid."""
