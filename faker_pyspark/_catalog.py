"""What a column name says about the data it should hold.

Three small tables: Faker methods that return text, other spellings of those names, and
plausible ranges for numeric columns. A name is looked up whole first and then by its
trailing words, so ``billing_city`` finds ``city`` and ``unit_price`` finds ``price``.
"""

from pyspark.sql import types as T

# Faker methods that return ``str`` in every locale that defines them.
STRING_METHODS: frozenset[str] = frozenset(
    {
        "aba",
        "address",
        "administrative_unit",
        "bban",
        "bs",
        "building_number",
        "catch_phrase",
        "city",
        "color_name",
        "company",
        "company_suffix",
        "country",
        "country_code",
        "credit_card_number",
        "cryptocurrency_code",
        "currency_code",
        "currency_name",
        "day_of_week",
        "domain_name",
        "ean13",
        "email",
        "file_name",
        "first_name",
        "hex_color",
        "hostname",
        "iban",
        "ipv4",
        "ipv6",
        "isbn13",
        "job",
        "language_code",
        "last_name",
        "license_plate",
        "locale",
        "mac_address",
        "md5",
        "month_name",
        "msisdn",
        "name",
        "nic_handle",
        "paragraph",
        "password",
        "phone_number",
        "postcode",
        "prefix",
        "safe_email",
        "sentence",
        "sha256",
        "slug",
        "ssn",
        "state",
        "street_address",
        "street_name",
        "suffix",
        "swift",
        "text",
        "timezone",
        "tld",
        "uri_path",
        "url",
        "user_agent",
        "user_name",
        "uuid4",
        "word",
        "zipcode",
    }
)

# Other common spellings, mapped to the Faker method that produces the data.
ALIASES: dict[str, str] = {
    "addr": "address",
    "colour": "color_name",
    "color": "color_name",
    "comment": "sentence",
    "company_name": "company",
    "currency": "currency_code",
    "description": "sentence",
    "domain": "domain_name",
    "email_address": "email",
    "firstname": "first_name",
    "fname": "first_name",
    "full_name": "name",
    "fullname": "name",
    "guid": "uuid4",
    "homepage": "url",
    "ip": "ipv4",
    "ip_address": "ipv4",
    "job_title": "job",
    "lastname": "last_name",
    "lname": "last_name",
    "mail": "email",
    "mobile": "phone_number",
    "notes": "sentence",
    "occupation": "job",
    "phone": "phone_number",
    "postal_code": "postcode",
    "street": "street_address",
    "surname": "last_name",
    "telephone": "phone_number",
    "username": "user_name",
    "uuid": "uuid4",
    "website": "url",
    "zip": "postcode",
    "zip_code": "postcode",
}

# Names too generic to match as the tail of a longer name: ``product_name`` is not a
# person's name and ``order_state`` is not a region.
WHOLE_NAME_ONLY: frozenset[str] = frozenset(
    {
        "bs",
        "job",
        "locale",
        "md5",
        "name",
        "paragraph",
        "prefix",
        "sentence",
        "sha256",
        "slug",
        "state",
        "suffix",
        "text",
        "tld",
        "word",
    }
)

# Inclusive ranges for numeric columns, by name.
NUMERIC_RANGES: dict[str, tuple[int, int]] = {
    "age": (0, 100),
    "amount": (0, 10_000),
    "cost": (0, 10_000),
    "count": (0, 1_000),
    "lat": (-90, 90),
    "latitude": (-90, 90),
    "lng": (-180, 180),
    "lon": (-180, 180),
    "longitude": (-180, 180),
    "percent": (0, 100),
    "percentage": (0, 100),
    "price": (0, 10_000),
    "qty": (0, 1_000),
    "quantity": (0, 1_000),
    "rating": (0, 5),
    "salary": (20_000, 250_000),
    "score": (0, 100),
    "total": (0, 10_000),
    "year": (1970, 2030),
}

# Inclusive year ranges for date columns, by name.
DATE_YEAR_RANGES: dict[str, tuple[int, int]] = {
    "birth_date": (1940, 2010),
    "birthdate": (1940, 2010),
    "date_of_birth": (1940, 2010),
    "dob": (1940, 2010),
}

# Columns a random schema can be built from. Every name is one the tables above
# recognise, so the schema alone is enough to regenerate fitting data.
RANDOM_COLUMNS: tuple[tuple[str, T.DataType], ...] = (
    ("first_name", T.StringType()),
    ("last_name", T.StringType()),
    ("email", T.StringType()),
    ("user_name", T.StringType()),
    ("phone_number", T.StringType()),
    ("street_address", T.StringType()),
    ("city", T.StringType()),
    ("postcode", T.StringType()),
    ("country", T.StringType()),
    ("country_code", T.StringType()),
    ("company", T.StringType()),
    ("job", T.StringType()),
    ("url", T.StringType()),
    ("ipv4", T.StringType()),
    ("uuid", T.StringType()),
    ("currency_code", T.StringType()),
    ("color_name", T.StringType()),
    ("description", T.StringType()),
    ("age", T.IntegerType()),
    ("quantity", T.IntegerType()),
    ("score", T.IntegerType()),
    ("year", T.ShortType()),
    ("count", T.LongType()),
    ("price", T.DecimalType(10, 2)),
    ("salary", T.DecimalType(12, 2)),
    ("rating", T.FloatType()),
    ("latitude", T.DoubleType()),
    ("longitude", T.DoubleType()),
    ("is_active", T.BooleanType()),
    ("date_of_birth", T.DateType()),
    ("created_at", T.TimestampType()),
)

# Extra columns offered when a random schema may nest.
RANDOM_NESTED_COLUMNS: tuple[tuple[str, T.DataType], ...] = (
    (
        "address",
        T.StructType(
            [
                T.StructField("street_address", T.StringType()),
                T.StructField("city", T.StringType()),
                T.StructField("postcode", T.StringType()),
                T.StructField("country", T.StringType()),
            ]
        ),
    ),
    ("tags", T.ArrayType(T.StringType())),
    ("attributes", T.MapType(T.StringType(), T.StringType())),
    (
        "contacts",
        T.ArrayType(
            T.StructType(
                [
                    T.StructField("name", T.StringType()),
                    T.StructField("email", T.StringType()),
                ]
            )
        ),
    ),
)


def lookup(name: str, table: dict[str, tuple[int, int]]) -> tuple[int, int] | None:
    """Find ``name`` in a range table, whole or by its trailing words."""
    for candidate in _tails(name):
        if candidate in table:
            return table[candidate]
    return None


def string_method(name: str) -> str | None:
    """The Faker method that fits a text column called ``name``, if any."""
    for position, candidate in enumerate(_tails(name)):
        method = ALIASES.get(candidate, candidate)
        if method not in STRING_METHODS:
            continue
        if position > 0 and candidate in WHOLE_NAME_ONLY:
            continue
        return method
    return None


def _tails(name: str) -> list[str]:
    words = name.strip().lower().replace("-", "_").replace(" ", "_").split("_")
    words = [word for word in words if word]
    return ["_".join(words[start:]) for start in range(len(words))]
