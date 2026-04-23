from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from psycopg import sql

if TYPE_CHECKING:
    from typing import TypeAlias


from .node import Conn, Node

JsonType: TypeAlias = Literal["array", "object", "string", "number", "boolean", "jsonb"]


class FixedValueNode(Node):
    def __init__(
        self,
        source: sql.Identifier,
        prop: str | None,
        path: sql.Composable,
        prefix: str,
    ):
        super().__init__(source, prop)

        self.path = path
        self.prefix = prefix

    @property
    def alias(self) -> str:
        if len(self.prefix) == 0:
            return self.snake_prop or ""
        return self.prefix + (
            ("__" + self.snake_prop) if self.snake_prop is not None else ""
        )

    @property
    def stmt(self) -> sql.Composable:
        # this should be abstract but Python can't use ABCs as a generic
        return sql.SQL("")


class TypedNode(FixedValueNode):
    def __init__(
        self,
        source: sql.Identifier,
        prop: str | None,
        path: sql.Composable,
        prefix: str,
        json_types: tuple[JsonType, JsonType],
    ):
        super().__init__(source, prop, path, prefix)

        self.is_mixed = json_types[0] != json_types[1]
        self.json_type: JsonType = "string" if self.is_mixed else json_types[0]
        self.is_uuid = False
        self.is_datetime = False
        self.is_float = False
        self.is_bigint = False

    @property
    def json_string(self) -> sql.Composable:
        if self.prop is None:
            str_extract = (
                sql.SQL("""TRIM(BOTH '"' FROM (""") + self.path + sql.SQL(")::text)")
            )
        else:
            str_extract = self.path + sql.SQL("->>") + sql.Literal(self.prop)

        return sql.SQL("NULLIF(NULLIF(") + str_extract + sql.SQL(", ''), 'null')")

    @property
    def stmt(self) -> sql.Composable:
        type_extract: sql.Composable
        if self.json_type == "jsonb":
            type_extract = self.path
        elif self.json_type == "number" and self.is_float:
            type_extract = self.json_string + sql.SQL("::numeric")
        elif self.json_type == "number" and self.is_bigint:
            type_extract = self.json_string + sql.SQL("::bigint")
        elif self.json_type == "number":
            type_extract = self.json_string + sql.SQL("::integer")
        elif self.json_type == "boolean":
            type_extract = self.json_string + sql.SQL("::bool")
        elif self.json_type == "string" and self.is_uuid:
            type_extract = self.json_string + sql.SQL("::uuid")
        elif self.json_type == "string" and self.is_datetime:
            type_extract = self.json_string + sql.SQL("::timestamptz")
        else:
            type_extract = self.json_string

        return type_extract + sql.SQL(" AS ") + sql.Identifier(self.alias)

    def specify_type(self, conn: Conn) -> None:
        if self.is_mixed or self.json_type not in ["string", "number"]:
            return

        cte = (
            sql.SQL("""
WITH string_values AS MATERIALIZED (
    SELECT """)
            + self.json_string
            + sql.SQL(""" AS string_value
    FROM {source}
)""").format(source=self.source)
        )

        if self.json_type == "string":
            with conn.cursor() as cur:
                specify = cte + sql.SQL(r"""
SELECT
    NOT EXISTS(
        SELECT 1 FROM string_values
        WHERE
            string_value IS NOT NULL AND
            string_value NOT LIKE '________-____-____-____-____________'
    ) AS is_uuid
    ,NOT EXISTS(
        SELECT 1 FROM string_values
        WHERE
            string_value IS NOT NULL AND
            string_value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?(Z|[+-][0-9]{2}(:?[0-9]{2})?)$'
    ) AS is_datetime;""")  # noqa: E501

                cur.execute(specify.as_string())
                if row := cur.fetchone():
                    (self.is_uuid, self.is_datetime) = row

        if self.json_type == "number":
            with conn.cursor() as cur:
                specify = cte + sql.SQL("""
SELECT
    EXISTS(
        SELECT 1 FROM string_values
        WHERE
            string_value IS NOT NULL AND
            SCALE(string_value::numeric) > 0
    ) AS is_float
    ,EXISTS(
        SELECT 1 FROM string_values
        WHERE
            string_value IS NOT NULL AND
            string_value::numeric > 2147483647
    ) AS is_bigint;""")

                cur.execute(specify.as_string())
                if row := cur.fetchone():
                    (self.is_float, self.is_bigint) = row


class OrdinalNode(FixedValueNode):
    def __init__(
        self,
        source: sql.Identifier,
        path: sql.Composable,
        prefix: str,
    ):
        super().__init__(source, None, path, prefix)

    @property
    def alias(self) -> str:
        return self.prefix + "__o"

    @property
    def stmt(self) -> sql.Composable:
        return sql.Identifier("a", "__o") + sql.SQL(" AS {alias}").format(
            alias=sql.Identifier(self.alias),
        )


class JsonbNode(TypedNode):
    def __init__(
        self,
        source: sql.Identifier,
        path: sql.Composable,
        prefix: str,
    ):
        super().__init__(source, None, path, prefix, ("jsonb", "jsonb"))
