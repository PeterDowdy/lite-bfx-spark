"""StructType -> pyarrow schema, and batching positional tuple rows into RecordBatches.

Yielding ``pyarrow.RecordBatch`` (rather than tuples) is the documented fast path for a
Python Data Source: columnar transfer avoids per-row Python<->JVM overhead.
"""

import pyarrow as pa
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, LongType, MapType,
                               StringType, StructType, TimestampType)

_BATCH = 8192


def to_arrow_type(dt):
    if isinstance(dt, StringType):
        return pa.string()
    if isinstance(dt, IntegerType):
        return pa.int32()
    if isinstance(dt, LongType):
        return pa.int64()
    if isinstance(dt, DoubleType):
        return pa.float64()
    if isinstance(dt, TimestampType):
        return pa.timestamp("us")
    if isinstance(dt, ArrayType):
        return pa.list_(to_arrow_type(dt.elementType))
    if isinstance(dt, MapType):
        return pa.map_(to_arrow_type(dt.keyType), to_arrow_type(dt.valueType))
    if isinstance(dt, StructType):
        return pa.struct([pa.field(f.name, to_arrow_type(f.dataType), f.nullable)
                          for f in dt.fields])
    raise TypeError(f"litebfx: unsupported type for arrow conversion: {dt}")


def to_arrow_schema(struct):
    return pa.schema([pa.field(f.name, to_arrow_type(f.dataType), f.nullable)
                      for f in struct.fields])


def batches(rows, schema, size=_BATCH):
    """Group positional tuples into ``pyarrow.RecordBatch`` objects matching ``schema``."""
    types = [schema.field(i).type for i in range(len(schema.names))]
    buf = []
    for row in rows:
        buf.append(row)
        if len(buf) >= size:
            yield _batch(buf, schema, types)
            buf = []
    if buf:
        yield _batch(buf, schema, types)


def _batch(rows, schema, types):
    arrays = [pa.array([r[i] for r in rows], type=t) for i, t in enumerate(types)]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)
