INSERT OVERWRITE INTO lineitem_large_parts
  SELECT
    l_partkey, l_suppkey, l_linenumber, l_quantity,
    l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate,
    l_receiptdate, l_shipinstruct, l_shipmode, l_comment, 1 as l_orderkey
  FROM lineitem_large;