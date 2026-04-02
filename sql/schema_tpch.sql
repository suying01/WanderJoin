CREATE TABLE IF NOT EXISTS region (
  r_regionkey INT PRIMARY KEY,
  r_name TEXT NOT NULL,
  r_comment TEXT
);

CREATE TABLE IF NOT EXISTS nation (
  n_nationkey INT PRIMARY KEY,
  n_name TEXT NOT NULL,
  n_regionkey INT NOT NULL REFERENCES region(r_regionkey),
  n_comment TEXT
);

CREATE TABLE IF NOT EXISTS supplier (
  s_suppkey INT PRIMARY KEY,
  s_name TEXT NOT NULL,
  s_address TEXT NOT NULL,
  s_nationkey INT NOT NULL REFERENCES nation(n_nationkey),
  s_phone TEXT NOT NULL,
  s_acctbal NUMERIC(15,2) NOT NULL,
  s_comment TEXT
);

CREATE TABLE IF NOT EXISTS customer (
  c_custkey INT PRIMARY KEY,
  c_name TEXT NOT NULL,
  c_address TEXT NOT NULL,
  c_nationkey INT NOT NULL REFERENCES nation(n_nationkey),
  c_phone TEXT NOT NULL,
  c_acctbal NUMERIC(15,2) NOT NULL,
  c_mktsegment TEXT NOT NULL,
  c_comment TEXT
);

CREATE TABLE IF NOT EXISTS part (
  p_partkey INT PRIMARY KEY,
  p_name TEXT NOT NULL,
  p_mfgr TEXT NOT NULL,
  p_brand TEXT NOT NULL,
  p_type TEXT NOT NULL,
  p_size INT NOT NULL,
  p_container TEXT NOT NULL,
  p_retailprice NUMERIC(15,2) NOT NULL,
  p_comment TEXT
);

CREATE TABLE IF NOT EXISTS partsupp (
  ps_partkey INT NOT NULL REFERENCES part(p_partkey),
  ps_suppkey INT NOT NULL REFERENCES supplier(s_suppkey),
  ps_availqty INT NOT NULL,
  ps_supplycost NUMERIC(15,2) NOT NULL,
  ps_comment TEXT,
  PRIMARY KEY (ps_partkey, ps_suppkey)
);

CREATE TABLE IF NOT EXISTS orders (
  o_orderkey INT PRIMARY KEY,
  o_custkey INT NOT NULL REFERENCES customer(c_custkey),
  o_orderstatus CHAR(1) NOT NULL,
  o_totalprice NUMERIC(15,2) NOT NULL,
  o_orderdate DATE NOT NULL,
  o_orderpriority TEXT NOT NULL,
  o_clerk TEXT NOT NULL,
  o_shippriority INT NOT NULL,
  o_comment TEXT
);

CREATE TABLE IF NOT EXISTS lineitem (
  l_orderkey INT NOT NULL REFERENCES orders(o_orderkey),
  l_partkey INT NOT NULL REFERENCES part(p_partkey),
  l_suppkey INT NOT NULL REFERENCES supplier(s_suppkey),
  l_linenumber INT NOT NULL,
  l_quantity NUMERIC(15,2) NOT NULL,
  l_extendedprice NUMERIC(15,2) NOT NULL,
  l_discount NUMERIC(15,2) NOT NULL,
  l_tax NUMERIC(15,2) NOT NULL,
  l_returnflag CHAR(1) NOT NULL,
  l_linestatus CHAR(1) NOT NULL,
  l_shipdate DATE NOT NULL,
  l_commitdate DATE NOT NULL,
  l_receiptdate DATE NOT NULL,
  l_shipinstruct TEXT NOT NULL,
  l_shipmode TEXT NOT NULL,
  l_comment TEXT,
  PRIMARY KEY (l_orderkey, l_linenumber)
);
