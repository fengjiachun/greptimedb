CREATE TABLE test (
  ts timestamp(3) time index,
  host STRING,
  idc STRING,
  val BIGINT,
  PRIMARY KEY(host, idc),
);

INSERT INTO TABLE test VALUES
    (0,     'host1', 'idc1', 1),
    (0,     'host2', 'idc1', 2),
    (5000,  'host1', 'idc2:zone1',3),
    (5000,  'host2', 'idc2',4),
    (10000, 'host1', 'idc3:zone2',5),
    (10000, 'host2', 'idc3',6),
    (15000, 'host1', 'idc4:zone3',7),
    (15000, 'host2', 'idc4',8);

-- Missing source labels --
TQL EVAL (0, 15, '5s') label_join(test{host="host1"}, "new_host", "-");

-- dst_label is equal to source label --
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_join(test{host="host1"}, "host", "-", "host");

-- dst_label is in source labels --
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_join(test{host="host1"}, "host", "-", "idc", "host");

-- test the empty source label --
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_join(test{host="host1"}, "host", "-", "");

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_join(test{host="host1"}, "new_host", "-", "idc", "host");

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_replace(test{host="host1"}, "new_idc", "$2", "idc", "(.*):(.*)");

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_replace(test{host="host1"}, "new_idc", "idc99", "idc", "idc2.*");

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_replace(test{host="host2"}, "new_idc", "$2", "idc", "(.*):(.*)");

-- dst_label is equal to source label --
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_replace(test{host="host2"}, "idc", "$2", "idc", "(.*):(.*)");

-- test the empty source label --
-- TODO(dennis): we can't remove the label currently --
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_replace(test{host="host2"}, "idc2", "", "", "");

-- Issue 5726 --
-- SQLNESS SORT_RESULT 3 1
TQL EVAL(0, 15, '5s') label_replace(vector(1), "host", "host1", "", "");

-- SQLNESS SORT_RESULT 3 1
TQL EVAL(0, 15, '5s') {__name__="test",host="host1"} * label_replace(vector(1), "host", "host1", "", "");

-- SQLNESS SORT_RESULT 3 1
TQL EVAL(0, 15, '5s') {__name__="test",host="host1"} + label_replace(vector(1), "host", "host1", "", "");

-- Empty regex with existing source label
-- SQLNESS SORT_RESULT 3 1
TQL EVAL(0, 15, '5s') label_replace(test{host="host1"}, "host2", "host2", "host", "");

-- Empty regex with not existing source label
-- SQLNESS SORT_RESULT 3 1
TQL EVAL(0, 15, '5s') label_replace(test{host="host1"}, "host2", "host2", "instance", "");

-- Empty regex with not existing source label, but replacement is empty
-- SQLNESS SORT_RESULT 3 1
TQL EVAL(0, 15, '5s') label_replace(test{host="host1"}, "host2", "", "instance", "");

-- Empty regex and different label value
-- SQLNESS SORT_RESULT 3 1
TQL EVAL(0, 15, '5s') {__name__="test",host="host1"} * label_replace(vector(1), "host", "host2", "host", "");

-- Empty regex and not existing label in left expression
-- SQLNESS SORT_RESULT 3 1
TQL EVAL(0, 15, '5s') {__name__="test",host="host1"} * label_replace(vector(1), "addr", "host1", "instance", "");

-- Issue 6438 --
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_replace(test{host="host1"}, "new_idc", "idc99", "idc", "idc2.*") == 1.0;

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 15, '5s') label_join(test{host="host1"}, "new_host", "-", "idc", "host") == 3;

DROP TABLE test;

CREATE TABLE test (
   ts timestamp(3) time index,
   host STRING,
   val BIGINT,
   PRIMARY KEY(host),
 );

INSERT INTO TABLE test VALUES
     (0, 'host1', 1),
     (0, 'host2', 2);

SELECT * FROM test;

-- test the non-existent matchers --
TQL EVAL (0, 1, '5s') test{job=~"host1|host3"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 1, '5s') test{job=~".*"};

TQL EVAL (0, 1, '5s') test{job=~".+"};

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (0, 1, '5s') test{job=""};

TQL EVAL (0, 1, '5s') test{job!=""};

DROP TABLE test;
