--
-- Greenplum Database database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET default_with_oids = false;

--
--

CREATE SCHEMA schema2;



SET search_path = public, pg_catalog;

SET default_tablespace = '';

--
--

CREATE TABLE foo (
    i integer
) DISTRIBUTED BY (i);



--
--

COPY foo (i) FROM stdin;
1
3
8
2
4
9
13
5
10
14
6
11
15
7
12
16
18
23
17
19
24
28
20
25
29
21
26
30
22
27
31
38
32
33
39
43
34
40
44
35
41
45
36
42
46
37
53
47
48
54
58
49
55
59
50
56
60
51
57
61
52
73
62
63
74
64
68
75
65
69
76
66
70
77
67
71
87
78
72
88
79
83
89
80
84
90
81
85
91
82
86
96
92
97
93
94
95
98
99
100
\.


--
--

CREATE TABLE holds (
    id integer,
    date date,
    amt numeric(10,2)
) DISTRIBUTED BY (id);



--
--

COPY holds (id, date, amt) FROM stdin;
\.


--
--

CREATE TABLE sales (
    id integer,
    date date,
    amt numeric(10,2)
) DISTRIBUTED BY (id) PARTITION BY RANGE(date) 
          (
          PARTITION jan17 START ('2017-01-01'::date) END ('2017-02-01'::date) WITH (tablename='sales_1_prt_jan17', appendonly=false ), 
          PARTITION feb17 START ('2017-02-01'::date) END ('2017-03-01'::date) WITH (tablename='sales_1_prt_feb17', appendonly=false ), 
          PARTITION mar17 START ('2017-03-01'::date) END ('2017-04-01'::date) WITH (tablename='sales_1_prt_mar17', appendonly=false ), 
          PARTITION apr17 START ('2017-04-01'::date) END ('2017-05-01'::date) WITH (tablename='sales_1_prt_apr17', appendonly=false ), 
          PARTITION may17 START ('2017-05-01'::date) END ('2017-06-01'::date) WITH (tablename='sales_1_prt_may17', appendonly=false ), 
          PARTITION jun17 START ('2017-06-01'::date) END ('2017-07-01'::date) WITH (tablename='sales_1_prt_jun17', appendonly=false ), 
          PARTITION jul17 START ('2017-07-01'::date) END ('2017-08-01'::date) WITH (tablename='sales_1_prt_jul17', appendonly=false ), 
          PARTITION aug17 START ('2017-08-01'::date) END ('2017-09-01'::date) WITH (tablename='sales_1_prt_aug17', appendonly=false ), 
          PARTITION sep17 START ('2017-09-01'::date) END ('2017-10-01'::date) WITH (tablename='sales_1_prt_sep17', appendonly=false ), 
          PARTITION oct17 START ('2017-10-01'::date) END ('2017-11-01'::date) WITH (tablename='sales_1_prt_oct17', appendonly=false ), 
          PARTITION nov17 START ('2017-11-01'::date) END ('2017-12-01'::date) WITH (tablename='sales_1_prt_nov17', appendonly=false ), 
          PARTITION dec17 START ('2017-12-01'::date) END ('2018-01-01'::date) WITH (tablename='sales_1_prt_dec17', appendonly=false )
          );



--
--

COPY sales (id, date, amt) FROM stdin;
1	2017-01-01	20.00
3	2017-03-01	20.00
25	2017-05-01	20.00
2	2017-02-01	20.00
4	2017-04-01	20.00
8	2017-08-01	20.00
15	2017-05-01	20.00
5	2017-05-01	20.00
9	2017-09-01	20.00
7	2017-07-01	20.00
11	2017-11-01	20.00
18	2017-08-01	20.00
12	2017-12-01	20.00
\.


SET search_path = schema2, pg_catalog;

--
--

CREATE TABLE foo2 (
    i integer
) DISTRIBUTED BY (i);



--
--

COPY foo2 (i) FROM stdin;
1
3
8
2
4
9
13
5
10
14
6
11
15
7
12
16
18
23
17
19
24
28
20
25
29
21
26
30
22
27
31
38
32
33
39
43
34
40
44
35
41
45
36
42
46
37
53
47
48
54
58
49
55
59
50
56
60
51
57
61
52
73
62
63
74
64
68
75
65
69
76
66
70
77
67
71
87
78
72
88
79
83
89
80
84
90
81
85
91
82
86
96
92
97
93
94
95
98
99
100
\.


--
--

CREATE TABLE returns (
    id integer,
    date date,
    amt numeric(10,2)
) DISTRIBUTED BY (id) PARTITION BY RANGE(date) 
          (
          PARTITION jan17 START ('2017-01-01'::date) END ('2017-02-01'::date) WITH (tablename='returns_1_prt_jan17', appendonly=false ), 
          PARTITION feb17 START ('2017-02-01'::date) END ('2017-03-01'::date) WITH (tablename='returns_1_prt_feb17', appendonly=false ), 
          PARTITION mar17 START ('2017-03-01'::date) END ('2017-04-01'::date) WITH (tablename='returns_1_prt_mar17', appendonly=false ), 
          PARTITION apr17 START ('2017-04-01'::date) END ('2017-05-01'::date) WITH (tablename='returns_1_prt_apr17', appendonly=false ), 
          PARTITION may17 START ('2017-05-01'::date) END ('2017-06-01'::date) WITH (tablename='returns_1_prt_may17', appendonly=false ), 
          PARTITION jun17 START ('2017-06-01'::date) END ('2017-07-01'::date) WITH (tablename='returns_1_prt_jun17', appendonly=false ), 
          PARTITION jul17 START ('2017-07-01'::date) END ('2017-08-01'::date) WITH (tablename='returns_1_prt_jul17', appendonly=false ), 
          PARTITION aug17 START ('2017-08-01'::date) END ('2017-09-01'::date) WITH (tablename='returns_1_prt_aug17', appendonly=false ), 
          PARTITION sep17 START ('2017-09-01'::date) END ('2017-10-01'::date) WITH (tablename='returns_1_prt_sep17', appendonly=false ), 
          PARTITION oct17 START ('2017-10-01'::date) END ('2017-11-01'::date) WITH (tablename='returns_1_prt_oct17', appendonly=false ), 
          PARTITION nov17 START ('2017-11-01'::date) END ('2017-12-01'::date) WITH (tablename='returns_1_prt_nov17', appendonly=false ), 
          PARTITION dec17 START ('2017-12-01'::date) END ('2018-01-01'::date) WITH (tablename='returns_1_prt_dec17', appendonly=false )
          );



--
--

COPY returns (id, date, amt) FROM stdin;
2	2017-02-01	20.00
7	2017-07-01	20.00
25	2017-05-01	20.00
15	2017-05-01	20.00
11	2017-11-01	20.00
12	2017-12-01	20.00
\.


--
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- Greenplum Database database dump complete
--

