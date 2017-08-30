--
-- Greenplum Database database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: foo0; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo0 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo0 OWNER TO pivotal;

--
-- Data for Name: foo0; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo0 (i) FROM stdin;
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
-- Name: foo1; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo1 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo1 OWNER TO pivotal;

--
-- Data for Name: foo1; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo1 (i) FROM stdin;
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
-- Name: foo10; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo10 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo10 OWNER TO pivotal;

--
-- Data for Name: foo10; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo10 (i) FROM stdin;
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
-- Name: foo100; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo100 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo100 OWNER TO pivotal;

--
-- Data for Name: foo100; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo100 (i) FROM stdin;
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
-- Name: foo1000; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo1000 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo1000 OWNER TO pivotal;

--
-- Data for Name: foo1000; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo1000 (i) FROM stdin;
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
-- Name: foo101; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo101 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo101 OWNER TO pivotal;

--
-- Data for Name: foo101; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo101 (i) FROM stdin;
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
-- Name: foo102; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo102 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo102 OWNER TO pivotal;

--
-- Data for Name: foo102; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo102 (i) FROM stdin;
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
-- Name: foo103; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo103 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo103 OWNER TO pivotal;

--
-- Data for Name: foo103; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo103 (i) FROM stdin;
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
-- Name: foo104; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo104 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo104 OWNER TO pivotal;

--
-- Data for Name: foo104; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo104 (i) FROM stdin;
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
-- Name: foo105; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo105 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo105 OWNER TO pivotal;

--
-- Data for Name: foo105; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo105 (i) FROM stdin;
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
-- Name: foo106; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo106 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo106 OWNER TO pivotal;

--
-- Data for Name: foo106; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo106 (i) FROM stdin;
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
-- Name: foo107; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo107 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo107 OWNER TO pivotal;

--
-- Data for Name: foo107; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo107 (i) FROM stdin;
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
-- Name: foo108; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo108 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo108 OWNER TO pivotal;

--
-- Data for Name: foo108; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo108 (i) FROM stdin;
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
-- Name: foo109; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo109 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo109 OWNER TO pivotal;

--
-- Data for Name: foo109; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo109 (i) FROM stdin;
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
-- Name: foo11; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo11 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo11 OWNER TO pivotal;

--
-- Data for Name: foo11; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo11 (i) FROM stdin;
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
-- Name: foo110; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo110 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo110 OWNER TO pivotal;

--
-- Data for Name: foo110; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo110 (i) FROM stdin;
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
-- Name: foo111; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo111 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo111 OWNER TO pivotal;

--
-- Data for Name: foo111; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo111 (i) FROM stdin;
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
-- Name: foo112; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo112 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo112 OWNER TO pivotal;

--
-- Data for Name: foo112; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo112 (i) FROM stdin;
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
-- Name: foo113; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo113 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo113 OWNER TO pivotal;

--
-- Data for Name: foo113; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo113 (i) FROM stdin;
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
-- Name: foo114; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo114 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo114 OWNER TO pivotal;

--
-- Data for Name: foo114; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo114 (i) FROM stdin;
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
-- Name: foo115; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo115 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo115 OWNER TO pivotal;

--
-- Data for Name: foo115; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo115 (i) FROM stdin;
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
-- Name: foo116; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo116 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo116 OWNER TO pivotal;

--
-- Data for Name: foo116; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo116 (i) FROM stdin;
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
-- Name: foo117; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo117 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo117 OWNER TO pivotal;

--
-- Data for Name: foo117; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo117 (i) FROM stdin;
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
-- Name: foo118; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo118 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo118 OWNER TO pivotal;

--
-- Data for Name: foo118; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo118 (i) FROM stdin;
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
-- Name: foo119; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo119 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo119 OWNER TO pivotal;

--
-- Data for Name: foo119; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo119 (i) FROM stdin;
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
-- Name: foo12; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo12 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo12 OWNER TO pivotal;

--
-- Data for Name: foo12; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo12 (i) FROM stdin;
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
-- Name: foo120; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo120 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo120 OWNER TO pivotal;

--
-- Data for Name: foo120; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo120 (i) FROM stdin;
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
-- Name: foo121; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo121 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo121 OWNER TO pivotal;

--
-- Data for Name: foo121; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo121 (i) FROM stdin;
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
-- Name: foo122; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo122 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo122 OWNER TO pivotal;

--
-- Data for Name: foo122; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo122 (i) FROM stdin;
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
-- Name: foo123; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo123 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo123 OWNER TO pivotal;

--
-- Data for Name: foo123; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo123 (i) FROM stdin;
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
-- Name: foo124; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo124 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo124 OWNER TO pivotal;

--
-- Data for Name: foo124; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo124 (i) FROM stdin;
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
-- Name: foo125; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo125 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo125 OWNER TO pivotal;

--
-- Data for Name: foo125; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo125 (i) FROM stdin;
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
-- Name: foo126; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo126 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo126 OWNER TO pivotal;

--
-- Data for Name: foo126; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo126 (i) FROM stdin;
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
-- Name: foo127; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo127 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo127 OWNER TO pivotal;

--
-- Data for Name: foo127; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo127 (i) FROM stdin;
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
-- Name: foo128; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo128 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo128 OWNER TO pivotal;

--
-- Data for Name: foo128; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo128 (i) FROM stdin;
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
-- Name: foo129; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo129 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo129 OWNER TO pivotal;

--
-- Data for Name: foo129; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo129 (i) FROM stdin;
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
-- Name: foo13; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo13 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo13 OWNER TO pivotal;

--
-- Data for Name: foo13; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo13 (i) FROM stdin;
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
-- Name: foo130; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo130 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo130 OWNER TO pivotal;

--
-- Data for Name: foo130; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo130 (i) FROM stdin;
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
-- Name: foo131; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo131 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo131 OWNER TO pivotal;

--
-- Data for Name: foo131; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo131 (i) FROM stdin;
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
-- Name: foo132; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo132 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo132 OWNER TO pivotal;

--
-- Data for Name: foo132; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo132 (i) FROM stdin;
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
-- Name: foo133; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo133 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo133 OWNER TO pivotal;

--
-- Data for Name: foo133; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo133 (i) FROM stdin;
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
-- Name: foo134; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo134 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo134 OWNER TO pivotal;

--
-- Data for Name: foo134; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo134 (i) FROM stdin;
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
-- Name: foo135; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo135 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo135 OWNER TO pivotal;

--
-- Data for Name: foo135; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo135 (i) FROM stdin;
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
-- Name: foo136; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo136 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo136 OWNER TO pivotal;

--
-- Data for Name: foo136; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo136 (i) FROM stdin;
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
-- Name: foo137; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo137 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo137 OWNER TO pivotal;

--
-- Data for Name: foo137; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo137 (i) FROM stdin;
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
-- Name: foo138; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo138 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo138 OWNER TO pivotal;

--
-- Data for Name: foo138; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo138 (i) FROM stdin;
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
-- Name: foo139; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo139 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo139 OWNER TO pivotal;

--
-- Data for Name: foo139; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo139 (i) FROM stdin;
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
-- Name: foo14; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo14 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo14 OWNER TO pivotal;

--
-- Data for Name: foo14; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo14 (i) FROM stdin;
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
-- Name: foo140; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo140 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo140 OWNER TO pivotal;

--
-- Data for Name: foo140; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo140 (i) FROM stdin;
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
-- Name: foo141; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo141 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo141 OWNER TO pivotal;

--
-- Data for Name: foo141; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo141 (i) FROM stdin;
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
-- Name: foo142; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo142 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo142 OWNER TO pivotal;

--
-- Data for Name: foo142; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo142 (i) FROM stdin;
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
-- Name: foo143; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo143 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo143 OWNER TO pivotal;

--
-- Data for Name: foo143; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo143 (i) FROM stdin;
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
-- Name: foo144; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo144 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo144 OWNER TO pivotal;

--
-- Data for Name: foo144; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo144 (i) FROM stdin;
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
-- Name: foo145; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo145 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo145 OWNER TO pivotal;

--
-- Data for Name: foo145; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo145 (i) FROM stdin;
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
-- Name: foo146; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo146 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo146 OWNER TO pivotal;

--
-- Data for Name: foo146; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo146 (i) FROM stdin;
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
-- Name: foo147; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo147 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo147 OWNER TO pivotal;

--
-- Data for Name: foo147; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo147 (i) FROM stdin;
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
-- Name: foo148; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo148 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo148 OWNER TO pivotal;

--
-- Data for Name: foo148; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo148 (i) FROM stdin;
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
-- Name: foo149; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo149 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo149 OWNER TO pivotal;

--
-- Data for Name: foo149; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo149 (i) FROM stdin;
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
-- Name: foo15; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo15 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo15 OWNER TO pivotal;

--
-- Data for Name: foo15; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo15 (i) FROM stdin;
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
-- Name: foo150; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo150 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo150 OWNER TO pivotal;

--
-- Data for Name: foo150; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo150 (i) FROM stdin;
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
-- Name: foo151; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo151 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo151 OWNER TO pivotal;

--
-- Data for Name: foo151; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo151 (i) FROM stdin;
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
-- Name: foo152; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo152 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo152 OWNER TO pivotal;

--
-- Data for Name: foo152; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo152 (i) FROM stdin;
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
-- Name: foo153; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo153 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo153 OWNER TO pivotal;

--
-- Data for Name: foo153; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo153 (i) FROM stdin;
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
-- Name: foo154; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo154 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo154 OWNER TO pivotal;

--
-- Data for Name: foo154; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo154 (i) FROM stdin;
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
-- Name: foo155; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo155 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo155 OWNER TO pivotal;

--
-- Data for Name: foo155; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo155 (i) FROM stdin;
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
-- Name: foo156; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo156 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo156 OWNER TO pivotal;

--
-- Data for Name: foo156; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo156 (i) FROM stdin;
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
-- Name: foo157; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo157 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo157 OWNER TO pivotal;

--
-- Data for Name: foo157; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo157 (i) FROM stdin;
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
-- Name: foo158; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo158 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo158 OWNER TO pivotal;

--
-- Data for Name: foo158; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo158 (i) FROM stdin;
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
-- Name: foo159; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo159 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo159 OWNER TO pivotal;

--
-- Data for Name: foo159; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo159 (i) FROM stdin;
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
-- Name: foo16; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo16 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo16 OWNER TO pivotal;

--
-- Data for Name: foo16; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo16 (i) FROM stdin;
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
-- Name: foo160; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo160 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo160 OWNER TO pivotal;

--
-- Data for Name: foo160; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo160 (i) FROM stdin;
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
-- Name: foo161; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo161 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo161 OWNER TO pivotal;

--
-- Data for Name: foo161; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo161 (i) FROM stdin;
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
-- Name: foo162; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo162 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo162 OWNER TO pivotal;

--
-- Data for Name: foo162; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo162 (i) FROM stdin;
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
-- Name: foo163; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo163 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo163 OWNER TO pivotal;

--
-- Data for Name: foo163; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo163 (i) FROM stdin;
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
-- Name: foo164; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo164 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo164 OWNER TO pivotal;

--
-- Data for Name: foo164; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo164 (i) FROM stdin;
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
-- Name: foo165; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo165 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo165 OWNER TO pivotal;

--
-- Data for Name: foo165; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo165 (i) FROM stdin;
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
-- Name: foo166; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo166 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo166 OWNER TO pivotal;

--
-- Data for Name: foo166; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo166 (i) FROM stdin;
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
-- Name: foo167; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo167 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo167 OWNER TO pivotal;

--
-- Data for Name: foo167; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo167 (i) FROM stdin;
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
-- Name: foo168; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo168 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo168 OWNER TO pivotal;

--
-- Data for Name: foo168; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo168 (i) FROM stdin;
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
-- Name: foo169; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo169 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo169 OWNER TO pivotal;

--
-- Data for Name: foo169; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo169 (i) FROM stdin;
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
-- Name: foo17; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo17 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo17 OWNER TO pivotal;

--
-- Data for Name: foo17; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo17 (i) FROM stdin;
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
-- Name: foo170; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo170 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo170 OWNER TO pivotal;

--
-- Data for Name: foo170; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo170 (i) FROM stdin;
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
-- Name: foo171; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo171 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo171 OWNER TO pivotal;

--
-- Data for Name: foo171; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo171 (i) FROM stdin;
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
-- Name: foo172; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo172 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo172 OWNER TO pivotal;

--
-- Data for Name: foo172; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo172 (i) FROM stdin;
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
-- Name: foo173; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo173 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo173 OWNER TO pivotal;

--
-- Data for Name: foo173; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo173 (i) FROM stdin;
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
-- Name: foo174; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo174 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo174 OWNER TO pivotal;

--
-- Data for Name: foo174; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo174 (i) FROM stdin;
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
-- Name: foo175; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo175 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo175 OWNER TO pivotal;

--
-- Data for Name: foo175; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo175 (i) FROM stdin;
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
-- Name: foo176; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo176 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo176 OWNER TO pivotal;

--
-- Data for Name: foo176; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo176 (i) FROM stdin;
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
-- Name: foo177; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo177 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo177 OWNER TO pivotal;

--
-- Data for Name: foo177; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo177 (i) FROM stdin;
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
-- Name: foo178; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo178 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo178 OWNER TO pivotal;

--
-- Data for Name: foo178; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo178 (i) FROM stdin;
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
-- Name: foo179; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo179 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo179 OWNER TO pivotal;

--
-- Data for Name: foo179; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo179 (i) FROM stdin;
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
-- Name: foo18; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo18 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo18 OWNER TO pivotal;

--
-- Data for Name: foo18; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo18 (i) FROM stdin;
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
-- Name: foo180; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo180 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo180 OWNER TO pivotal;

--
-- Data for Name: foo180; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo180 (i) FROM stdin;
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
-- Name: foo181; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo181 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo181 OWNER TO pivotal;

--
-- Data for Name: foo181; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo181 (i) FROM stdin;
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
-- Name: foo182; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo182 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo182 OWNER TO pivotal;

--
-- Data for Name: foo182; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo182 (i) FROM stdin;
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
-- Name: foo183; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo183 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo183 OWNER TO pivotal;

--
-- Data for Name: foo183; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo183 (i) FROM stdin;
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
-- Name: foo184; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo184 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo184 OWNER TO pivotal;

--
-- Data for Name: foo184; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo184 (i) FROM stdin;
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
-- Name: foo185; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo185 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo185 OWNER TO pivotal;

--
-- Data for Name: foo185; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo185 (i) FROM stdin;
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
-- Name: foo186; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo186 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo186 OWNER TO pivotal;

--
-- Data for Name: foo186; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo186 (i) FROM stdin;
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
-- Name: foo187; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo187 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo187 OWNER TO pivotal;

--
-- Data for Name: foo187; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo187 (i) FROM stdin;
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
-- Name: foo188; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo188 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo188 OWNER TO pivotal;

--
-- Data for Name: foo188; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo188 (i) FROM stdin;
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
-- Name: foo189; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo189 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo189 OWNER TO pivotal;

--
-- Data for Name: foo189; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo189 (i) FROM stdin;
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
-- Name: foo19; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo19 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo19 OWNER TO pivotal;

--
-- Data for Name: foo19; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo19 (i) FROM stdin;
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
-- Name: foo190; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo190 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo190 OWNER TO pivotal;

--
-- Data for Name: foo190; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo190 (i) FROM stdin;
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
-- Name: foo191; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo191 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo191 OWNER TO pivotal;

--
-- Data for Name: foo191; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo191 (i) FROM stdin;
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
-- Name: foo192; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo192 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo192 OWNER TO pivotal;

--
-- Data for Name: foo192; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo192 (i) FROM stdin;
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
-- Name: foo193; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo193 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo193 OWNER TO pivotal;

--
-- Data for Name: foo193; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo193 (i) FROM stdin;
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
-- Name: foo194; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo194 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo194 OWNER TO pivotal;

--
-- Data for Name: foo194; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo194 (i) FROM stdin;
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
-- Name: foo195; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo195 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo195 OWNER TO pivotal;

--
-- Data for Name: foo195; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo195 (i) FROM stdin;
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
-- Name: foo196; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo196 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo196 OWNER TO pivotal;

--
-- Data for Name: foo196; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo196 (i) FROM stdin;
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
-- Name: foo197; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo197 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo197 OWNER TO pivotal;

--
-- Data for Name: foo197; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo197 (i) FROM stdin;
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
-- Name: foo198; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo198 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo198 OWNER TO pivotal;

--
-- Data for Name: foo198; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo198 (i) FROM stdin;
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
-- Name: foo199; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo199 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo199 OWNER TO pivotal;

--
-- Data for Name: foo199; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo199 (i) FROM stdin;
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
-- Name: foo2; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo2 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo2 OWNER TO pivotal;

--
-- Data for Name: foo2; Type: TABLE DATA; Schema: public; Owner: pivotal
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
-- Name: foo20; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo20 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo20 OWNER TO pivotal;

--
-- Data for Name: foo20; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo20 (i) FROM stdin;
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
-- Name: foo200; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo200 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo200 OWNER TO pivotal;

--
-- Data for Name: foo200; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo200 (i) FROM stdin;
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
-- Name: foo201; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo201 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo201 OWNER TO pivotal;

--
-- Data for Name: foo201; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo201 (i) FROM stdin;
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
-- Name: foo202; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo202 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo202 OWNER TO pivotal;

--
-- Data for Name: foo202; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo202 (i) FROM stdin;
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
-- Name: foo203; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo203 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo203 OWNER TO pivotal;

--
-- Data for Name: foo203; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo203 (i) FROM stdin;
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
-- Name: foo204; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo204 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo204 OWNER TO pivotal;

--
-- Data for Name: foo204; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo204 (i) FROM stdin;
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
-- Name: foo205; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo205 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo205 OWNER TO pivotal;

--
-- Data for Name: foo205; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo205 (i) FROM stdin;
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
-- Name: foo206; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo206 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo206 OWNER TO pivotal;

--
-- Data for Name: foo206; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo206 (i) FROM stdin;
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
-- Name: foo207; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo207 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo207 OWNER TO pivotal;

--
-- Data for Name: foo207; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo207 (i) FROM stdin;
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
-- Name: foo208; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo208 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo208 OWNER TO pivotal;

--
-- Data for Name: foo208; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo208 (i) FROM stdin;
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
-- Name: foo209; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo209 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo209 OWNER TO pivotal;

--
-- Data for Name: foo209; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo209 (i) FROM stdin;
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
-- Name: foo21; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo21 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo21 OWNER TO pivotal;

--
-- Data for Name: foo21; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo21 (i) FROM stdin;
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
-- Name: foo210; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo210 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo210 OWNER TO pivotal;

--
-- Data for Name: foo210; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo210 (i) FROM stdin;
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
-- Name: foo211; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo211 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo211 OWNER TO pivotal;

--
-- Data for Name: foo211; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo211 (i) FROM stdin;
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
-- Name: foo212; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo212 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo212 OWNER TO pivotal;

--
-- Data for Name: foo212; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo212 (i) FROM stdin;
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
-- Name: foo213; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo213 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo213 OWNER TO pivotal;

--
-- Data for Name: foo213; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo213 (i) FROM stdin;
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
-- Name: foo214; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo214 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo214 OWNER TO pivotal;

--
-- Data for Name: foo214; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo214 (i) FROM stdin;
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
-- Name: foo215; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo215 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo215 OWNER TO pivotal;

--
-- Data for Name: foo215; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo215 (i) FROM stdin;
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
-- Name: foo216; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo216 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo216 OWNER TO pivotal;

--
-- Data for Name: foo216; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo216 (i) FROM stdin;
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
-- Name: foo217; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo217 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo217 OWNER TO pivotal;

--
-- Data for Name: foo217; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo217 (i) FROM stdin;
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
-- Name: foo218; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo218 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo218 OWNER TO pivotal;

--
-- Data for Name: foo218; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo218 (i) FROM stdin;
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
-- Name: foo219; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo219 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo219 OWNER TO pivotal;

--
-- Data for Name: foo219; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo219 (i) FROM stdin;
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
-- Name: foo22; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo22 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo22 OWNER TO pivotal;

--
-- Data for Name: foo22; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo22 (i) FROM stdin;
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
-- Name: foo220; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo220 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo220 OWNER TO pivotal;

--
-- Data for Name: foo220; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo220 (i) FROM stdin;
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
-- Name: foo221; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo221 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo221 OWNER TO pivotal;

--
-- Data for Name: foo221; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo221 (i) FROM stdin;
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
-- Name: foo222; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo222 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo222 OWNER TO pivotal;

--
-- Data for Name: foo222; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo222 (i) FROM stdin;
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
-- Name: foo223; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo223 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo223 OWNER TO pivotal;

--
-- Data for Name: foo223; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo223 (i) FROM stdin;
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
-- Name: foo224; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo224 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo224 OWNER TO pivotal;

--
-- Data for Name: foo224; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo224 (i) FROM stdin;
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
-- Name: foo225; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo225 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo225 OWNER TO pivotal;

--
-- Data for Name: foo225; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo225 (i) FROM stdin;
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
-- Name: foo226; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo226 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo226 OWNER TO pivotal;

--
-- Data for Name: foo226; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo226 (i) FROM stdin;
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
-- Name: foo227; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo227 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo227 OWNER TO pivotal;

--
-- Data for Name: foo227; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo227 (i) FROM stdin;
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
-- Name: foo228; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo228 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo228 OWNER TO pivotal;

--
-- Data for Name: foo228; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo228 (i) FROM stdin;
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
-- Name: foo229; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo229 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo229 OWNER TO pivotal;

--
-- Data for Name: foo229; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo229 (i) FROM stdin;
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
-- Name: foo23; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo23 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo23 OWNER TO pivotal;

--
-- Data for Name: foo23; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo23 (i) FROM stdin;
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
-- Name: foo230; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo230 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo230 OWNER TO pivotal;

--
-- Data for Name: foo230; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo230 (i) FROM stdin;
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
-- Name: foo231; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo231 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo231 OWNER TO pivotal;

--
-- Data for Name: foo231; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo231 (i) FROM stdin;
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
-- Name: foo232; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo232 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo232 OWNER TO pivotal;

--
-- Data for Name: foo232; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo232 (i) FROM stdin;
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
-- Name: foo233; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo233 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo233 OWNER TO pivotal;

--
-- Data for Name: foo233; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo233 (i) FROM stdin;
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
-- Name: foo234; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo234 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo234 OWNER TO pivotal;

--
-- Data for Name: foo234; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo234 (i) FROM stdin;
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
-- Name: foo235; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo235 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo235 OWNER TO pivotal;

--
-- Data for Name: foo235; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo235 (i) FROM stdin;
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
-- Name: foo236; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo236 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo236 OWNER TO pivotal;

--
-- Data for Name: foo236; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo236 (i) FROM stdin;
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
-- Name: foo237; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo237 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo237 OWNER TO pivotal;

--
-- Data for Name: foo237; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo237 (i) FROM stdin;
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
-- Name: foo238; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo238 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo238 OWNER TO pivotal;

--
-- Data for Name: foo238; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo238 (i) FROM stdin;
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
-- Name: foo239; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo239 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo239 OWNER TO pivotal;

--
-- Data for Name: foo239; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo239 (i) FROM stdin;
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
-- Name: foo24; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo24 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo24 OWNER TO pivotal;

--
-- Data for Name: foo24; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo24 (i) FROM stdin;
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
-- Name: foo240; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo240 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo240 OWNER TO pivotal;

--
-- Data for Name: foo240; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo240 (i) FROM stdin;
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
-- Name: foo241; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo241 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo241 OWNER TO pivotal;

--
-- Data for Name: foo241; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo241 (i) FROM stdin;
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
-- Name: foo242; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo242 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo242 OWNER TO pivotal;

--
-- Data for Name: foo242; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo242 (i) FROM stdin;
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
-- Name: foo243; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo243 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo243 OWNER TO pivotal;

--
-- Data for Name: foo243; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo243 (i) FROM stdin;
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
-- Name: foo244; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo244 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo244 OWNER TO pivotal;

--
-- Data for Name: foo244; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo244 (i) FROM stdin;
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
-- Name: foo245; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo245 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo245 OWNER TO pivotal;

--
-- Data for Name: foo245; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo245 (i) FROM stdin;
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
-- Name: foo246; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo246 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo246 OWNER TO pivotal;

--
-- Data for Name: foo246; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo246 (i) FROM stdin;
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
-- Name: foo247; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo247 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo247 OWNER TO pivotal;

--
-- Data for Name: foo247; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo247 (i) FROM stdin;
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
-- Name: foo248; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo248 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo248 OWNER TO pivotal;

--
-- Data for Name: foo248; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo248 (i) FROM stdin;
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
-- Name: foo249; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo249 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo249 OWNER TO pivotal;

--
-- Data for Name: foo249; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo249 (i) FROM stdin;
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
-- Name: foo25; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo25 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo25 OWNER TO pivotal;

--
-- Data for Name: foo25; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo25 (i) FROM stdin;
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
-- Name: foo250; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo250 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo250 OWNER TO pivotal;

--
-- Data for Name: foo250; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo250 (i) FROM stdin;
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
-- Name: foo251; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo251 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo251 OWNER TO pivotal;

--
-- Data for Name: foo251; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo251 (i) FROM stdin;
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
-- Name: foo252; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo252 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo252 OWNER TO pivotal;

--
-- Data for Name: foo252; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo252 (i) FROM stdin;
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
-- Name: foo253; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo253 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo253 OWNER TO pivotal;

--
-- Data for Name: foo253; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo253 (i) FROM stdin;
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
-- Name: foo254; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo254 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo254 OWNER TO pivotal;

--
-- Data for Name: foo254; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo254 (i) FROM stdin;
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
-- Name: foo255; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo255 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo255 OWNER TO pivotal;

--
-- Data for Name: foo255; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo255 (i) FROM stdin;
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
-- Name: foo256; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo256 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo256 OWNER TO pivotal;

--
-- Data for Name: foo256; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo256 (i) FROM stdin;
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
-- Name: foo257; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo257 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo257 OWNER TO pivotal;

--
-- Data for Name: foo257; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo257 (i) FROM stdin;
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
-- Name: foo258; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo258 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo258 OWNER TO pivotal;

--
-- Data for Name: foo258; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo258 (i) FROM stdin;
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
-- Name: foo259; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo259 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo259 OWNER TO pivotal;

--
-- Data for Name: foo259; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo259 (i) FROM stdin;
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
-- Name: foo26; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo26 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo26 OWNER TO pivotal;

--
-- Data for Name: foo26; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo26 (i) FROM stdin;
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
-- Name: foo260; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo260 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo260 OWNER TO pivotal;

--
-- Data for Name: foo260; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo260 (i) FROM stdin;
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
-- Name: foo261; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo261 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo261 OWNER TO pivotal;

--
-- Data for Name: foo261; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo261 (i) FROM stdin;
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
-- Name: foo262; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo262 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo262 OWNER TO pivotal;

--
-- Data for Name: foo262; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo262 (i) FROM stdin;
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
-- Name: foo263; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo263 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo263 OWNER TO pivotal;

--
-- Data for Name: foo263; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo263 (i) FROM stdin;
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
-- Name: foo264; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo264 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo264 OWNER TO pivotal;

--
-- Data for Name: foo264; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo264 (i) FROM stdin;
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
-- Name: foo265; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo265 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo265 OWNER TO pivotal;

--
-- Data for Name: foo265; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo265 (i) FROM stdin;
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
-- Name: foo266; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo266 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo266 OWNER TO pivotal;

--
-- Data for Name: foo266; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo266 (i) FROM stdin;
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
-- Name: foo267; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo267 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo267 OWNER TO pivotal;

--
-- Data for Name: foo267; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo267 (i) FROM stdin;
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
-- Name: foo268; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo268 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo268 OWNER TO pivotal;

--
-- Data for Name: foo268; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo268 (i) FROM stdin;
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
-- Name: foo269; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo269 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo269 OWNER TO pivotal;

--
-- Data for Name: foo269; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo269 (i) FROM stdin;
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
-- Name: foo27; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo27 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo27 OWNER TO pivotal;

--
-- Data for Name: foo27; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo27 (i) FROM stdin;
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
-- Name: foo270; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo270 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo270 OWNER TO pivotal;

--
-- Data for Name: foo270; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo270 (i) FROM stdin;
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
-- Name: foo271; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo271 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo271 OWNER TO pivotal;

--
-- Data for Name: foo271; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo271 (i) FROM stdin;
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
-- Name: foo272; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo272 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo272 OWNER TO pivotal;

--
-- Data for Name: foo272; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo272 (i) FROM stdin;
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
-- Name: foo273; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo273 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo273 OWNER TO pivotal;

--
-- Data for Name: foo273; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo273 (i) FROM stdin;
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
-- Name: foo274; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo274 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo274 OWNER TO pivotal;

--
-- Data for Name: foo274; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo274 (i) FROM stdin;
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
-- Name: foo275; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo275 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo275 OWNER TO pivotal;

--
-- Data for Name: foo275; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo275 (i) FROM stdin;
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
-- Name: foo276; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo276 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo276 OWNER TO pivotal;

--
-- Data for Name: foo276; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo276 (i) FROM stdin;
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
-- Name: foo277; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo277 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo277 OWNER TO pivotal;

--
-- Data for Name: foo277; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo277 (i) FROM stdin;
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
-- Name: foo278; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo278 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo278 OWNER TO pivotal;

--
-- Data for Name: foo278; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo278 (i) FROM stdin;
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
-- Name: foo279; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo279 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo279 OWNER TO pivotal;

--
-- Data for Name: foo279; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo279 (i) FROM stdin;
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
-- Name: foo28; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo28 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo28 OWNER TO pivotal;

--
-- Data for Name: foo28; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo28 (i) FROM stdin;
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
-- Name: foo280; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo280 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo280 OWNER TO pivotal;

--
-- Data for Name: foo280; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo280 (i) FROM stdin;
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
-- Name: foo281; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo281 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo281 OWNER TO pivotal;

--
-- Data for Name: foo281; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo281 (i) FROM stdin;
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
-- Name: foo282; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo282 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo282 OWNER TO pivotal;

--
-- Data for Name: foo282; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo282 (i) FROM stdin;
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
-- Name: foo283; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo283 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo283 OWNER TO pivotal;

--
-- Data for Name: foo283; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo283 (i) FROM stdin;
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
-- Name: foo284; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo284 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo284 OWNER TO pivotal;

--
-- Data for Name: foo284; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo284 (i) FROM stdin;
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
-- Name: foo285; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo285 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo285 OWNER TO pivotal;

--
-- Data for Name: foo285; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo285 (i) FROM stdin;
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
-- Name: foo286; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo286 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo286 OWNER TO pivotal;

--
-- Data for Name: foo286; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo286 (i) FROM stdin;
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
-- Name: foo287; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo287 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo287 OWNER TO pivotal;

--
-- Data for Name: foo287; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo287 (i) FROM stdin;
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
-- Name: foo288; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo288 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo288 OWNER TO pivotal;

--
-- Data for Name: foo288; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo288 (i) FROM stdin;
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
-- Name: foo289; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo289 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo289 OWNER TO pivotal;

--
-- Data for Name: foo289; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo289 (i) FROM stdin;
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
-- Name: foo29; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo29 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo29 OWNER TO pivotal;

--
-- Data for Name: foo29; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo29 (i) FROM stdin;
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
-- Name: foo290; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo290 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo290 OWNER TO pivotal;

--
-- Data for Name: foo290; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo290 (i) FROM stdin;
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
-- Name: foo291; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo291 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo291 OWNER TO pivotal;

--
-- Data for Name: foo291; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo291 (i) FROM stdin;
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
-- Name: foo292; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo292 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo292 OWNER TO pivotal;

--
-- Data for Name: foo292; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo292 (i) FROM stdin;
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
-- Name: foo293; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo293 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo293 OWNER TO pivotal;

--
-- Data for Name: foo293; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo293 (i) FROM stdin;
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
-- Name: foo294; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo294 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo294 OWNER TO pivotal;

--
-- Data for Name: foo294; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo294 (i) FROM stdin;
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
-- Name: foo295; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo295 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo295 OWNER TO pivotal;

--
-- Data for Name: foo295; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo295 (i) FROM stdin;
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
-- Name: foo296; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo296 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo296 OWNER TO pivotal;

--
-- Data for Name: foo296; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo296 (i) FROM stdin;
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
-- Name: foo297; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo297 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo297 OWNER TO pivotal;

--
-- Data for Name: foo297; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo297 (i) FROM stdin;
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
-- Name: foo298; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo298 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo298 OWNER TO pivotal;

--
-- Data for Name: foo298; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo298 (i) FROM stdin;
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
-- Name: foo299; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo299 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo299 OWNER TO pivotal;

--
-- Data for Name: foo299; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo299 (i) FROM stdin;
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
-- Name: foo3; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo3 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo3 OWNER TO pivotal;

--
-- Data for Name: foo3; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo3 (i) FROM stdin;
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
-- Name: foo30; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo30 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo30 OWNER TO pivotal;

--
-- Data for Name: foo30; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo30 (i) FROM stdin;
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
-- Name: foo300; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo300 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo300 OWNER TO pivotal;

--
-- Data for Name: foo300; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo300 (i) FROM stdin;
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
-- Name: foo301; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo301 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo301 OWNER TO pivotal;

--
-- Data for Name: foo301; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo301 (i) FROM stdin;
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
-- Name: foo302; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo302 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo302 OWNER TO pivotal;

--
-- Data for Name: foo302; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo302 (i) FROM stdin;
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
-- Name: foo303; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo303 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo303 OWNER TO pivotal;

--
-- Data for Name: foo303; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo303 (i) FROM stdin;
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
-- Name: foo304; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo304 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo304 OWNER TO pivotal;

--
-- Data for Name: foo304; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo304 (i) FROM stdin;
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
-- Name: foo305; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo305 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo305 OWNER TO pivotal;

--
-- Data for Name: foo305; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo305 (i) FROM stdin;
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
-- Name: foo306; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo306 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo306 OWNER TO pivotal;

--
-- Data for Name: foo306; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo306 (i) FROM stdin;
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
-- Name: foo307; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo307 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo307 OWNER TO pivotal;

--
-- Data for Name: foo307; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo307 (i) FROM stdin;
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
-- Name: foo308; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo308 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo308 OWNER TO pivotal;

--
-- Data for Name: foo308; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo308 (i) FROM stdin;
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
-- Name: foo309; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo309 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo309 OWNER TO pivotal;

--
-- Data for Name: foo309; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo309 (i) FROM stdin;
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
-- Name: foo31; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo31 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo31 OWNER TO pivotal;

--
-- Data for Name: foo31; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo31 (i) FROM stdin;
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
-- Name: foo310; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo310 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo310 OWNER TO pivotal;

--
-- Data for Name: foo310; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo310 (i) FROM stdin;
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
-- Name: foo311; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo311 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo311 OWNER TO pivotal;

--
-- Data for Name: foo311; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo311 (i) FROM stdin;
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
-- Name: foo312; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo312 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo312 OWNER TO pivotal;

--
-- Data for Name: foo312; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo312 (i) FROM stdin;
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
-- Name: foo313; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo313 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo313 OWNER TO pivotal;

--
-- Data for Name: foo313; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo313 (i) FROM stdin;
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
-- Name: foo314; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo314 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo314 OWNER TO pivotal;

--
-- Data for Name: foo314; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo314 (i) FROM stdin;
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
-- Name: foo315; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo315 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo315 OWNER TO pivotal;

--
-- Data for Name: foo315; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo315 (i) FROM stdin;
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
-- Name: foo316; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo316 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo316 OWNER TO pivotal;

--
-- Data for Name: foo316; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo316 (i) FROM stdin;
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
-- Name: foo317; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo317 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo317 OWNER TO pivotal;

--
-- Data for Name: foo317; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo317 (i) FROM stdin;
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
-- Name: foo318; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo318 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo318 OWNER TO pivotal;

--
-- Data for Name: foo318; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo318 (i) FROM stdin;
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
-- Name: foo319; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo319 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo319 OWNER TO pivotal;

--
-- Data for Name: foo319; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo319 (i) FROM stdin;
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
-- Name: foo32; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo32 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo32 OWNER TO pivotal;

--
-- Data for Name: foo32; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo32 (i) FROM stdin;
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
-- Name: foo320; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo320 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo320 OWNER TO pivotal;

--
-- Data for Name: foo320; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo320 (i) FROM stdin;
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
-- Name: foo321; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo321 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo321 OWNER TO pivotal;

--
-- Data for Name: foo321; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo321 (i) FROM stdin;
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
-- Name: foo322; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo322 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo322 OWNER TO pivotal;

--
-- Data for Name: foo322; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo322 (i) FROM stdin;
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
-- Name: foo323; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo323 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo323 OWNER TO pivotal;

--
-- Data for Name: foo323; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo323 (i) FROM stdin;
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
-- Name: foo324; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo324 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo324 OWNER TO pivotal;

--
-- Data for Name: foo324; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo324 (i) FROM stdin;
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
-- Name: foo325; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo325 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo325 OWNER TO pivotal;

--
-- Data for Name: foo325; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo325 (i) FROM stdin;
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
-- Name: foo326; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo326 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo326 OWNER TO pivotal;

--
-- Data for Name: foo326; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo326 (i) FROM stdin;
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
-- Name: foo327; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo327 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo327 OWNER TO pivotal;

--
-- Data for Name: foo327; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo327 (i) FROM stdin;
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
-- Name: foo328; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo328 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo328 OWNER TO pivotal;

--
-- Data for Name: foo328; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo328 (i) FROM stdin;
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
-- Name: foo329; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo329 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo329 OWNER TO pivotal;

--
-- Data for Name: foo329; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo329 (i) FROM stdin;
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
-- Name: foo33; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo33 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo33 OWNER TO pivotal;

--
-- Data for Name: foo33; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo33 (i) FROM stdin;
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
-- Name: foo330; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo330 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo330 OWNER TO pivotal;

--
-- Data for Name: foo330; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo330 (i) FROM stdin;
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
-- Name: foo331; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo331 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo331 OWNER TO pivotal;

--
-- Data for Name: foo331; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo331 (i) FROM stdin;
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
-- Name: foo332; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo332 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo332 OWNER TO pivotal;

--
-- Data for Name: foo332; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo332 (i) FROM stdin;
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
-- Name: foo333; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo333 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo333 OWNER TO pivotal;

--
-- Data for Name: foo333; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo333 (i) FROM stdin;
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
-- Name: foo334; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo334 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo334 OWNER TO pivotal;

--
-- Data for Name: foo334; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo334 (i) FROM stdin;
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
-- Name: foo335; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo335 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo335 OWNER TO pivotal;

--
-- Data for Name: foo335; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo335 (i) FROM stdin;
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
-- Name: foo336; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo336 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo336 OWNER TO pivotal;

--
-- Data for Name: foo336; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo336 (i) FROM stdin;
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
-- Name: foo337; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo337 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo337 OWNER TO pivotal;

--
-- Data for Name: foo337; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo337 (i) FROM stdin;
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
-- Name: foo338; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo338 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo338 OWNER TO pivotal;

--
-- Data for Name: foo338; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo338 (i) FROM stdin;
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
-- Name: foo339; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo339 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo339 OWNER TO pivotal;

--
-- Data for Name: foo339; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo339 (i) FROM stdin;
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
-- Name: foo34; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo34 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo34 OWNER TO pivotal;

--
-- Data for Name: foo34; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo34 (i) FROM stdin;
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
-- Name: foo340; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo340 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo340 OWNER TO pivotal;

--
-- Data for Name: foo340; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo340 (i) FROM stdin;
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
-- Name: foo341; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo341 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo341 OWNER TO pivotal;

--
-- Data for Name: foo341; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo341 (i) FROM stdin;
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
-- Name: foo342; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo342 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo342 OWNER TO pivotal;

--
-- Data for Name: foo342; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo342 (i) FROM stdin;
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
-- Name: foo343; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo343 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo343 OWNER TO pivotal;

--
-- Data for Name: foo343; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo343 (i) FROM stdin;
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
-- Name: foo344; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo344 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo344 OWNER TO pivotal;

--
-- Data for Name: foo344; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo344 (i) FROM stdin;
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
-- Name: foo345; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo345 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo345 OWNER TO pivotal;

--
-- Data for Name: foo345; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo345 (i) FROM stdin;
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
-- Name: foo346; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo346 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo346 OWNER TO pivotal;

--
-- Data for Name: foo346; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo346 (i) FROM stdin;
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
-- Name: foo347; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo347 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo347 OWNER TO pivotal;

--
-- Data for Name: foo347; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo347 (i) FROM stdin;
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
-- Name: foo348; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo348 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo348 OWNER TO pivotal;

--
-- Data for Name: foo348; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo348 (i) FROM stdin;
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
-- Name: foo349; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo349 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo349 OWNER TO pivotal;

--
-- Data for Name: foo349; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo349 (i) FROM stdin;
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
-- Name: foo35; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo35 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo35 OWNER TO pivotal;

--
-- Data for Name: foo35; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo35 (i) FROM stdin;
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
-- Name: foo350; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo350 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo350 OWNER TO pivotal;

--
-- Data for Name: foo350; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo350 (i) FROM stdin;
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
-- Name: foo351; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo351 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo351 OWNER TO pivotal;

--
-- Data for Name: foo351; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo351 (i) FROM stdin;
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
-- Name: foo352; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo352 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo352 OWNER TO pivotal;

--
-- Data for Name: foo352; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo352 (i) FROM stdin;
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
-- Name: foo353; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo353 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo353 OWNER TO pivotal;

--
-- Data for Name: foo353; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo353 (i) FROM stdin;
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
-- Name: foo354; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo354 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo354 OWNER TO pivotal;

--
-- Data for Name: foo354; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo354 (i) FROM stdin;
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
-- Name: foo355; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo355 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo355 OWNER TO pivotal;

--
-- Data for Name: foo355; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo355 (i) FROM stdin;
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
-- Name: foo356; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo356 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo356 OWNER TO pivotal;

--
-- Data for Name: foo356; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo356 (i) FROM stdin;
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
-- Name: foo357; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo357 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo357 OWNER TO pivotal;

--
-- Data for Name: foo357; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo357 (i) FROM stdin;
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
-- Name: foo358; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo358 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo358 OWNER TO pivotal;

--
-- Data for Name: foo358; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo358 (i) FROM stdin;
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
-- Name: foo359; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo359 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo359 OWNER TO pivotal;

--
-- Data for Name: foo359; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo359 (i) FROM stdin;
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
-- Name: foo36; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo36 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo36 OWNER TO pivotal;

--
-- Data for Name: foo36; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo36 (i) FROM stdin;
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
-- Name: foo360; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo360 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo360 OWNER TO pivotal;

--
-- Data for Name: foo360; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo360 (i) FROM stdin;
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
-- Name: foo361; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo361 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo361 OWNER TO pivotal;

--
-- Data for Name: foo361; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo361 (i) FROM stdin;
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
-- Name: foo362; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo362 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo362 OWNER TO pivotal;

--
-- Data for Name: foo362; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo362 (i) FROM stdin;
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
-- Name: foo363; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo363 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo363 OWNER TO pivotal;

--
-- Data for Name: foo363; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo363 (i) FROM stdin;
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
-- Name: foo364; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo364 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo364 OWNER TO pivotal;

--
-- Data for Name: foo364; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo364 (i) FROM stdin;
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
-- Name: foo365; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo365 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo365 OWNER TO pivotal;

--
-- Data for Name: foo365; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo365 (i) FROM stdin;
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
-- Name: foo366; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo366 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo366 OWNER TO pivotal;

--
-- Data for Name: foo366; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo366 (i) FROM stdin;
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
-- Name: foo367; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo367 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo367 OWNER TO pivotal;

--
-- Data for Name: foo367; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo367 (i) FROM stdin;
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
-- Name: foo368; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo368 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo368 OWNER TO pivotal;

--
-- Data for Name: foo368; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo368 (i) FROM stdin;
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
-- Name: foo369; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo369 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo369 OWNER TO pivotal;

--
-- Data for Name: foo369; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo369 (i) FROM stdin;
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
-- Name: foo37; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo37 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo37 OWNER TO pivotal;

--
-- Data for Name: foo37; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo37 (i) FROM stdin;
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
-- Name: foo370; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo370 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo370 OWNER TO pivotal;

--
-- Data for Name: foo370; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo370 (i) FROM stdin;
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
-- Name: foo371; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo371 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo371 OWNER TO pivotal;

--
-- Data for Name: foo371; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo371 (i) FROM stdin;
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
-- Name: foo372; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo372 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo372 OWNER TO pivotal;

--
-- Data for Name: foo372; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo372 (i) FROM stdin;
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
-- Name: foo373; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo373 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo373 OWNER TO pivotal;

--
-- Data for Name: foo373; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo373 (i) FROM stdin;
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
-- Name: foo374; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo374 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo374 OWNER TO pivotal;

--
-- Data for Name: foo374; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo374 (i) FROM stdin;
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
-- Name: foo375; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo375 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo375 OWNER TO pivotal;

--
-- Data for Name: foo375; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo375 (i) FROM stdin;
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
-- Name: foo376; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo376 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo376 OWNER TO pivotal;

--
-- Data for Name: foo376; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo376 (i) FROM stdin;
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
-- Name: foo377; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo377 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo377 OWNER TO pivotal;

--
-- Data for Name: foo377; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo377 (i) FROM stdin;
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
-- Name: foo378; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo378 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo378 OWNER TO pivotal;

--
-- Data for Name: foo378; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo378 (i) FROM stdin;
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
-- Name: foo379; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo379 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo379 OWNER TO pivotal;

--
-- Data for Name: foo379; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo379 (i) FROM stdin;
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
-- Name: foo38; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo38 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo38 OWNER TO pivotal;

--
-- Data for Name: foo38; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo38 (i) FROM stdin;
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
-- Name: foo380; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo380 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo380 OWNER TO pivotal;

--
-- Data for Name: foo380; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo380 (i) FROM stdin;
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
-- Name: foo381; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo381 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo381 OWNER TO pivotal;

--
-- Data for Name: foo381; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo381 (i) FROM stdin;
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
-- Name: foo382; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo382 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo382 OWNER TO pivotal;

--
-- Data for Name: foo382; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo382 (i) FROM stdin;
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
-- Name: foo383; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo383 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo383 OWNER TO pivotal;

--
-- Data for Name: foo383; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo383 (i) FROM stdin;
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
-- Name: foo384; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo384 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo384 OWNER TO pivotal;

--
-- Data for Name: foo384; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo384 (i) FROM stdin;
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
-- Name: foo385; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo385 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo385 OWNER TO pivotal;

--
-- Data for Name: foo385; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo385 (i) FROM stdin;
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
-- Name: foo386; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo386 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo386 OWNER TO pivotal;

--
-- Data for Name: foo386; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo386 (i) FROM stdin;
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
-- Name: foo387; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo387 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo387 OWNER TO pivotal;

--
-- Data for Name: foo387; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo387 (i) FROM stdin;
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
-- Name: foo388; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo388 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo388 OWNER TO pivotal;

--
-- Data for Name: foo388; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo388 (i) FROM stdin;
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
-- Name: foo389; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo389 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo389 OWNER TO pivotal;

--
-- Data for Name: foo389; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo389 (i) FROM stdin;
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
-- Name: foo39; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo39 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo39 OWNER TO pivotal;

--
-- Data for Name: foo39; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo39 (i) FROM stdin;
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
-- Name: foo390; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo390 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo390 OWNER TO pivotal;

--
-- Data for Name: foo390; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo390 (i) FROM stdin;
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
-- Name: foo391; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo391 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo391 OWNER TO pivotal;

--
-- Data for Name: foo391; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo391 (i) FROM stdin;
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
-- Name: foo392; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo392 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo392 OWNER TO pivotal;

--
-- Data for Name: foo392; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo392 (i) FROM stdin;
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
-- Name: foo393; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo393 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo393 OWNER TO pivotal;

--
-- Data for Name: foo393; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo393 (i) FROM stdin;
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
-- Name: foo394; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo394 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo394 OWNER TO pivotal;

--
-- Data for Name: foo394; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo394 (i) FROM stdin;
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
-- Name: foo395; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo395 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo395 OWNER TO pivotal;

--
-- Data for Name: foo395; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo395 (i) FROM stdin;
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
-- Name: foo396; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo396 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo396 OWNER TO pivotal;

--
-- Data for Name: foo396; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo396 (i) FROM stdin;
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
-- Name: foo397; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo397 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo397 OWNER TO pivotal;

--
-- Data for Name: foo397; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo397 (i) FROM stdin;
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
-- Name: foo398; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo398 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo398 OWNER TO pivotal;

--
-- Data for Name: foo398; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo398 (i) FROM stdin;
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
-- Name: foo399; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo399 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo399 OWNER TO pivotal;

--
-- Data for Name: foo399; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo399 (i) FROM stdin;
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
-- Name: foo4; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo4 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo4 OWNER TO pivotal;

--
-- Data for Name: foo4; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo4 (i) FROM stdin;
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
-- Name: foo40; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo40 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo40 OWNER TO pivotal;

--
-- Data for Name: foo40; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo40 (i) FROM stdin;
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
-- Name: foo400; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo400 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo400 OWNER TO pivotal;

--
-- Data for Name: foo400; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo400 (i) FROM stdin;
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
-- Name: foo401; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo401 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo401 OWNER TO pivotal;

--
-- Data for Name: foo401; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo401 (i) FROM stdin;
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
-- Name: foo402; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo402 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo402 OWNER TO pivotal;

--
-- Data for Name: foo402; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo402 (i) FROM stdin;
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
-- Name: foo403; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo403 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo403 OWNER TO pivotal;

--
-- Data for Name: foo403; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo403 (i) FROM stdin;
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
-- Name: foo404; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo404 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo404 OWNER TO pivotal;

--
-- Data for Name: foo404; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo404 (i) FROM stdin;
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
-- Name: foo405; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo405 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo405 OWNER TO pivotal;

--
-- Data for Name: foo405; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo405 (i) FROM stdin;
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
-- Name: foo406; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo406 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo406 OWNER TO pivotal;

--
-- Data for Name: foo406; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo406 (i) FROM stdin;
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
-- Name: foo407; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo407 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo407 OWNER TO pivotal;

--
-- Data for Name: foo407; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo407 (i) FROM stdin;
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
-- Name: foo408; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo408 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo408 OWNER TO pivotal;

--
-- Data for Name: foo408; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo408 (i) FROM stdin;
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
-- Name: foo409; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo409 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo409 OWNER TO pivotal;

--
-- Data for Name: foo409; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo409 (i) FROM stdin;
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
-- Name: foo41; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo41 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo41 OWNER TO pivotal;

--
-- Data for Name: foo41; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo41 (i) FROM stdin;
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
-- Name: foo410; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo410 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo410 OWNER TO pivotal;

--
-- Data for Name: foo410; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo410 (i) FROM stdin;
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
-- Name: foo411; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo411 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo411 OWNER TO pivotal;

--
-- Data for Name: foo411; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo411 (i) FROM stdin;
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
-- Name: foo412; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo412 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo412 OWNER TO pivotal;

--
-- Data for Name: foo412; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo412 (i) FROM stdin;
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
-- Name: foo413; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo413 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo413 OWNER TO pivotal;

--
-- Data for Name: foo413; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo413 (i) FROM stdin;
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
-- Name: foo414; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo414 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo414 OWNER TO pivotal;

--
-- Data for Name: foo414; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo414 (i) FROM stdin;
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
-- Name: foo415; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo415 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo415 OWNER TO pivotal;

--
-- Data for Name: foo415; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo415 (i) FROM stdin;
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
-- Name: foo416; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo416 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo416 OWNER TO pivotal;

--
-- Data for Name: foo416; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo416 (i) FROM stdin;
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
-- Name: foo417; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo417 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo417 OWNER TO pivotal;

--
-- Data for Name: foo417; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo417 (i) FROM stdin;
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
-- Name: foo418; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo418 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo418 OWNER TO pivotal;

--
-- Data for Name: foo418; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo418 (i) FROM stdin;
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
-- Name: foo419; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo419 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo419 OWNER TO pivotal;

--
-- Data for Name: foo419; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo419 (i) FROM stdin;
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
-- Name: foo42; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo42 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo42 OWNER TO pivotal;

--
-- Data for Name: foo42; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo42 (i) FROM stdin;
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
-- Name: foo420; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo420 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo420 OWNER TO pivotal;

--
-- Data for Name: foo420; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo420 (i) FROM stdin;
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
-- Name: foo421; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo421 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo421 OWNER TO pivotal;

--
-- Data for Name: foo421; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo421 (i) FROM stdin;
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
-- Name: foo422; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo422 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo422 OWNER TO pivotal;

--
-- Data for Name: foo422; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo422 (i) FROM stdin;
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
-- Name: foo423; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo423 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo423 OWNER TO pivotal;

--
-- Data for Name: foo423; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo423 (i) FROM stdin;
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
-- Name: foo424; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo424 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo424 OWNER TO pivotal;

--
-- Data for Name: foo424; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo424 (i) FROM stdin;
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
-- Name: foo425; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo425 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo425 OWNER TO pivotal;

--
-- Data for Name: foo425; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo425 (i) FROM stdin;
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
-- Name: foo426; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo426 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo426 OWNER TO pivotal;

--
-- Data for Name: foo426; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo426 (i) FROM stdin;
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
-- Name: foo427; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo427 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo427 OWNER TO pivotal;

--
-- Data for Name: foo427; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo427 (i) FROM stdin;
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
-- Name: foo428; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo428 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo428 OWNER TO pivotal;

--
-- Data for Name: foo428; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo428 (i) FROM stdin;
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
-- Name: foo429; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo429 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo429 OWNER TO pivotal;

--
-- Data for Name: foo429; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo429 (i) FROM stdin;
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
-- Name: foo43; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo43 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo43 OWNER TO pivotal;

--
-- Data for Name: foo43; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo43 (i) FROM stdin;
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
-- Name: foo430; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo430 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo430 OWNER TO pivotal;

--
-- Data for Name: foo430; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo430 (i) FROM stdin;
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
-- Name: foo431; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo431 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo431 OWNER TO pivotal;

--
-- Data for Name: foo431; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo431 (i) FROM stdin;
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
-- Name: foo432; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo432 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo432 OWNER TO pivotal;

--
-- Data for Name: foo432; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo432 (i) FROM stdin;
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
-- Name: foo433; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo433 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo433 OWNER TO pivotal;

--
-- Data for Name: foo433; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo433 (i) FROM stdin;
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
-- Name: foo434; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo434 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo434 OWNER TO pivotal;

--
-- Data for Name: foo434; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo434 (i) FROM stdin;
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
-- Name: foo435; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo435 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo435 OWNER TO pivotal;

--
-- Data for Name: foo435; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo435 (i) FROM stdin;
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
-- Name: foo436; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo436 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo436 OWNER TO pivotal;

--
-- Data for Name: foo436; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo436 (i) FROM stdin;
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
-- Name: foo437; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo437 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo437 OWNER TO pivotal;

--
-- Data for Name: foo437; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo437 (i) FROM stdin;
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
-- Name: foo438; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo438 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo438 OWNER TO pivotal;

--
-- Data for Name: foo438; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo438 (i) FROM stdin;
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
-- Name: foo439; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo439 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo439 OWNER TO pivotal;

--
-- Data for Name: foo439; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo439 (i) FROM stdin;
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
-- Name: foo44; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo44 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo44 OWNER TO pivotal;

--
-- Data for Name: foo44; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo44 (i) FROM stdin;
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
-- Name: foo440; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo440 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo440 OWNER TO pivotal;

--
-- Data for Name: foo440; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo440 (i) FROM stdin;
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
-- Name: foo441; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo441 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo441 OWNER TO pivotal;

--
-- Data for Name: foo441; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo441 (i) FROM stdin;
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
-- Name: foo442; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo442 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo442 OWNER TO pivotal;

--
-- Data for Name: foo442; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo442 (i) FROM stdin;
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
-- Name: foo443; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo443 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo443 OWNER TO pivotal;

--
-- Data for Name: foo443; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo443 (i) FROM stdin;
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
-- Name: foo444; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo444 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo444 OWNER TO pivotal;

--
-- Data for Name: foo444; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo444 (i) FROM stdin;
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
-- Name: foo445; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo445 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo445 OWNER TO pivotal;

--
-- Data for Name: foo445; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo445 (i) FROM stdin;
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
-- Name: foo446; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo446 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo446 OWNER TO pivotal;

--
-- Data for Name: foo446; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo446 (i) FROM stdin;
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
-- Name: foo447; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo447 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo447 OWNER TO pivotal;

--
-- Data for Name: foo447; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo447 (i) FROM stdin;
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
-- Name: foo448; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo448 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo448 OWNER TO pivotal;

--
-- Data for Name: foo448; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo448 (i) FROM stdin;
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
-- Name: foo449; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo449 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo449 OWNER TO pivotal;

--
-- Data for Name: foo449; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo449 (i) FROM stdin;
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
-- Name: foo45; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo45 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo45 OWNER TO pivotal;

--
-- Data for Name: foo45; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo45 (i) FROM stdin;
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
-- Name: foo450; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo450 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo450 OWNER TO pivotal;

--
-- Data for Name: foo450; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo450 (i) FROM stdin;
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
-- Name: foo451; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo451 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo451 OWNER TO pivotal;

--
-- Data for Name: foo451; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo451 (i) FROM stdin;
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
-- Name: foo452; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo452 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo452 OWNER TO pivotal;

--
-- Data for Name: foo452; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo452 (i) FROM stdin;
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
-- Name: foo453; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo453 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo453 OWNER TO pivotal;

--
-- Data for Name: foo453; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo453 (i) FROM stdin;
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
-- Name: foo454; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo454 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo454 OWNER TO pivotal;

--
-- Data for Name: foo454; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo454 (i) FROM stdin;
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
-- Name: foo455; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo455 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo455 OWNER TO pivotal;

--
-- Data for Name: foo455; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo455 (i) FROM stdin;
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
-- Name: foo456; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo456 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo456 OWNER TO pivotal;

--
-- Data for Name: foo456; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo456 (i) FROM stdin;
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
-- Name: foo457; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo457 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo457 OWNER TO pivotal;

--
-- Data for Name: foo457; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo457 (i) FROM stdin;
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
-- Name: foo458; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo458 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo458 OWNER TO pivotal;

--
-- Data for Name: foo458; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo458 (i) FROM stdin;
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
-- Name: foo459; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo459 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo459 OWNER TO pivotal;

--
-- Data for Name: foo459; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo459 (i) FROM stdin;
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
-- Name: foo46; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo46 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo46 OWNER TO pivotal;

--
-- Data for Name: foo46; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo46 (i) FROM stdin;
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
-- Name: foo460; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo460 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo460 OWNER TO pivotal;

--
-- Data for Name: foo460; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo460 (i) FROM stdin;
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
-- Name: foo461; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo461 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo461 OWNER TO pivotal;

--
-- Data for Name: foo461; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo461 (i) FROM stdin;
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
-- Name: foo462; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo462 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo462 OWNER TO pivotal;

--
-- Data for Name: foo462; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo462 (i) FROM stdin;
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
-- Name: foo463; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo463 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo463 OWNER TO pivotal;

--
-- Data for Name: foo463; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo463 (i) FROM stdin;
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
-- Name: foo464; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo464 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo464 OWNER TO pivotal;

--
-- Data for Name: foo464; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo464 (i) FROM stdin;
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
-- Name: foo465; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo465 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo465 OWNER TO pivotal;

--
-- Data for Name: foo465; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo465 (i) FROM stdin;
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
-- Name: foo466; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo466 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo466 OWNER TO pivotal;

--
-- Data for Name: foo466; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo466 (i) FROM stdin;
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
-- Name: foo467; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo467 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo467 OWNER TO pivotal;

--
-- Data for Name: foo467; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo467 (i) FROM stdin;
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
-- Name: foo468; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo468 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo468 OWNER TO pivotal;

--
-- Data for Name: foo468; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo468 (i) FROM stdin;
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
-- Name: foo469; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo469 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo469 OWNER TO pivotal;

--
-- Data for Name: foo469; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo469 (i) FROM stdin;
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
-- Name: foo47; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo47 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo47 OWNER TO pivotal;

--
-- Data for Name: foo47; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo47 (i) FROM stdin;
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
-- Name: foo470; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo470 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo470 OWNER TO pivotal;

--
-- Data for Name: foo470; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo470 (i) FROM stdin;
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
-- Name: foo471; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo471 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo471 OWNER TO pivotal;

--
-- Data for Name: foo471; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo471 (i) FROM stdin;
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
-- Name: foo472; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo472 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo472 OWNER TO pivotal;

--
-- Data for Name: foo472; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo472 (i) FROM stdin;
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
-- Name: foo473; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo473 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo473 OWNER TO pivotal;

--
-- Data for Name: foo473; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo473 (i) FROM stdin;
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
-- Name: foo474; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo474 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo474 OWNER TO pivotal;

--
-- Data for Name: foo474; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo474 (i) FROM stdin;
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
-- Name: foo475; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo475 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo475 OWNER TO pivotal;

--
-- Data for Name: foo475; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo475 (i) FROM stdin;
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
-- Name: foo476; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo476 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo476 OWNER TO pivotal;

--
-- Data for Name: foo476; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo476 (i) FROM stdin;
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
-- Name: foo477; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo477 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo477 OWNER TO pivotal;

--
-- Data for Name: foo477; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo477 (i) FROM stdin;
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
-- Name: foo478; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo478 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo478 OWNER TO pivotal;

--
-- Data for Name: foo478; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo478 (i) FROM stdin;
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
-- Name: foo479; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo479 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo479 OWNER TO pivotal;

--
-- Data for Name: foo479; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo479 (i) FROM stdin;
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
-- Name: foo48; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo48 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo48 OWNER TO pivotal;

--
-- Data for Name: foo48; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo48 (i) FROM stdin;
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
-- Name: foo480; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo480 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo480 OWNER TO pivotal;

--
-- Data for Name: foo480; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo480 (i) FROM stdin;
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
-- Name: foo481; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo481 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo481 OWNER TO pivotal;

--
-- Data for Name: foo481; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo481 (i) FROM stdin;
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
-- Name: foo482; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo482 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo482 OWNER TO pivotal;

--
-- Data for Name: foo482; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo482 (i) FROM stdin;
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
-- Name: foo483; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo483 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo483 OWNER TO pivotal;

--
-- Data for Name: foo483; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo483 (i) FROM stdin;
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
-- Name: foo484; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo484 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo484 OWNER TO pivotal;

--
-- Data for Name: foo484; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo484 (i) FROM stdin;
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
-- Name: foo485; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo485 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo485 OWNER TO pivotal;

--
-- Data for Name: foo485; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo485 (i) FROM stdin;
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
-- Name: foo486; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo486 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo486 OWNER TO pivotal;

--
-- Data for Name: foo486; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo486 (i) FROM stdin;
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
-- Name: foo487; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo487 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo487 OWNER TO pivotal;

--
-- Data for Name: foo487; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo487 (i) FROM stdin;
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
-- Name: foo488; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo488 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo488 OWNER TO pivotal;

--
-- Data for Name: foo488; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo488 (i) FROM stdin;
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
-- Name: foo489; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo489 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo489 OWNER TO pivotal;

--
-- Data for Name: foo489; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo489 (i) FROM stdin;
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
-- Name: foo49; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo49 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo49 OWNER TO pivotal;

--
-- Data for Name: foo49; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo49 (i) FROM stdin;
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
-- Name: foo490; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo490 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo490 OWNER TO pivotal;

--
-- Data for Name: foo490; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo490 (i) FROM stdin;
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
-- Name: foo491; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo491 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo491 OWNER TO pivotal;

--
-- Data for Name: foo491; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo491 (i) FROM stdin;
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
-- Name: foo492; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo492 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo492 OWNER TO pivotal;

--
-- Data for Name: foo492; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo492 (i) FROM stdin;
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
-- Name: foo493; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo493 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo493 OWNER TO pivotal;

--
-- Data for Name: foo493; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo493 (i) FROM stdin;
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
-- Name: foo494; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo494 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo494 OWNER TO pivotal;

--
-- Data for Name: foo494; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo494 (i) FROM stdin;
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
-- Name: foo495; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo495 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo495 OWNER TO pivotal;

--
-- Data for Name: foo495; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo495 (i) FROM stdin;
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
-- Name: foo496; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo496 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo496 OWNER TO pivotal;

--
-- Data for Name: foo496; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo496 (i) FROM stdin;
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
-- Name: foo497; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo497 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo497 OWNER TO pivotal;

--
-- Data for Name: foo497; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo497 (i) FROM stdin;
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
-- Name: foo498; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo498 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo498 OWNER TO pivotal;

--
-- Data for Name: foo498; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo498 (i) FROM stdin;
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
-- Name: foo499; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo499 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo499 OWNER TO pivotal;

--
-- Data for Name: foo499; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo499 (i) FROM stdin;
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
-- Name: foo5; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo5 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo5 OWNER TO pivotal;

--
-- Data for Name: foo5; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo5 (i) FROM stdin;
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
-- Name: foo50; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo50 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo50 OWNER TO pivotal;

--
-- Data for Name: foo50; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo50 (i) FROM stdin;
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
-- Name: foo500; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo500 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo500 OWNER TO pivotal;

--
-- Data for Name: foo500; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo500 (i) FROM stdin;
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
-- Name: foo501; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo501 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo501 OWNER TO pivotal;

--
-- Data for Name: foo501; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo501 (i) FROM stdin;
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
-- Name: foo502; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo502 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo502 OWNER TO pivotal;

--
-- Data for Name: foo502; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo502 (i) FROM stdin;
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
-- Name: foo503; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo503 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo503 OWNER TO pivotal;

--
-- Data for Name: foo503; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo503 (i) FROM stdin;
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
-- Name: foo504; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo504 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo504 OWNER TO pivotal;

--
-- Data for Name: foo504; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo504 (i) FROM stdin;
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
-- Name: foo505; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo505 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo505 OWNER TO pivotal;

--
-- Data for Name: foo505; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo505 (i) FROM stdin;
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
-- Name: foo506; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo506 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo506 OWNER TO pivotal;

--
-- Data for Name: foo506; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo506 (i) FROM stdin;
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
-- Name: foo507; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo507 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo507 OWNER TO pivotal;

--
-- Data for Name: foo507; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo507 (i) FROM stdin;
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
-- Name: foo508; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo508 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo508 OWNER TO pivotal;

--
-- Data for Name: foo508; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo508 (i) FROM stdin;
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
-- Name: foo509; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo509 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo509 OWNER TO pivotal;

--
-- Data for Name: foo509; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo509 (i) FROM stdin;
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
-- Name: foo51; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo51 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo51 OWNER TO pivotal;

--
-- Data for Name: foo51; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo51 (i) FROM stdin;
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
-- Name: foo510; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo510 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo510 OWNER TO pivotal;

--
-- Data for Name: foo510; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo510 (i) FROM stdin;
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
-- Name: foo511; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo511 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo511 OWNER TO pivotal;

--
-- Data for Name: foo511; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo511 (i) FROM stdin;
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
-- Name: foo512; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo512 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo512 OWNER TO pivotal;

--
-- Data for Name: foo512; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo512 (i) FROM stdin;
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
-- Name: foo513; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo513 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo513 OWNER TO pivotal;

--
-- Data for Name: foo513; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo513 (i) FROM stdin;
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
-- Name: foo514; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo514 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo514 OWNER TO pivotal;

--
-- Data for Name: foo514; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo514 (i) FROM stdin;
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
-- Name: foo515; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo515 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo515 OWNER TO pivotal;

--
-- Data for Name: foo515; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo515 (i) FROM stdin;
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
-- Name: foo516; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo516 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo516 OWNER TO pivotal;

--
-- Data for Name: foo516; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo516 (i) FROM stdin;
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
-- Name: foo517; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo517 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo517 OWNER TO pivotal;

--
-- Data for Name: foo517; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo517 (i) FROM stdin;
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
-- Name: foo518; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo518 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo518 OWNER TO pivotal;

--
-- Data for Name: foo518; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo518 (i) FROM stdin;
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
-- Name: foo519; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo519 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo519 OWNER TO pivotal;

--
-- Data for Name: foo519; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo519 (i) FROM stdin;
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
-- Name: foo52; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo52 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo52 OWNER TO pivotal;

--
-- Data for Name: foo52; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo52 (i) FROM stdin;
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
-- Name: foo520; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo520 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo520 OWNER TO pivotal;

--
-- Data for Name: foo520; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo520 (i) FROM stdin;
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
-- Name: foo521; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo521 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo521 OWNER TO pivotal;

--
-- Data for Name: foo521; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo521 (i) FROM stdin;
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
-- Name: foo522; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo522 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo522 OWNER TO pivotal;

--
-- Data for Name: foo522; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo522 (i) FROM stdin;
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
-- Name: foo523; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo523 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo523 OWNER TO pivotal;

--
-- Data for Name: foo523; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo523 (i) FROM stdin;
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
-- Name: foo524; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo524 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo524 OWNER TO pivotal;

--
-- Data for Name: foo524; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo524 (i) FROM stdin;
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
-- Name: foo525; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo525 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo525 OWNER TO pivotal;

--
-- Data for Name: foo525; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo525 (i) FROM stdin;
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
-- Name: foo526; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo526 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo526 OWNER TO pivotal;

--
-- Data for Name: foo526; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo526 (i) FROM stdin;
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
-- Name: foo527; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo527 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo527 OWNER TO pivotal;

--
-- Data for Name: foo527; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo527 (i) FROM stdin;
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
-- Name: foo528; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo528 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo528 OWNER TO pivotal;

--
-- Data for Name: foo528; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo528 (i) FROM stdin;
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
-- Name: foo529; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo529 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo529 OWNER TO pivotal;

--
-- Data for Name: foo529; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo529 (i) FROM stdin;
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
-- Name: foo53; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo53 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo53 OWNER TO pivotal;

--
-- Data for Name: foo53; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo53 (i) FROM stdin;
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
-- Name: foo530; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo530 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo530 OWNER TO pivotal;

--
-- Data for Name: foo530; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo530 (i) FROM stdin;
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
-- Name: foo531; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo531 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo531 OWNER TO pivotal;

--
-- Data for Name: foo531; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo531 (i) FROM stdin;
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
-- Name: foo532; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo532 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo532 OWNER TO pivotal;

--
-- Data for Name: foo532; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo532 (i) FROM stdin;
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
-- Name: foo533; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo533 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo533 OWNER TO pivotal;

--
-- Data for Name: foo533; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo533 (i) FROM stdin;
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
-- Name: foo534; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo534 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo534 OWNER TO pivotal;

--
-- Data for Name: foo534; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo534 (i) FROM stdin;
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
-- Name: foo535; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo535 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo535 OWNER TO pivotal;

--
-- Data for Name: foo535; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo535 (i) FROM stdin;
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
-- Name: foo536; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo536 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo536 OWNER TO pivotal;

--
-- Data for Name: foo536; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo536 (i) FROM stdin;
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
-- Name: foo537; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo537 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo537 OWNER TO pivotal;

--
-- Data for Name: foo537; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo537 (i) FROM stdin;
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
-- Name: foo538; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo538 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo538 OWNER TO pivotal;

--
-- Data for Name: foo538; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo538 (i) FROM stdin;
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
-- Name: foo539; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo539 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo539 OWNER TO pivotal;

--
-- Data for Name: foo539; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo539 (i) FROM stdin;
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
-- Name: foo54; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo54 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo54 OWNER TO pivotal;

--
-- Data for Name: foo54; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo54 (i) FROM stdin;
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
-- Name: foo540; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo540 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo540 OWNER TO pivotal;

--
-- Data for Name: foo540; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo540 (i) FROM stdin;
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
-- Name: foo541; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo541 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo541 OWNER TO pivotal;

--
-- Data for Name: foo541; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo541 (i) FROM stdin;
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
-- Name: foo542; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo542 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo542 OWNER TO pivotal;

--
-- Data for Name: foo542; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo542 (i) FROM stdin;
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
-- Name: foo543; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo543 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo543 OWNER TO pivotal;

--
-- Data for Name: foo543; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo543 (i) FROM stdin;
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
-- Name: foo544; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo544 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo544 OWNER TO pivotal;

--
-- Data for Name: foo544; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo544 (i) FROM stdin;
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
-- Name: foo545; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo545 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo545 OWNER TO pivotal;

--
-- Data for Name: foo545; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo545 (i) FROM stdin;
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
-- Name: foo546; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo546 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo546 OWNER TO pivotal;

--
-- Data for Name: foo546; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo546 (i) FROM stdin;
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
-- Name: foo547; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo547 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo547 OWNER TO pivotal;

--
-- Data for Name: foo547; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo547 (i) FROM stdin;
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
-- Name: foo548; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo548 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo548 OWNER TO pivotal;

--
-- Data for Name: foo548; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo548 (i) FROM stdin;
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
-- Name: foo549; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo549 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo549 OWNER TO pivotal;

--
-- Data for Name: foo549; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo549 (i) FROM stdin;
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
-- Name: foo55; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo55 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo55 OWNER TO pivotal;

--
-- Data for Name: foo55; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo55 (i) FROM stdin;
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
-- Name: foo550; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo550 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo550 OWNER TO pivotal;

--
-- Data for Name: foo550; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo550 (i) FROM stdin;
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
-- Name: foo551; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo551 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo551 OWNER TO pivotal;

--
-- Data for Name: foo551; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo551 (i) FROM stdin;
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
-- Name: foo552; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo552 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo552 OWNER TO pivotal;

--
-- Data for Name: foo552; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo552 (i) FROM stdin;
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
-- Name: foo553; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo553 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo553 OWNER TO pivotal;

--
-- Data for Name: foo553; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo553 (i) FROM stdin;
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
-- Name: foo554; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo554 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo554 OWNER TO pivotal;

--
-- Data for Name: foo554; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo554 (i) FROM stdin;
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
-- Name: foo555; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo555 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo555 OWNER TO pivotal;

--
-- Data for Name: foo555; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo555 (i) FROM stdin;
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
-- Name: foo556; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo556 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo556 OWNER TO pivotal;

--
-- Data for Name: foo556; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo556 (i) FROM stdin;
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
-- Name: foo557; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo557 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo557 OWNER TO pivotal;

--
-- Data for Name: foo557; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo557 (i) FROM stdin;
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
-- Name: foo558; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo558 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo558 OWNER TO pivotal;

--
-- Data for Name: foo558; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo558 (i) FROM stdin;
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
-- Name: foo559; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo559 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo559 OWNER TO pivotal;

--
-- Data for Name: foo559; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo559 (i) FROM stdin;
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
-- Name: foo56; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo56 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo56 OWNER TO pivotal;

--
-- Data for Name: foo56; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo56 (i) FROM stdin;
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
-- Name: foo560; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo560 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo560 OWNER TO pivotal;

--
-- Data for Name: foo560; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo560 (i) FROM stdin;
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
-- Name: foo561; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo561 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo561 OWNER TO pivotal;

--
-- Data for Name: foo561; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo561 (i) FROM stdin;
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
-- Name: foo562; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo562 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo562 OWNER TO pivotal;

--
-- Data for Name: foo562; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo562 (i) FROM stdin;
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
-- Name: foo563; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo563 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo563 OWNER TO pivotal;

--
-- Data for Name: foo563; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo563 (i) FROM stdin;
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
-- Name: foo564; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo564 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo564 OWNER TO pivotal;

--
-- Data for Name: foo564; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo564 (i) FROM stdin;
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
-- Name: foo565; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo565 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo565 OWNER TO pivotal;

--
-- Data for Name: foo565; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo565 (i) FROM stdin;
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
-- Name: foo566; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo566 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo566 OWNER TO pivotal;

--
-- Data for Name: foo566; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo566 (i) FROM stdin;
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
-- Name: foo567; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo567 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo567 OWNER TO pivotal;

--
-- Data for Name: foo567; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo567 (i) FROM stdin;
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
-- Name: foo568; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo568 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo568 OWNER TO pivotal;

--
-- Data for Name: foo568; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo568 (i) FROM stdin;
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
-- Name: foo569; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo569 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo569 OWNER TO pivotal;

--
-- Data for Name: foo569; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo569 (i) FROM stdin;
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
-- Name: foo57; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo57 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo57 OWNER TO pivotal;

--
-- Data for Name: foo57; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo57 (i) FROM stdin;
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
-- Name: foo570; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo570 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo570 OWNER TO pivotal;

--
-- Data for Name: foo570; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo570 (i) FROM stdin;
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
-- Name: foo571; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo571 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo571 OWNER TO pivotal;

--
-- Data for Name: foo571; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo571 (i) FROM stdin;
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
-- Name: foo572; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo572 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo572 OWNER TO pivotal;

--
-- Data for Name: foo572; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo572 (i) FROM stdin;
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
-- Name: foo573; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo573 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo573 OWNER TO pivotal;

--
-- Data for Name: foo573; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo573 (i) FROM stdin;
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
-- Name: foo574; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo574 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo574 OWNER TO pivotal;

--
-- Data for Name: foo574; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo574 (i) FROM stdin;
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
-- Name: foo575; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo575 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo575 OWNER TO pivotal;

--
-- Data for Name: foo575; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo575 (i) FROM stdin;
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
-- Name: foo576; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo576 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo576 OWNER TO pivotal;

--
-- Data for Name: foo576; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo576 (i) FROM stdin;
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
-- Name: foo577; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo577 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo577 OWNER TO pivotal;

--
-- Data for Name: foo577; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo577 (i) FROM stdin;
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
-- Name: foo578; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo578 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo578 OWNER TO pivotal;

--
-- Data for Name: foo578; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo578 (i) FROM stdin;
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
-- Name: foo579; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo579 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo579 OWNER TO pivotal;

--
-- Data for Name: foo579; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo579 (i) FROM stdin;
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
-- Name: foo58; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo58 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo58 OWNER TO pivotal;

--
-- Data for Name: foo58; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo58 (i) FROM stdin;
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
-- Name: foo580; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo580 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo580 OWNER TO pivotal;

--
-- Data for Name: foo580; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo580 (i) FROM stdin;
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
-- Name: foo581; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo581 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo581 OWNER TO pivotal;

--
-- Data for Name: foo581; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo581 (i) FROM stdin;
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
-- Name: foo582; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo582 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo582 OWNER TO pivotal;

--
-- Data for Name: foo582; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo582 (i) FROM stdin;
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
-- Name: foo583; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo583 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo583 OWNER TO pivotal;

--
-- Data for Name: foo583; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo583 (i) FROM stdin;
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
-- Name: foo584; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo584 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo584 OWNER TO pivotal;

--
-- Data for Name: foo584; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo584 (i) FROM stdin;
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
-- Name: foo585; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo585 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo585 OWNER TO pivotal;

--
-- Data for Name: foo585; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo585 (i) FROM stdin;
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
-- Name: foo586; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo586 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo586 OWNER TO pivotal;

--
-- Data for Name: foo586; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo586 (i) FROM stdin;
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
-- Name: foo587; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo587 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo587 OWNER TO pivotal;

--
-- Data for Name: foo587; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo587 (i) FROM stdin;
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
-- Name: foo588; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo588 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo588 OWNER TO pivotal;

--
-- Data for Name: foo588; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo588 (i) FROM stdin;
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
-- Name: foo589; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo589 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo589 OWNER TO pivotal;

--
-- Data for Name: foo589; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo589 (i) FROM stdin;
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
-- Name: foo59; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo59 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo59 OWNER TO pivotal;

--
-- Data for Name: foo59; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo59 (i) FROM stdin;
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
-- Name: foo590; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo590 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo590 OWNER TO pivotal;

--
-- Data for Name: foo590; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo590 (i) FROM stdin;
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
-- Name: foo591; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo591 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo591 OWNER TO pivotal;

--
-- Data for Name: foo591; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo591 (i) FROM stdin;
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
-- Name: foo592; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo592 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo592 OWNER TO pivotal;

--
-- Data for Name: foo592; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo592 (i) FROM stdin;
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
-- Name: foo593; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo593 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo593 OWNER TO pivotal;

--
-- Data for Name: foo593; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo593 (i) FROM stdin;
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
-- Name: foo594; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo594 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo594 OWNER TO pivotal;

--
-- Data for Name: foo594; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo594 (i) FROM stdin;
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
-- Name: foo595; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo595 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo595 OWNER TO pivotal;

--
-- Data for Name: foo595; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo595 (i) FROM stdin;
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
-- Name: foo596; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo596 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo596 OWNER TO pivotal;

--
-- Data for Name: foo596; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo596 (i) FROM stdin;
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
-- Name: foo597; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo597 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo597 OWNER TO pivotal;

--
-- Data for Name: foo597; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo597 (i) FROM stdin;
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
-- Name: foo598; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo598 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo598 OWNER TO pivotal;

--
-- Data for Name: foo598; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo598 (i) FROM stdin;
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
-- Name: foo599; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo599 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo599 OWNER TO pivotal;

--
-- Data for Name: foo599; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo599 (i) FROM stdin;
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
-- Name: foo6; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo6 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo6 OWNER TO pivotal;

--
-- Data for Name: foo6; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo6 (i) FROM stdin;
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
-- Name: foo60; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo60 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo60 OWNER TO pivotal;

--
-- Data for Name: foo60; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo60 (i) FROM stdin;
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
-- Name: foo600; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo600 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo600 OWNER TO pivotal;

--
-- Data for Name: foo600; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo600 (i) FROM stdin;
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
-- Name: foo601; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo601 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo601 OWNER TO pivotal;

--
-- Data for Name: foo601; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo601 (i) FROM stdin;
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
-- Name: foo602; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo602 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo602 OWNER TO pivotal;

--
-- Data for Name: foo602; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo602 (i) FROM stdin;
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
-- Name: foo603; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo603 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo603 OWNER TO pivotal;

--
-- Data for Name: foo603; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo603 (i) FROM stdin;
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
-- Name: foo604; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo604 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo604 OWNER TO pivotal;

--
-- Data for Name: foo604; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo604 (i) FROM stdin;
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
-- Name: foo605; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo605 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo605 OWNER TO pivotal;

--
-- Data for Name: foo605; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo605 (i) FROM stdin;
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
-- Name: foo606; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo606 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo606 OWNER TO pivotal;

--
-- Data for Name: foo606; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo606 (i) FROM stdin;
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
-- Name: foo607; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo607 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo607 OWNER TO pivotal;

--
-- Data for Name: foo607; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo607 (i) FROM stdin;
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
-- Name: foo608; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo608 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo608 OWNER TO pivotal;

--
-- Data for Name: foo608; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo608 (i) FROM stdin;
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
-- Name: foo609; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo609 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo609 OWNER TO pivotal;

--
-- Data for Name: foo609; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo609 (i) FROM stdin;
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
-- Name: foo61; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo61 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo61 OWNER TO pivotal;

--
-- Data for Name: foo61; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo61 (i) FROM stdin;
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
-- Name: foo610; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo610 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo610 OWNER TO pivotal;

--
-- Data for Name: foo610; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo610 (i) FROM stdin;
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
-- Name: foo611; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo611 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo611 OWNER TO pivotal;

--
-- Data for Name: foo611; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo611 (i) FROM stdin;
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
-- Name: foo612; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo612 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo612 OWNER TO pivotal;

--
-- Data for Name: foo612; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo612 (i) FROM stdin;
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
-- Name: foo613; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo613 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo613 OWNER TO pivotal;

--
-- Data for Name: foo613; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo613 (i) FROM stdin;
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
-- Name: foo614; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo614 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo614 OWNER TO pivotal;

--
-- Data for Name: foo614; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo614 (i) FROM stdin;
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
-- Name: foo615; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo615 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo615 OWNER TO pivotal;

--
-- Data for Name: foo615; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo615 (i) FROM stdin;
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
-- Name: foo616; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo616 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo616 OWNER TO pivotal;

--
-- Data for Name: foo616; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo616 (i) FROM stdin;
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
-- Name: foo617; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo617 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo617 OWNER TO pivotal;

--
-- Data for Name: foo617; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo617 (i) FROM stdin;
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
-- Name: foo618; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo618 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo618 OWNER TO pivotal;

--
-- Data for Name: foo618; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo618 (i) FROM stdin;
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
-- Name: foo619; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo619 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo619 OWNER TO pivotal;

--
-- Data for Name: foo619; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo619 (i) FROM stdin;
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
-- Name: foo62; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo62 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo62 OWNER TO pivotal;

--
-- Data for Name: foo62; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo62 (i) FROM stdin;
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
-- Name: foo620; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo620 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo620 OWNER TO pivotal;

--
-- Data for Name: foo620; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo620 (i) FROM stdin;
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
-- Name: foo621; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo621 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo621 OWNER TO pivotal;

--
-- Data for Name: foo621; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo621 (i) FROM stdin;
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
-- Name: foo622; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo622 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo622 OWNER TO pivotal;

--
-- Data for Name: foo622; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo622 (i) FROM stdin;
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
-- Name: foo623; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo623 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo623 OWNER TO pivotal;

--
-- Data for Name: foo623; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo623 (i) FROM stdin;
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
-- Name: foo624; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo624 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo624 OWNER TO pivotal;

--
-- Data for Name: foo624; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo624 (i) FROM stdin;
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
-- Name: foo625; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo625 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo625 OWNER TO pivotal;

--
-- Data for Name: foo625; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo625 (i) FROM stdin;
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
-- Name: foo626; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo626 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo626 OWNER TO pivotal;

--
-- Data for Name: foo626; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo626 (i) FROM stdin;
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
-- Name: foo627; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo627 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo627 OWNER TO pivotal;

--
-- Data for Name: foo627; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo627 (i) FROM stdin;
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
-- Name: foo628; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo628 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo628 OWNER TO pivotal;

--
-- Data for Name: foo628; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo628 (i) FROM stdin;
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
-- Name: foo629; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo629 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo629 OWNER TO pivotal;

--
-- Data for Name: foo629; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo629 (i) FROM stdin;
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
-- Name: foo63; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo63 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo63 OWNER TO pivotal;

--
-- Data for Name: foo63; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo63 (i) FROM stdin;
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
-- Name: foo630; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo630 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo630 OWNER TO pivotal;

--
-- Data for Name: foo630; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo630 (i) FROM stdin;
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
-- Name: foo631; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo631 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo631 OWNER TO pivotal;

--
-- Data for Name: foo631; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo631 (i) FROM stdin;
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
-- Name: foo632; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo632 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo632 OWNER TO pivotal;

--
-- Data for Name: foo632; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo632 (i) FROM stdin;
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
-- Name: foo633; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo633 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo633 OWNER TO pivotal;

--
-- Data for Name: foo633; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo633 (i) FROM stdin;
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
-- Name: foo634; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo634 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo634 OWNER TO pivotal;

--
-- Data for Name: foo634; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo634 (i) FROM stdin;
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
-- Name: foo635; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo635 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo635 OWNER TO pivotal;

--
-- Data for Name: foo635; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo635 (i) FROM stdin;
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
-- Name: foo636; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo636 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo636 OWNER TO pivotal;

--
-- Data for Name: foo636; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo636 (i) FROM stdin;
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
-- Name: foo637; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo637 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo637 OWNER TO pivotal;

--
-- Data for Name: foo637; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo637 (i) FROM stdin;
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
-- Name: foo638; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo638 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo638 OWNER TO pivotal;

--
-- Data for Name: foo638; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo638 (i) FROM stdin;
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
-- Name: foo639; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo639 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo639 OWNER TO pivotal;

--
-- Data for Name: foo639; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo639 (i) FROM stdin;
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
-- Name: foo64; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo64 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo64 OWNER TO pivotal;

--
-- Data for Name: foo64; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo64 (i) FROM stdin;
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
-- Name: foo640; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo640 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo640 OWNER TO pivotal;

--
-- Data for Name: foo640; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo640 (i) FROM stdin;
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
-- Name: foo641; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo641 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo641 OWNER TO pivotal;

--
-- Data for Name: foo641; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo641 (i) FROM stdin;
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
-- Name: foo642; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo642 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo642 OWNER TO pivotal;

--
-- Data for Name: foo642; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo642 (i) FROM stdin;
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
-- Name: foo643; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo643 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo643 OWNER TO pivotal;

--
-- Data for Name: foo643; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo643 (i) FROM stdin;
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
-- Name: foo644; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo644 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo644 OWNER TO pivotal;

--
-- Data for Name: foo644; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo644 (i) FROM stdin;
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
-- Name: foo645; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo645 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo645 OWNER TO pivotal;

--
-- Data for Name: foo645; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo645 (i) FROM stdin;
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
-- Name: foo646; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo646 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo646 OWNER TO pivotal;

--
-- Data for Name: foo646; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo646 (i) FROM stdin;
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
-- Name: foo647; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo647 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo647 OWNER TO pivotal;

--
-- Data for Name: foo647; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo647 (i) FROM stdin;
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
-- Name: foo648; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo648 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo648 OWNER TO pivotal;

--
-- Data for Name: foo648; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo648 (i) FROM stdin;
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
-- Name: foo649; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo649 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo649 OWNER TO pivotal;

--
-- Data for Name: foo649; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo649 (i) FROM stdin;
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
-- Name: foo65; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo65 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo65 OWNER TO pivotal;

--
-- Data for Name: foo65; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo65 (i) FROM stdin;
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
-- Name: foo650; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo650 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo650 OWNER TO pivotal;

--
-- Data for Name: foo650; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo650 (i) FROM stdin;
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
-- Name: foo651; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo651 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo651 OWNER TO pivotal;

--
-- Data for Name: foo651; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo651 (i) FROM stdin;
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
-- Name: foo652; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo652 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo652 OWNER TO pivotal;

--
-- Data for Name: foo652; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo652 (i) FROM stdin;
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
-- Name: foo653; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo653 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo653 OWNER TO pivotal;

--
-- Data for Name: foo653; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo653 (i) FROM stdin;
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
-- Name: foo654; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo654 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo654 OWNER TO pivotal;

--
-- Data for Name: foo654; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo654 (i) FROM stdin;
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
-- Name: foo655; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo655 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo655 OWNER TO pivotal;

--
-- Data for Name: foo655; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo655 (i) FROM stdin;
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
-- Name: foo656; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo656 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo656 OWNER TO pivotal;

--
-- Data for Name: foo656; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo656 (i) FROM stdin;
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
-- Name: foo657; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo657 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo657 OWNER TO pivotal;

--
-- Data for Name: foo657; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo657 (i) FROM stdin;
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
-- Name: foo658; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo658 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo658 OWNER TO pivotal;

--
-- Data for Name: foo658; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo658 (i) FROM stdin;
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
-- Name: foo659; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo659 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo659 OWNER TO pivotal;

--
-- Data for Name: foo659; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo659 (i) FROM stdin;
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
-- Name: foo66; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo66 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo66 OWNER TO pivotal;

--
-- Data for Name: foo66; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo66 (i) FROM stdin;
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
-- Name: foo660; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo660 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo660 OWNER TO pivotal;

--
-- Data for Name: foo660; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo660 (i) FROM stdin;
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
-- Name: foo661; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo661 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo661 OWNER TO pivotal;

--
-- Data for Name: foo661; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo661 (i) FROM stdin;
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
-- Name: foo662; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo662 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo662 OWNER TO pivotal;

--
-- Data for Name: foo662; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo662 (i) FROM stdin;
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
-- Name: foo663; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo663 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo663 OWNER TO pivotal;

--
-- Data for Name: foo663; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo663 (i) FROM stdin;
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
-- Name: foo664; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo664 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo664 OWNER TO pivotal;

--
-- Data for Name: foo664; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo664 (i) FROM stdin;
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
-- Name: foo665; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo665 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo665 OWNER TO pivotal;

--
-- Data for Name: foo665; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo665 (i) FROM stdin;
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
-- Name: foo666; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo666 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo666 OWNER TO pivotal;

--
-- Data for Name: foo666; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo666 (i) FROM stdin;
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
-- Name: foo667; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo667 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo667 OWNER TO pivotal;

--
-- Data for Name: foo667; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo667 (i) FROM stdin;
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
-- Name: foo668; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo668 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo668 OWNER TO pivotal;

--
-- Data for Name: foo668; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo668 (i) FROM stdin;
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
-- Name: foo669; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo669 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo669 OWNER TO pivotal;

--
-- Data for Name: foo669; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo669 (i) FROM stdin;
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
-- Name: foo67; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo67 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo67 OWNER TO pivotal;

--
-- Data for Name: foo67; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo67 (i) FROM stdin;
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
-- Name: foo670; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo670 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo670 OWNER TO pivotal;

--
-- Data for Name: foo670; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo670 (i) FROM stdin;
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
-- Name: foo671; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo671 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo671 OWNER TO pivotal;

--
-- Data for Name: foo671; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo671 (i) FROM stdin;
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
-- Name: foo672; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo672 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo672 OWNER TO pivotal;

--
-- Data for Name: foo672; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo672 (i) FROM stdin;
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
-- Name: foo673; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo673 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo673 OWNER TO pivotal;

--
-- Data for Name: foo673; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo673 (i) FROM stdin;
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
-- Name: foo674; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo674 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo674 OWNER TO pivotal;

--
-- Data for Name: foo674; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo674 (i) FROM stdin;
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
-- Name: foo675; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo675 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo675 OWNER TO pivotal;

--
-- Data for Name: foo675; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo675 (i) FROM stdin;
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
-- Name: foo676; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo676 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo676 OWNER TO pivotal;

--
-- Data for Name: foo676; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo676 (i) FROM stdin;
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
-- Name: foo677; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo677 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo677 OWNER TO pivotal;

--
-- Data for Name: foo677; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo677 (i) FROM stdin;
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
-- Name: foo678; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo678 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo678 OWNER TO pivotal;

--
-- Data for Name: foo678; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo678 (i) FROM stdin;
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
-- Name: foo679; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo679 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo679 OWNER TO pivotal;

--
-- Data for Name: foo679; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo679 (i) FROM stdin;
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
-- Name: foo68; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo68 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo68 OWNER TO pivotal;

--
-- Data for Name: foo68; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo68 (i) FROM stdin;
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
-- Name: foo680; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo680 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo680 OWNER TO pivotal;

--
-- Data for Name: foo680; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo680 (i) FROM stdin;
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
-- Name: foo681; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo681 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo681 OWNER TO pivotal;

--
-- Data for Name: foo681; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo681 (i) FROM stdin;
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
-- Name: foo682; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo682 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo682 OWNER TO pivotal;

--
-- Data for Name: foo682; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo682 (i) FROM stdin;
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
-- Name: foo683; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo683 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo683 OWNER TO pivotal;

--
-- Data for Name: foo683; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo683 (i) FROM stdin;
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
-- Name: foo684; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo684 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo684 OWNER TO pivotal;

--
-- Data for Name: foo684; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo684 (i) FROM stdin;
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
-- Name: foo685; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo685 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo685 OWNER TO pivotal;

--
-- Data for Name: foo685; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo685 (i) FROM stdin;
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
-- Name: foo686; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo686 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo686 OWNER TO pivotal;

--
-- Data for Name: foo686; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo686 (i) FROM stdin;
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
-- Name: foo687; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo687 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo687 OWNER TO pivotal;

--
-- Data for Name: foo687; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo687 (i) FROM stdin;
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
-- Name: foo688; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo688 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo688 OWNER TO pivotal;

--
-- Data for Name: foo688; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo688 (i) FROM stdin;
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
-- Name: foo689; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo689 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo689 OWNER TO pivotal;

--
-- Data for Name: foo689; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo689 (i) FROM stdin;
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
-- Name: foo69; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo69 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo69 OWNER TO pivotal;

--
-- Data for Name: foo69; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo69 (i) FROM stdin;
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
-- Name: foo690; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo690 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo690 OWNER TO pivotal;

--
-- Data for Name: foo690; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo690 (i) FROM stdin;
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
-- Name: foo691; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo691 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo691 OWNER TO pivotal;

--
-- Data for Name: foo691; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo691 (i) FROM stdin;
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
-- Name: foo692; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo692 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo692 OWNER TO pivotal;

--
-- Data for Name: foo692; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo692 (i) FROM stdin;
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
-- Name: foo693; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo693 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo693 OWNER TO pivotal;

--
-- Data for Name: foo693; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo693 (i) FROM stdin;
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
-- Name: foo694; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo694 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo694 OWNER TO pivotal;

--
-- Data for Name: foo694; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo694 (i) FROM stdin;
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
-- Name: foo695; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo695 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo695 OWNER TO pivotal;

--
-- Data for Name: foo695; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo695 (i) FROM stdin;
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
-- Name: foo696; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo696 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo696 OWNER TO pivotal;

--
-- Data for Name: foo696; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo696 (i) FROM stdin;
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
-- Name: foo697; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo697 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo697 OWNER TO pivotal;

--
-- Data for Name: foo697; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo697 (i) FROM stdin;
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
-- Name: foo698; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo698 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo698 OWNER TO pivotal;

--
-- Data for Name: foo698; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo698 (i) FROM stdin;
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
-- Name: foo699; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo699 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo699 OWNER TO pivotal;

--
-- Data for Name: foo699; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo699 (i) FROM stdin;
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
-- Name: foo7; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo7 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo7 OWNER TO pivotal;

--
-- Data for Name: foo7; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo7 (i) FROM stdin;
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
-- Name: foo70; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo70 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo70 OWNER TO pivotal;

--
-- Data for Name: foo70; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo70 (i) FROM stdin;
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
-- Name: foo700; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo700 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo700 OWNER TO pivotal;

--
-- Data for Name: foo700; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo700 (i) FROM stdin;
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
-- Name: foo701; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo701 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo701 OWNER TO pivotal;

--
-- Data for Name: foo701; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo701 (i) FROM stdin;
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
-- Name: foo702; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo702 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo702 OWNER TO pivotal;

--
-- Data for Name: foo702; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo702 (i) FROM stdin;
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
-- Name: foo703; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo703 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo703 OWNER TO pivotal;

--
-- Data for Name: foo703; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo703 (i) FROM stdin;
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
-- Name: foo704; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo704 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo704 OWNER TO pivotal;

--
-- Data for Name: foo704; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo704 (i) FROM stdin;
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
-- Name: foo705; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo705 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo705 OWNER TO pivotal;

--
-- Data for Name: foo705; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo705 (i) FROM stdin;
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
-- Name: foo706; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo706 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo706 OWNER TO pivotal;

--
-- Data for Name: foo706; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo706 (i) FROM stdin;
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
-- Name: foo707; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo707 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo707 OWNER TO pivotal;

--
-- Data for Name: foo707; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo707 (i) FROM stdin;
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
-- Name: foo708; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo708 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo708 OWNER TO pivotal;

--
-- Data for Name: foo708; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo708 (i) FROM stdin;
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
-- Name: foo709; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo709 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo709 OWNER TO pivotal;

--
-- Data for Name: foo709; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo709 (i) FROM stdin;
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
-- Name: foo71; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo71 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo71 OWNER TO pivotal;

--
-- Data for Name: foo71; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo71 (i) FROM stdin;
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
-- Name: foo710; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo710 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo710 OWNER TO pivotal;

--
-- Data for Name: foo710; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo710 (i) FROM stdin;
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
-- Name: foo711; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo711 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo711 OWNER TO pivotal;

--
-- Data for Name: foo711; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo711 (i) FROM stdin;
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
-- Name: foo712; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo712 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo712 OWNER TO pivotal;

--
-- Data for Name: foo712; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo712 (i) FROM stdin;
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
-- Name: foo713; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo713 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo713 OWNER TO pivotal;

--
-- Data for Name: foo713; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo713 (i) FROM stdin;
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
-- Name: foo714; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo714 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo714 OWNER TO pivotal;

--
-- Data for Name: foo714; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo714 (i) FROM stdin;
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
-- Name: foo715; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo715 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo715 OWNER TO pivotal;

--
-- Data for Name: foo715; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo715 (i) FROM stdin;
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
-- Name: foo716; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo716 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo716 OWNER TO pivotal;

--
-- Data for Name: foo716; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo716 (i) FROM stdin;
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
-- Name: foo717; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo717 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo717 OWNER TO pivotal;

--
-- Data for Name: foo717; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo717 (i) FROM stdin;
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
-- Name: foo718; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo718 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo718 OWNER TO pivotal;

--
-- Data for Name: foo718; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo718 (i) FROM stdin;
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
-- Name: foo719; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo719 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo719 OWNER TO pivotal;

--
-- Data for Name: foo719; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo719 (i) FROM stdin;
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
-- Name: foo72; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo72 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo72 OWNER TO pivotal;

--
-- Data for Name: foo72; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo72 (i) FROM stdin;
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
-- Name: foo720; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo720 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo720 OWNER TO pivotal;

--
-- Data for Name: foo720; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo720 (i) FROM stdin;
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
-- Name: foo721; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo721 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo721 OWNER TO pivotal;

--
-- Data for Name: foo721; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo721 (i) FROM stdin;
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
-- Name: foo722; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo722 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo722 OWNER TO pivotal;

--
-- Data for Name: foo722; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo722 (i) FROM stdin;
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
-- Name: foo723; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo723 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo723 OWNER TO pivotal;

--
-- Data for Name: foo723; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo723 (i) FROM stdin;
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
-- Name: foo724; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo724 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo724 OWNER TO pivotal;

--
-- Data for Name: foo724; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo724 (i) FROM stdin;
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
-- Name: foo725; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo725 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo725 OWNER TO pivotal;

--
-- Data for Name: foo725; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo725 (i) FROM stdin;
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
-- Name: foo726; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo726 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo726 OWNER TO pivotal;

--
-- Data for Name: foo726; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo726 (i) FROM stdin;
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
-- Name: foo727; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo727 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo727 OWNER TO pivotal;

--
-- Data for Name: foo727; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo727 (i) FROM stdin;
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
-- Name: foo728; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo728 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo728 OWNER TO pivotal;

--
-- Data for Name: foo728; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo728 (i) FROM stdin;
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
-- Name: foo729; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo729 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo729 OWNER TO pivotal;

--
-- Data for Name: foo729; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo729 (i) FROM stdin;
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
-- Name: foo73; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo73 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo73 OWNER TO pivotal;

--
-- Data for Name: foo73; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo73 (i) FROM stdin;
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
-- Name: foo730; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo730 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo730 OWNER TO pivotal;

--
-- Data for Name: foo730; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo730 (i) FROM stdin;
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
-- Name: foo731; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo731 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo731 OWNER TO pivotal;

--
-- Data for Name: foo731; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo731 (i) FROM stdin;
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
-- Name: foo732; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo732 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo732 OWNER TO pivotal;

--
-- Data for Name: foo732; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo732 (i) FROM stdin;
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
-- Name: foo733; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo733 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo733 OWNER TO pivotal;

--
-- Data for Name: foo733; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo733 (i) FROM stdin;
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
-- Name: foo734; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo734 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo734 OWNER TO pivotal;

--
-- Data for Name: foo734; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo734 (i) FROM stdin;
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
-- Name: foo735; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo735 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo735 OWNER TO pivotal;

--
-- Data for Name: foo735; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo735 (i) FROM stdin;
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
-- Name: foo736; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo736 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo736 OWNER TO pivotal;

--
-- Data for Name: foo736; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo736 (i) FROM stdin;
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
-- Name: foo737; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo737 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo737 OWNER TO pivotal;

--
-- Data for Name: foo737; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo737 (i) FROM stdin;
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
-- Name: foo738; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo738 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo738 OWNER TO pivotal;

--
-- Data for Name: foo738; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo738 (i) FROM stdin;
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
-- Name: foo739; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo739 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo739 OWNER TO pivotal;

--
-- Data for Name: foo739; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo739 (i) FROM stdin;
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
-- Name: foo74; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo74 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo74 OWNER TO pivotal;

--
-- Data for Name: foo74; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo74 (i) FROM stdin;
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
-- Name: foo740; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo740 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo740 OWNER TO pivotal;

--
-- Data for Name: foo740; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo740 (i) FROM stdin;
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
-- Name: foo741; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo741 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo741 OWNER TO pivotal;

--
-- Data for Name: foo741; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo741 (i) FROM stdin;
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
-- Name: foo742; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo742 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo742 OWNER TO pivotal;

--
-- Data for Name: foo742; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo742 (i) FROM stdin;
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
-- Name: foo743; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo743 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo743 OWNER TO pivotal;

--
-- Data for Name: foo743; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo743 (i) FROM stdin;
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
-- Name: foo744; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo744 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo744 OWNER TO pivotal;

--
-- Data for Name: foo744; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo744 (i) FROM stdin;
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
-- Name: foo745; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo745 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo745 OWNER TO pivotal;

--
-- Data for Name: foo745; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo745 (i) FROM stdin;
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
-- Name: foo746; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo746 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo746 OWNER TO pivotal;

--
-- Data for Name: foo746; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo746 (i) FROM stdin;
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
-- Name: foo747; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo747 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo747 OWNER TO pivotal;

--
-- Data for Name: foo747; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo747 (i) FROM stdin;
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
-- Name: foo748; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo748 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo748 OWNER TO pivotal;

--
-- Data for Name: foo748; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo748 (i) FROM stdin;
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
-- Name: foo749; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo749 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo749 OWNER TO pivotal;

--
-- Data for Name: foo749; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo749 (i) FROM stdin;
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
-- Name: foo75; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo75 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo75 OWNER TO pivotal;

--
-- Data for Name: foo75; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo75 (i) FROM stdin;
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
-- Name: foo750; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo750 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo750 OWNER TO pivotal;

--
-- Data for Name: foo750; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo750 (i) FROM stdin;
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
-- Name: foo751; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo751 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo751 OWNER TO pivotal;

--
-- Data for Name: foo751; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo751 (i) FROM stdin;
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
-- Name: foo752; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo752 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo752 OWNER TO pivotal;

--
-- Data for Name: foo752; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo752 (i) FROM stdin;
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
-- Name: foo753; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo753 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo753 OWNER TO pivotal;

--
-- Data for Name: foo753; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo753 (i) FROM stdin;
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
-- Name: foo754; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo754 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo754 OWNER TO pivotal;

--
-- Data for Name: foo754; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo754 (i) FROM stdin;
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
-- Name: foo755; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo755 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo755 OWNER TO pivotal;

--
-- Data for Name: foo755; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo755 (i) FROM stdin;
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
-- Name: foo756; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo756 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo756 OWNER TO pivotal;

--
-- Data for Name: foo756; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo756 (i) FROM stdin;
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
-- Name: foo757; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo757 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo757 OWNER TO pivotal;

--
-- Data for Name: foo757; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo757 (i) FROM stdin;
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
-- Name: foo758; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo758 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo758 OWNER TO pivotal;

--
-- Data for Name: foo758; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo758 (i) FROM stdin;
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
-- Name: foo759; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo759 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo759 OWNER TO pivotal;

--
-- Data for Name: foo759; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo759 (i) FROM stdin;
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
-- Name: foo76; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo76 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo76 OWNER TO pivotal;

--
-- Data for Name: foo76; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo76 (i) FROM stdin;
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
-- Name: foo760; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo760 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo760 OWNER TO pivotal;

--
-- Data for Name: foo760; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo760 (i) FROM stdin;
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
-- Name: foo761; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo761 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo761 OWNER TO pivotal;

--
-- Data for Name: foo761; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo761 (i) FROM stdin;
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
-- Name: foo762; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo762 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo762 OWNER TO pivotal;

--
-- Data for Name: foo762; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo762 (i) FROM stdin;
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
-- Name: foo763; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo763 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo763 OWNER TO pivotal;

--
-- Data for Name: foo763; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo763 (i) FROM stdin;
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
-- Name: foo764; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo764 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo764 OWNER TO pivotal;

--
-- Data for Name: foo764; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo764 (i) FROM stdin;
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
-- Name: foo765; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo765 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo765 OWNER TO pivotal;

--
-- Data for Name: foo765; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo765 (i) FROM stdin;
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
-- Name: foo766; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo766 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo766 OWNER TO pivotal;

--
-- Data for Name: foo766; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo766 (i) FROM stdin;
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
-- Name: foo767; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo767 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo767 OWNER TO pivotal;

--
-- Data for Name: foo767; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo767 (i) FROM stdin;
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
-- Name: foo768; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo768 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo768 OWNER TO pivotal;

--
-- Data for Name: foo768; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo768 (i) FROM stdin;
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
-- Name: foo769; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo769 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo769 OWNER TO pivotal;

--
-- Data for Name: foo769; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo769 (i) FROM stdin;
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
-- Name: foo77; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo77 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo77 OWNER TO pivotal;

--
-- Data for Name: foo77; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo77 (i) FROM stdin;
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
-- Name: foo770; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo770 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo770 OWNER TO pivotal;

--
-- Data for Name: foo770; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo770 (i) FROM stdin;
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
-- Name: foo771; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo771 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo771 OWNER TO pivotal;

--
-- Data for Name: foo771; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo771 (i) FROM stdin;
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
-- Name: foo772; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo772 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo772 OWNER TO pivotal;

--
-- Data for Name: foo772; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo772 (i) FROM stdin;
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
-- Name: foo773; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo773 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo773 OWNER TO pivotal;

--
-- Data for Name: foo773; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo773 (i) FROM stdin;
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
-- Name: foo774; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo774 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo774 OWNER TO pivotal;

--
-- Data for Name: foo774; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo774 (i) FROM stdin;
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
-- Name: foo775; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo775 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo775 OWNER TO pivotal;

--
-- Data for Name: foo775; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo775 (i) FROM stdin;
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
-- Name: foo776; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo776 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo776 OWNER TO pivotal;

--
-- Data for Name: foo776; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo776 (i) FROM stdin;
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
-- Name: foo777; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo777 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo777 OWNER TO pivotal;

--
-- Data for Name: foo777; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo777 (i) FROM stdin;
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
-- Name: foo778; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo778 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo778 OWNER TO pivotal;

--
-- Data for Name: foo778; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo778 (i) FROM stdin;
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
-- Name: foo779; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo779 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo779 OWNER TO pivotal;

--
-- Data for Name: foo779; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo779 (i) FROM stdin;
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
-- Name: foo78; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo78 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo78 OWNER TO pivotal;

--
-- Data for Name: foo78; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo78 (i) FROM stdin;
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
-- Name: foo780; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo780 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo780 OWNER TO pivotal;

--
-- Data for Name: foo780; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo780 (i) FROM stdin;
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
-- Name: foo781; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo781 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo781 OWNER TO pivotal;

--
-- Data for Name: foo781; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo781 (i) FROM stdin;
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
-- Name: foo782; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo782 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo782 OWNER TO pivotal;

--
-- Data for Name: foo782; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo782 (i) FROM stdin;
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
-- Name: foo783; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo783 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo783 OWNER TO pivotal;

--
-- Data for Name: foo783; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo783 (i) FROM stdin;
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
-- Name: foo784; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo784 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo784 OWNER TO pivotal;

--
-- Data for Name: foo784; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo784 (i) FROM stdin;
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
-- Name: foo785; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo785 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo785 OWNER TO pivotal;

--
-- Data for Name: foo785; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo785 (i) FROM stdin;
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
-- Name: foo786; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo786 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo786 OWNER TO pivotal;

--
-- Data for Name: foo786; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo786 (i) FROM stdin;
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
-- Name: foo787; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo787 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo787 OWNER TO pivotal;

--
-- Data for Name: foo787; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo787 (i) FROM stdin;
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
-- Name: foo788; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo788 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo788 OWNER TO pivotal;

--
-- Data for Name: foo788; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo788 (i) FROM stdin;
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
-- Name: foo789; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo789 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo789 OWNER TO pivotal;

--
-- Data for Name: foo789; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo789 (i) FROM stdin;
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
-- Name: foo79; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo79 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo79 OWNER TO pivotal;

--
-- Data for Name: foo79; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo79 (i) FROM stdin;
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
-- Name: foo790; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo790 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo790 OWNER TO pivotal;

--
-- Data for Name: foo790; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo790 (i) FROM stdin;
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
-- Name: foo791; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo791 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo791 OWNER TO pivotal;

--
-- Data for Name: foo791; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo791 (i) FROM stdin;
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
-- Name: foo792; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo792 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo792 OWNER TO pivotal;

--
-- Data for Name: foo792; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo792 (i) FROM stdin;
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
-- Name: foo793; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo793 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo793 OWNER TO pivotal;

--
-- Data for Name: foo793; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo793 (i) FROM stdin;
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
-- Name: foo794; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo794 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo794 OWNER TO pivotal;

--
-- Data for Name: foo794; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo794 (i) FROM stdin;
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
-- Name: foo795; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo795 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo795 OWNER TO pivotal;

--
-- Data for Name: foo795; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo795 (i) FROM stdin;
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
-- Name: foo796; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo796 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo796 OWNER TO pivotal;

--
-- Data for Name: foo796; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo796 (i) FROM stdin;
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
-- Name: foo797; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo797 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo797 OWNER TO pivotal;

--
-- Data for Name: foo797; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo797 (i) FROM stdin;
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
-- Name: foo798; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo798 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo798 OWNER TO pivotal;

--
-- Data for Name: foo798; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo798 (i) FROM stdin;
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
-- Name: foo799; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo799 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo799 OWNER TO pivotal;

--
-- Data for Name: foo799; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo799 (i) FROM stdin;
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
-- Name: foo8; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo8 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo8 OWNER TO pivotal;

--
-- Data for Name: foo8; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo8 (i) FROM stdin;
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
-- Name: foo80; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo80 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo80 OWNER TO pivotal;

--
-- Data for Name: foo80; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo80 (i) FROM stdin;
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
-- Name: foo800; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo800 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo800 OWNER TO pivotal;

--
-- Data for Name: foo800; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo800 (i) FROM stdin;
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
-- Name: foo801; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo801 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo801 OWNER TO pivotal;

--
-- Data for Name: foo801; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo801 (i) FROM stdin;
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
-- Name: foo802; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo802 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo802 OWNER TO pivotal;

--
-- Data for Name: foo802; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo802 (i) FROM stdin;
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
-- Name: foo803; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo803 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo803 OWNER TO pivotal;

--
-- Data for Name: foo803; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo803 (i) FROM stdin;
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
-- Name: foo804; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo804 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo804 OWNER TO pivotal;

--
-- Data for Name: foo804; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo804 (i) FROM stdin;
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
-- Name: foo805; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo805 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo805 OWNER TO pivotal;

--
-- Data for Name: foo805; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo805 (i) FROM stdin;
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
-- Name: foo806; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo806 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo806 OWNER TO pivotal;

--
-- Data for Name: foo806; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo806 (i) FROM stdin;
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
-- Name: foo807; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo807 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo807 OWNER TO pivotal;

--
-- Data for Name: foo807; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo807 (i) FROM stdin;
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
-- Name: foo808; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo808 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo808 OWNER TO pivotal;

--
-- Data for Name: foo808; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo808 (i) FROM stdin;
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
-- Name: foo809; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo809 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo809 OWNER TO pivotal;

--
-- Data for Name: foo809; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo809 (i) FROM stdin;
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
-- Name: foo81; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo81 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo81 OWNER TO pivotal;

--
-- Data for Name: foo81; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo81 (i) FROM stdin;
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
-- Name: foo810; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo810 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo810 OWNER TO pivotal;

--
-- Data for Name: foo810; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo810 (i) FROM stdin;
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
-- Name: foo811; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo811 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo811 OWNER TO pivotal;

--
-- Data for Name: foo811; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo811 (i) FROM stdin;
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
-- Name: foo812; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo812 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo812 OWNER TO pivotal;

--
-- Data for Name: foo812; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo812 (i) FROM stdin;
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
-- Name: foo813; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo813 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo813 OWNER TO pivotal;

--
-- Data for Name: foo813; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo813 (i) FROM stdin;
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
-- Name: foo814; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo814 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo814 OWNER TO pivotal;

--
-- Data for Name: foo814; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo814 (i) FROM stdin;
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
-- Name: foo815; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo815 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo815 OWNER TO pivotal;

--
-- Data for Name: foo815; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo815 (i) FROM stdin;
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
-- Name: foo816; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo816 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo816 OWNER TO pivotal;

--
-- Data for Name: foo816; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo816 (i) FROM stdin;
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
-- Name: foo817; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo817 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo817 OWNER TO pivotal;

--
-- Data for Name: foo817; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo817 (i) FROM stdin;
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
-- Name: foo818; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo818 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo818 OWNER TO pivotal;

--
-- Data for Name: foo818; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo818 (i) FROM stdin;
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
-- Name: foo819; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo819 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo819 OWNER TO pivotal;

--
-- Data for Name: foo819; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo819 (i) FROM stdin;
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
-- Name: foo82; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo82 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo82 OWNER TO pivotal;

--
-- Data for Name: foo82; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo82 (i) FROM stdin;
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
-- Name: foo820; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo820 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo820 OWNER TO pivotal;

--
-- Data for Name: foo820; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo820 (i) FROM stdin;
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
-- Name: foo821; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo821 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo821 OWNER TO pivotal;

--
-- Data for Name: foo821; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo821 (i) FROM stdin;
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
-- Name: foo822; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo822 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo822 OWNER TO pivotal;

--
-- Data for Name: foo822; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo822 (i) FROM stdin;
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
-- Name: foo823; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo823 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo823 OWNER TO pivotal;

--
-- Data for Name: foo823; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo823 (i) FROM stdin;
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
-- Name: foo824; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo824 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo824 OWNER TO pivotal;

--
-- Data for Name: foo824; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo824 (i) FROM stdin;
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
-- Name: foo825; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo825 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo825 OWNER TO pivotal;

--
-- Data for Name: foo825; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo825 (i) FROM stdin;
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
-- Name: foo826; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo826 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo826 OWNER TO pivotal;

--
-- Data for Name: foo826; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo826 (i) FROM stdin;
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
-- Name: foo827; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo827 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo827 OWNER TO pivotal;

--
-- Data for Name: foo827; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo827 (i) FROM stdin;
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
-- Name: foo828; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo828 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo828 OWNER TO pivotal;

--
-- Data for Name: foo828; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo828 (i) FROM stdin;
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
-- Name: foo829; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo829 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo829 OWNER TO pivotal;

--
-- Data for Name: foo829; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo829 (i) FROM stdin;
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
-- Name: foo83; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo83 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo83 OWNER TO pivotal;

--
-- Data for Name: foo83; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo83 (i) FROM stdin;
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
-- Name: foo830; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo830 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo830 OWNER TO pivotal;

--
-- Data for Name: foo830; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo830 (i) FROM stdin;
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
-- Name: foo831; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo831 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo831 OWNER TO pivotal;

--
-- Data for Name: foo831; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo831 (i) FROM stdin;
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
-- Name: foo832; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo832 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo832 OWNER TO pivotal;

--
-- Data for Name: foo832; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo832 (i) FROM stdin;
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
-- Name: foo833; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo833 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo833 OWNER TO pivotal;

--
-- Data for Name: foo833; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo833 (i) FROM stdin;
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
-- Name: foo834; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo834 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo834 OWNER TO pivotal;

--
-- Data for Name: foo834; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo834 (i) FROM stdin;
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
-- Name: foo835; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo835 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo835 OWNER TO pivotal;

--
-- Data for Name: foo835; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo835 (i) FROM stdin;
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
-- Name: foo836; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo836 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo836 OWNER TO pivotal;

--
-- Data for Name: foo836; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo836 (i) FROM stdin;
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
-- Name: foo837; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo837 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo837 OWNER TO pivotal;

--
-- Data for Name: foo837; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo837 (i) FROM stdin;
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
-- Name: foo838; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo838 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo838 OWNER TO pivotal;

--
-- Data for Name: foo838; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo838 (i) FROM stdin;
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
-- Name: foo839; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo839 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo839 OWNER TO pivotal;

--
-- Data for Name: foo839; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo839 (i) FROM stdin;
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
-- Name: foo84; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo84 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo84 OWNER TO pivotal;

--
-- Data for Name: foo84; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo84 (i) FROM stdin;
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
-- Name: foo840; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo840 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo840 OWNER TO pivotal;

--
-- Data for Name: foo840; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo840 (i) FROM stdin;
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
-- Name: foo841; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo841 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo841 OWNER TO pivotal;

--
-- Data for Name: foo841; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo841 (i) FROM stdin;
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
-- Name: foo842; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo842 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo842 OWNER TO pivotal;

--
-- Data for Name: foo842; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo842 (i) FROM stdin;
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
-- Name: foo843; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo843 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo843 OWNER TO pivotal;

--
-- Data for Name: foo843; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo843 (i) FROM stdin;
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
-- Name: foo844; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo844 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo844 OWNER TO pivotal;

--
-- Data for Name: foo844; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo844 (i) FROM stdin;
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
-- Name: foo845; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo845 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo845 OWNER TO pivotal;

--
-- Data for Name: foo845; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo845 (i) FROM stdin;
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
-- Name: foo846; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo846 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo846 OWNER TO pivotal;

--
-- Data for Name: foo846; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo846 (i) FROM stdin;
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
-- Name: foo847; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo847 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo847 OWNER TO pivotal;

--
-- Data for Name: foo847; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo847 (i) FROM stdin;
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
-- Name: foo848; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo848 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo848 OWNER TO pivotal;

--
-- Data for Name: foo848; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo848 (i) FROM stdin;
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
-- Name: foo849; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo849 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo849 OWNER TO pivotal;

--
-- Data for Name: foo849; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo849 (i) FROM stdin;
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
-- Name: foo85; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo85 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo85 OWNER TO pivotal;

--
-- Data for Name: foo85; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo85 (i) FROM stdin;
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
-- Name: foo850; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo850 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo850 OWNER TO pivotal;

--
-- Data for Name: foo850; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo850 (i) FROM stdin;
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
-- Name: foo851; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo851 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo851 OWNER TO pivotal;

--
-- Data for Name: foo851; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo851 (i) FROM stdin;
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
-- Name: foo852; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo852 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo852 OWNER TO pivotal;

--
-- Data for Name: foo852; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo852 (i) FROM stdin;
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
-- Name: foo853; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo853 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo853 OWNER TO pivotal;

--
-- Data for Name: foo853; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo853 (i) FROM stdin;
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
-- Name: foo854; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo854 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo854 OWNER TO pivotal;

--
-- Data for Name: foo854; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo854 (i) FROM stdin;
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
-- Name: foo855; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo855 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo855 OWNER TO pivotal;

--
-- Data for Name: foo855; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo855 (i) FROM stdin;
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
-- Name: foo856; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo856 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo856 OWNER TO pivotal;

--
-- Data for Name: foo856; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo856 (i) FROM stdin;
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
-- Name: foo857; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo857 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo857 OWNER TO pivotal;

--
-- Data for Name: foo857; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo857 (i) FROM stdin;
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
-- Name: foo858; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo858 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo858 OWNER TO pivotal;

--
-- Data for Name: foo858; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo858 (i) FROM stdin;
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
-- Name: foo859; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo859 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo859 OWNER TO pivotal;

--
-- Data for Name: foo859; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo859 (i) FROM stdin;
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
-- Name: foo86; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo86 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo86 OWNER TO pivotal;

--
-- Data for Name: foo86; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo86 (i) FROM stdin;
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
-- Name: foo860; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo860 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo860 OWNER TO pivotal;

--
-- Data for Name: foo860; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo860 (i) FROM stdin;
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
-- Name: foo861; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo861 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo861 OWNER TO pivotal;

--
-- Data for Name: foo861; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo861 (i) FROM stdin;
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
-- Name: foo862; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo862 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo862 OWNER TO pivotal;

--
-- Data for Name: foo862; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo862 (i) FROM stdin;
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
-- Name: foo863; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo863 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo863 OWNER TO pivotal;

--
-- Data for Name: foo863; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo863 (i) FROM stdin;
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
-- Name: foo864; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo864 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo864 OWNER TO pivotal;

--
-- Data for Name: foo864; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo864 (i) FROM stdin;
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
-- Name: foo865; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo865 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo865 OWNER TO pivotal;

--
-- Data for Name: foo865; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo865 (i) FROM stdin;
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
-- Name: foo866; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo866 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo866 OWNER TO pivotal;

--
-- Data for Name: foo866; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo866 (i) FROM stdin;
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
-- Name: foo867; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo867 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo867 OWNER TO pivotal;

--
-- Data for Name: foo867; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo867 (i) FROM stdin;
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
-- Name: foo868; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo868 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo868 OWNER TO pivotal;

--
-- Data for Name: foo868; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo868 (i) FROM stdin;
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
-- Name: foo869; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo869 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo869 OWNER TO pivotal;

--
-- Data for Name: foo869; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo869 (i) FROM stdin;
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
-- Name: foo87; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo87 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo87 OWNER TO pivotal;

--
-- Data for Name: foo87; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo87 (i) FROM stdin;
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
-- Name: foo870; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo870 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo870 OWNER TO pivotal;

--
-- Data for Name: foo870; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo870 (i) FROM stdin;
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
-- Name: foo871; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo871 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo871 OWNER TO pivotal;

--
-- Data for Name: foo871; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo871 (i) FROM stdin;
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
-- Name: foo872; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo872 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo872 OWNER TO pivotal;

--
-- Data for Name: foo872; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo872 (i) FROM stdin;
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
-- Name: foo873; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo873 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo873 OWNER TO pivotal;

--
-- Data for Name: foo873; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo873 (i) FROM stdin;
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
-- Name: foo874; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo874 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo874 OWNER TO pivotal;

--
-- Data for Name: foo874; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo874 (i) FROM stdin;
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
-- Name: foo875; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo875 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo875 OWNER TO pivotal;

--
-- Data for Name: foo875; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo875 (i) FROM stdin;
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
-- Name: foo876; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo876 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo876 OWNER TO pivotal;

--
-- Data for Name: foo876; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo876 (i) FROM stdin;
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
-- Name: foo877; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo877 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo877 OWNER TO pivotal;

--
-- Data for Name: foo877; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo877 (i) FROM stdin;
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
-- Name: foo878; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo878 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo878 OWNER TO pivotal;

--
-- Data for Name: foo878; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo878 (i) FROM stdin;
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
-- Name: foo879; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo879 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo879 OWNER TO pivotal;

--
-- Data for Name: foo879; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo879 (i) FROM stdin;
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
-- Name: foo88; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo88 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo88 OWNER TO pivotal;

--
-- Data for Name: foo88; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo88 (i) FROM stdin;
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
-- Name: foo880; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo880 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo880 OWNER TO pivotal;

--
-- Data for Name: foo880; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo880 (i) FROM stdin;
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
-- Name: foo881; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo881 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo881 OWNER TO pivotal;

--
-- Data for Name: foo881; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo881 (i) FROM stdin;
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
-- Name: foo882; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo882 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo882 OWNER TO pivotal;

--
-- Data for Name: foo882; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo882 (i) FROM stdin;
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
-- Name: foo883; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo883 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo883 OWNER TO pivotal;

--
-- Data for Name: foo883; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo883 (i) FROM stdin;
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
-- Name: foo884; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo884 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo884 OWNER TO pivotal;

--
-- Data for Name: foo884; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo884 (i) FROM stdin;
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
-- Name: foo885; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo885 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo885 OWNER TO pivotal;

--
-- Data for Name: foo885; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo885 (i) FROM stdin;
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
-- Name: foo886; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo886 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo886 OWNER TO pivotal;

--
-- Data for Name: foo886; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo886 (i) FROM stdin;
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
-- Name: foo887; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo887 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo887 OWNER TO pivotal;

--
-- Data for Name: foo887; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo887 (i) FROM stdin;
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
-- Name: foo888; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo888 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo888 OWNER TO pivotal;

--
-- Data for Name: foo888; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo888 (i) FROM stdin;
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
-- Name: foo889; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo889 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo889 OWNER TO pivotal;

--
-- Data for Name: foo889; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo889 (i) FROM stdin;
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
-- Name: foo89; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo89 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo89 OWNER TO pivotal;

--
-- Data for Name: foo89; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo89 (i) FROM stdin;
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
-- Name: foo890; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo890 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo890 OWNER TO pivotal;

--
-- Data for Name: foo890; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo890 (i) FROM stdin;
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
-- Name: foo891; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo891 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo891 OWNER TO pivotal;

--
-- Data for Name: foo891; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo891 (i) FROM stdin;
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
-- Name: foo892; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo892 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo892 OWNER TO pivotal;

--
-- Data for Name: foo892; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo892 (i) FROM stdin;
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
-- Name: foo893; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo893 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo893 OWNER TO pivotal;

--
-- Data for Name: foo893; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo893 (i) FROM stdin;
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
-- Name: foo894; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo894 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo894 OWNER TO pivotal;

--
-- Data for Name: foo894; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo894 (i) FROM stdin;
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
-- Name: foo895; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo895 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo895 OWNER TO pivotal;

--
-- Data for Name: foo895; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo895 (i) FROM stdin;
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
-- Name: foo896; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo896 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo896 OWNER TO pivotal;

--
-- Data for Name: foo896; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo896 (i) FROM stdin;
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
-- Name: foo897; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo897 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo897 OWNER TO pivotal;

--
-- Data for Name: foo897; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo897 (i) FROM stdin;
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
-- Name: foo898; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo898 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo898 OWNER TO pivotal;

--
-- Data for Name: foo898; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo898 (i) FROM stdin;
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
-- Name: foo899; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo899 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo899 OWNER TO pivotal;

--
-- Data for Name: foo899; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo899 (i) FROM stdin;
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
-- Name: foo9; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo9 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo9 OWNER TO pivotal;

--
-- Data for Name: foo9; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo9 (i) FROM stdin;
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
-- Name: foo90; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo90 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo90 OWNER TO pivotal;

--
-- Data for Name: foo90; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo90 (i) FROM stdin;
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
-- Name: foo900; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo900 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo900 OWNER TO pivotal;

--
-- Data for Name: foo900; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo900 (i) FROM stdin;
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
-- Name: foo901; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo901 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo901 OWNER TO pivotal;

--
-- Data for Name: foo901; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo901 (i) FROM stdin;
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
-- Name: foo902; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo902 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo902 OWNER TO pivotal;

--
-- Data for Name: foo902; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo902 (i) FROM stdin;
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
-- Name: foo903; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo903 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo903 OWNER TO pivotal;

--
-- Data for Name: foo903; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo903 (i) FROM stdin;
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
-- Name: foo904; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo904 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo904 OWNER TO pivotal;

--
-- Data for Name: foo904; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo904 (i) FROM stdin;
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
-- Name: foo905; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo905 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo905 OWNER TO pivotal;

--
-- Data for Name: foo905; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo905 (i) FROM stdin;
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
-- Name: foo906; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo906 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo906 OWNER TO pivotal;

--
-- Data for Name: foo906; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo906 (i) FROM stdin;
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
-- Name: foo907; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo907 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo907 OWNER TO pivotal;

--
-- Data for Name: foo907; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo907 (i) FROM stdin;
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
-- Name: foo908; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo908 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo908 OWNER TO pivotal;

--
-- Data for Name: foo908; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo908 (i) FROM stdin;
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
-- Name: foo909; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo909 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo909 OWNER TO pivotal;

--
-- Data for Name: foo909; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo909 (i) FROM stdin;
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
-- Name: foo91; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo91 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo91 OWNER TO pivotal;

--
-- Data for Name: foo91; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo91 (i) FROM stdin;
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
-- Name: foo910; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo910 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo910 OWNER TO pivotal;

--
-- Data for Name: foo910; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo910 (i) FROM stdin;
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
-- Name: foo911; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo911 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo911 OWNER TO pivotal;

--
-- Data for Name: foo911; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo911 (i) FROM stdin;
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
-- Name: foo912; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo912 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo912 OWNER TO pivotal;

--
-- Data for Name: foo912; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo912 (i) FROM stdin;
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
-- Name: foo913; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo913 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo913 OWNER TO pivotal;

--
-- Data for Name: foo913; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo913 (i) FROM stdin;
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
-- Name: foo914; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo914 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo914 OWNER TO pivotal;

--
-- Data for Name: foo914; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo914 (i) FROM stdin;
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
-- Name: foo915; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo915 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo915 OWNER TO pivotal;

--
-- Data for Name: foo915; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo915 (i) FROM stdin;
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
-- Name: foo916; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo916 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo916 OWNER TO pivotal;

--
-- Data for Name: foo916; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo916 (i) FROM stdin;
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
-- Name: foo917; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo917 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo917 OWNER TO pivotal;

--
-- Data for Name: foo917; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo917 (i) FROM stdin;
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
-- Name: foo918; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo918 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo918 OWNER TO pivotal;

--
-- Data for Name: foo918; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo918 (i) FROM stdin;
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
-- Name: foo919; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo919 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo919 OWNER TO pivotal;

--
-- Data for Name: foo919; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo919 (i) FROM stdin;
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
-- Name: foo92; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo92 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo92 OWNER TO pivotal;

--
-- Data for Name: foo92; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo92 (i) FROM stdin;
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
-- Name: foo920; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo920 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo920 OWNER TO pivotal;

--
-- Data for Name: foo920; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo920 (i) FROM stdin;
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
-- Name: foo921; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo921 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo921 OWNER TO pivotal;

--
-- Data for Name: foo921; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo921 (i) FROM stdin;
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
-- Name: foo922; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo922 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo922 OWNER TO pivotal;

--
-- Data for Name: foo922; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo922 (i) FROM stdin;
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
-- Name: foo923; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo923 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo923 OWNER TO pivotal;

--
-- Data for Name: foo923; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo923 (i) FROM stdin;
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
-- Name: foo924; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo924 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo924 OWNER TO pivotal;

--
-- Data for Name: foo924; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo924 (i) FROM stdin;
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
-- Name: foo925; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo925 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo925 OWNER TO pivotal;

--
-- Data for Name: foo925; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo925 (i) FROM stdin;
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
-- Name: foo926; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo926 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo926 OWNER TO pivotal;

--
-- Data for Name: foo926; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo926 (i) FROM stdin;
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
-- Name: foo927; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo927 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo927 OWNER TO pivotal;

--
-- Data for Name: foo927; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo927 (i) FROM stdin;
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
-- Name: foo928; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo928 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo928 OWNER TO pivotal;

--
-- Data for Name: foo928; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo928 (i) FROM stdin;
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
-- Name: foo929; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo929 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo929 OWNER TO pivotal;

--
-- Data for Name: foo929; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo929 (i) FROM stdin;
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
-- Name: foo93; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo93 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo93 OWNER TO pivotal;

--
-- Data for Name: foo93; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo93 (i) FROM stdin;
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
-- Name: foo930; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo930 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo930 OWNER TO pivotal;

--
-- Data for Name: foo930; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo930 (i) FROM stdin;
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
-- Name: foo931; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo931 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo931 OWNER TO pivotal;

--
-- Data for Name: foo931; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo931 (i) FROM stdin;
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
-- Name: foo932; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo932 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo932 OWNER TO pivotal;

--
-- Data for Name: foo932; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo932 (i) FROM stdin;
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
-- Name: foo933; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo933 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo933 OWNER TO pivotal;

--
-- Data for Name: foo933; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo933 (i) FROM stdin;
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
-- Name: foo934; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo934 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo934 OWNER TO pivotal;

--
-- Data for Name: foo934; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo934 (i) FROM stdin;
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
-- Name: foo935; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo935 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo935 OWNER TO pivotal;

--
-- Data for Name: foo935; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo935 (i) FROM stdin;
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
-- Name: foo936; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo936 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo936 OWNER TO pivotal;

--
-- Data for Name: foo936; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo936 (i) FROM stdin;
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
-- Name: foo937; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo937 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo937 OWNER TO pivotal;

--
-- Data for Name: foo937; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo937 (i) FROM stdin;
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
-- Name: foo938; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo938 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo938 OWNER TO pivotal;

--
-- Data for Name: foo938; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo938 (i) FROM stdin;
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
-- Name: foo939; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo939 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo939 OWNER TO pivotal;

--
-- Data for Name: foo939; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo939 (i) FROM stdin;
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
-- Name: foo94; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo94 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo94 OWNER TO pivotal;

--
-- Data for Name: foo94; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo94 (i) FROM stdin;
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
-- Name: foo940; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo940 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo940 OWNER TO pivotal;

--
-- Data for Name: foo940; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo940 (i) FROM stdin;
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
-- Name: foo941; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo941 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo941 OWNER TO pivotal;

--
-- Data for Name: foo941; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo941 (i) FROM stdin;
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
-- Name: foo942; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo942 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo942 OWNER TO pivotal;

--
-- Data for Name: foo942; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo942 (i) FROM stdin;
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
-- Name: foo943; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo943 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo943 OWNER TO pivotal;

--
-- Data for Name: foo943; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo943 (i) FROM stdin;
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
-- Name: foo944; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo944 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo944 OWNER TO pivotal;

--
-- Data for Name: foo944; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo944 (i) FROM stdin;
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
-- Name: foo945; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo945 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo945 OWNER TO pivotal;

--
-- Data for Name: foo945; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo945 (i) FROM stdin;
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
-- Name: foo946; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo946 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo946 OWNER TO pivotal;

--
-- Data for Name: foo946; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo946 (i) FROM stdin;
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
-- Name: foo947; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo947 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo947 OWNER TO pivotal;

--
-- Data for Name: foo947; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo947 (i) FROM stdin;
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
-- Name: foo948; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo948 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo948 OWNER TO pivotal;

--
-- Data for Name: foo948; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo948 (i) FROM stdin;
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
-- Name: foo949; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo949 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo949 OWNER TO pivotal;

--
-- Data for Name: foo949; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo949 (i) FROM stdin;
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
-- Name: foo95; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo95 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo95 OWNER TO pivotal;

--
-- Data for Name: foo95; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo95 (i) FROM stdin;
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
-- Name: foo950; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo950 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo950 OWNER TO pivotal;

--
-- Data for Name: foo950; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo950 (i) FROM stdin;
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
-- Name: foo951; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo951 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo951 OWNER TO pivotal;

--
-- Data for Name: foo951; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo951 (i) FROM stdin;
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
-- Name: foo952; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo952 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo952 OWNER TO pivotal;

--
-- Data for Name: foo952; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo952 (i) FROM stdin;
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
-- Name: foo953; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo953 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo953 OWNER TO pivotal;

--
-- Data for Name: foo953; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo953 (i) FROM stdin;
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
-- Name: foo954; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo954 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo954 OWNER TO pivotal;

--
-- Data for Name: foo954; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo954 (i) FROM stdin;
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
-- Name: foo955; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo955 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo955 OWNER TO pivotal;

--
-- Data for Name: foo955; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo955 (i) FROM stdin;
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
-- Name: foo956; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo956 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo956 OWNER TO pivotal;

--
-- Data for Name: foo956; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo956 (i) FROM stdin;
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
-- Name: foo957; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo957 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo957 OWNER TO pivotal;

--
-- Data for Name: foo957; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo957 (i) FROM stdin;
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
-- Name: foo958; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo958 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo958 OWNER TO pivotal;

--
-- Data for Name: foo958; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo958 (i) FROM stdin;
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
-- Name: foo959; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo959 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo959 OWNER TO pivotal;

--
-- Data for Name: foo959; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo959 (i) FROM stdin;
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
-- Name: foo96; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo96 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo96 OWNER TO pivotal;

--
-- Data for Name: foo96; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo96 (i) FROM stdin;
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
-- Name: foo960; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo960 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo960 OWNER TO pivotal;

--
-- Data for Name: foo960; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo960 (i) FROM stdin;
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
-- Name: foo961; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo961 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo961 OWNER TO pivotal;

--
-- Data for Name: foo961; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo961 (i) FROM stdin;
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
-- Name: foo962; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo962 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo962 OWNER TO pivotal;

--
-- Data for Name: foo962; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo962 (i) FROM stdin;
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
-- Name: foo963; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo963 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo963 OWNER TO pivotal;

--
-- Data for Name: foo963; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo963 (i) FROM stdin;
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
-- Name: foo964; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo964 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo964 OWNER TO pivotal;

--
-- Data for Name: foo964; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo964 (i) FROM stdin;
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
-- Name: foo965; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo965 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo965 OWNER TO pivotal;

--
-- Data for Name: foo965; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo965 (i) FROM stdin;
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
-- Name: foo966; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo966 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo966 OWNER TO pivotal;

--
-- Data for Name: foo966; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo966 (i) FROM stdin;
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
-- Name: foo967; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo967 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo967 OWNER TO pivotal;

--
-- Data for Name: foo967; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo967 (i) FROM stdin;
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
-- Name: foo968; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo968 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo968 OWNER TO pivotal;

--
-- Data for Name: foo968; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo968 (i) FROM stdin;
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
-- Name: foo969; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo969 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo969 OWNER TO pivotal;

--
-- Data for Name: foo969; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo969 (i) FROM stdin;
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
-- Name: foo97; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo97 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo97 OWNER TO pivotal;

--
-- Data for Name: foo97; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo97 (i) FROM stdin;
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
-- Name: foo970; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo970 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo970 OWNER TO pivotal;

--
-- Data for Name: foo970; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo970 (i) FROM stdin;
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
-- Name: foo971; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo971 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo971 OWNER TO pivotal;

--
-- Data for Name: foo971; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo971 (i) FROM stdin;
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
-- Name: foo972; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo972 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo972 OWNER TO pivotal;

--
-- Data for Name: foo972; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo972 (i) FROM stdin;
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
-- Name: foo973; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo973 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo973 OWNER TO pivotal;

--
-- Data for Name: foo973; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo973 (i) FROM stdin;
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
-- Name: foo974; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo974 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo974 OWNER TO pivotal;

--
-- Data for Name: foo974; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo974 (i) FROM stdin;
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
-- Name: foo975; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo975 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo975 OWNER TO pivotal;

--
-- Data for Name: foo975; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo975 (i) FROM stdin;
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
-- Name: foo976; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo976 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo976 OWNER TO pivotal;

--
-- Data for Name: foo976; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo976 (i) FROM stdin;
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
-- Name: foo977; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo977 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo977 OWNER TO pivotal;

--
-- Data for Name: foo977; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo977 (i) FROM stdin;
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
-- Name: foo978; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo978 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo978 OWNER TO pivotal;

--
-- Data for Name: foo978; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo978 (i) FROM stdin;
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
-- Name: foo979; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo979 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo979 OWNER TO pivotal;

--
-- Data for Name: foo979; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo979 (i) FROM stdin;
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
-- Name: foo98; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo98 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo98 OWNER TO pivotal;

--
-- Data for Name: foo98; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo98 (i) FROM stdin;
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
-- Name: foo980; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo980 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo980 OWNER TO pivotal;

--
-- Data for Name: foo980; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo980 (i) FROM stdin;
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
-- Name: foo981; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo981 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo981 OWNER TO pivotal;

--
-- Data for Name: foo981; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo981 (i) FROM stdin;
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
-- Name: foo982; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo982 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo982 OWNER TO pivotal;

--
-- Data for Name: foo982; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo982 (i) FROM stdin;
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
-- Name: foo983; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo983 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo983 OWNER TO pivotal;

--
-- Data for Name: foo983; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo983 (i) FROM stdin;
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
-- Name: foo984; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo984 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo984 OWNER TO pivotal;

--
-- Data for Name: foo984; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo984 (i) FROM stdin;
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
-- Name: foo985; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo985 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo985 OWNER TO pivotal;

--
-- Data for Name: foo985; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo985 (i) FROM stdin;
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
-- Name: foo986; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo986 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo986 OWNER TO pivotal;

--
-- Data for Name: foo986; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo986 (i) FROM stdin;
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
-- Name: foo987; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo987 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo987 OWNER TO pivotal;

--
-- Data for Name: foo987; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo987 (i) FROM stdin;
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
-- Name: foo988; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo988 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo988 OWNER TO pivotal;

--
-- Data for Name: foo988; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo988 (i) FROM stdin;
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
-- Name: foo989; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo989 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo989 OWNER TO pivotal;

--
-- Data for Name: foo989; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo989 (i) FROM stdin;
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
-- Name: foo99; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo99 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo99 OWNER TO pivotal;

--
-- Data for Name: foo99; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo99 (i) FROM stdin;
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
-- Name: foo990; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo990 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo990 OWNER TO pivotal;

--
-- Data for Name: foo990; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo990 (i) FROM stdin;
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
-- Name: foo991; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo991 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo991 OWNER TO pivotal;

--
-- Data for Name: foo991; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo991 (i) FROM stdin;
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
-- Name: foo992; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo992 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo992 OWNER TO pivotal;

--
-- Data for Name: foo992; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo992 (i) FROM stdin;
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
-- Name: foo993; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo993 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo993 OWNER TO pivotal;

--
-- Data for Name: foo993; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo993 (i) FROM stdin;
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
-- Name: foo994; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo994 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo994 OWNER TO pivotal;

--
-- Data for Name: foo994; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo994 (i) FROM stdin;
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
-- Name: foo995; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo995 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo995 OWNER TO pivotal;

--
-- Data for Name: foo995; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo995 (i) FROM stdin;
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
-- Name: foo996; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo996 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo996 OWNER TO pivotal;

--
-- Data for Name: foo996; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo996 (i) FROM stdin;
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
-- Name: foo997; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo997 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo997 OWNER TO pivotal;

--
-- Data for Name: foo997; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo997 (i) FROM stdin;
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
-- Name: foo998; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo998 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo998 OWNER TO pivotal;

--
-- Data for Name: foo998; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo998 (i) FROM stdin;
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
-- Name: foo999; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo999 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.foo999 OWNER TO pivotal;

--
-- Data for Name: foo999; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo999 (i) FROM stdin;
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
-- Name: p; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE p (
    a integer,
    b integer
) DISTRIBUTED BY (a);


ALTER TABLE public.p OWNER TO pivotal;

--
-- Data for Name: p; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY p (a, b) FROM stdin;
\.


--
-- Name: p2; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE p2 (
    a integer,
    b integer
)
INHERITS (p) DISTRIBUTED BY (b);


ALTER TABLE public.p2 OWNER TO pivotal;

--
-- Data for Name: p2; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY p2 (a, b) FROM stdin;
\.


--
-- Name: person; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE person (
    name text,
    age integer,
    location point
) DISTRIBUTED BY (name);


ALTER TABLE public.person OWNER TO pivotal;

--
-- Data for Name: person; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY person (name, age, location) FROM stdin;
\.


--
-- Name: pg_table_inherit_alter_base; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE pg_table_inherit_alter_base (
    did integer,
    name character varying(40),
    CONSTRAINT con1 CHECK (((did > 99) AND ((name)::text <> ''::text)))
) DISTRIBUTED RANDOMLY;


ALTER TABLE public.pg_table_inherit_alter_base OWNER TO pivotal;

--
-- Name: pg_table_inherit; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE pg_table_inherit (
)
INHERITS (pg_table_inherit_alter_base) DISTRIBUTED RANDOMLY;


ALTER TABLE public.pg_table_inherit OWNER TO pivotal;

--
-- Data for Name: pg_table_inherit; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY pg_table_inherit (did, name) FROM stdin;
\.


--
-- Data for Name: pg_table_inherit_alter_base; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY pg_table_inherit_alter_base (did, name) FROM stdin;
\.


SET default_with_oids = true;

--
-- Name: staff_member; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE staff_member (
    salary integer,
    manager name
)
INHERITS (person) DISTRIBUTED BY (name);


ALTER TABLE public.staff_member OWNER TO pivotal;

SET default_with_oids = false;

--
-- Data for Name: staff_member; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY staff_member (name, age, location, salary, manager) FROM stdin;
\.


--
-- Name: student; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE student (
    gpa double precision
)
INHERITS (person) DISTRIBUTED BY (name);


ALTER TABLE public.student OWNER TO pivotal;

SET default_with_oids = true;

--
-- Name: stud_emp; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE stud_emp (
    percent integer
)
INHERITS (staff_member, student) DISTRIBUTED BY (name);


ALTER TABLE public.stud_emp OWNER TO pivotal;

SET default_with_oids = false;

--
-- Data for Name: stud_emp; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY stud_emp (name, age, location, salary, manager, gpa, percent) FROM stdin;
\.


--
-- Data for Name: student; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY student (name, age, location, gpa) FROM stdin;
\.


--
-- Name: public; Type: ACL; Schema: -; Owner: pivotal
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM pivotal;
GRANT ALL ON SCHEMA public TO pivotal;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- Greenplum Database database dump complete
--

