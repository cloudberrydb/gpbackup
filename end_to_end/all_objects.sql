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
-- Name: schema2; Type: SCHEMA; Schema: -; Owner: pivotal
--

CREATE SCHEMA schema2;


ALTER SCHEMA schema2 OWNER TO pivotal;

--
-- Name: plperl; Type: PROCEDURAL LANGUAGE; Schema: -; Owner: pivotal
--

CREATE PROCEDURAL LANGUAGE plperl;
ALTER FUNCTION plperl_call_handler() OWNER TO pivotal;
ALTER FUNCTION plperl_validator(oid) OWNER TO pivotal;


SET search_path = public, pg_catalog;

--
-- Name: base_type; Type: SHELL TYPE; Schema: public; Owner: pivotal
--

CREATE TYPE base_type;


--
-- Name: base_fn_in(cstring); Type: FUNCTION; Schema: public; Owner: pivotal
--

CREATE FUNCTION base_fn_in(cstring) RETURNS base_type
    AS $$boolin$$
    LANGUAGE internal NO SQL;


ALTER FUNCTION public.base_fn_in(cstring) OWNER TO pivotal;

--
-- Name: base_fn_out(base_type); Type: FUNCTION; Schema: public; Owner: pivotal
--

CREATE FUNCTION base_fn_out(base_type) RETURNS cstring
    AS $$boolout$$
    LANGUAGE internal NO SQL;


ALTER FUNCTION public.base_fn_out(base_type) OWNER TO pivotal;

--
-- Name: base_type; Type: TYPE; Schema: public; Owner: pivotal
--

CREATE TYPE base_type (
    INTERNALLENGTH = variable,
    INPUT = base_fn_in,
    OUTPUT = base_fn_out,
    ALIGNMENT = int4,
    STORAGE = plain
);


ALTER TYPE public.base_type OWNER TO pivotal;

--
-- Name: composite_type; Type: TYPE; Schema: public; Owner: pivotal
--

CREATE TYPE composite_type AS (
	name integer,
	name1 integer,
	name2 text
);


ALTER TYPE public.composite_type OWNER TO pivotal;

--
-- Name: enum_type; Type: TYPE; Schema: public; Owner: pivotal
--

CREATE TYPE enum_type AS ENUM (
    '750582',
    '750583',
    '750584'
);


ALTER TYPE public.enum_type OWNER TO pivotal;

--
-- Name: casttoint(text); Type: FUNCTION; Schema: public; Owner: pivotal
--

CREATE FUNCTION casttoint(text) RETURNS integer
    AS $_$
SELECT cast($1 as integer);
$_$
    LANGUAGE sql IMMUTABLE STRICT CONTAINS SQL;


ALTER FUNCTION public.casttoint(text) OWNER TO pivotal;

--
-- Name: dup(integer); Type: FUNCTION; Schema: public; Owner: pivotal
--

CREATE FUNCTION dup(integer DEFAULT 42, OUT f1 integer, OUT f2 text) RETURNS record
    AS $_$
SELECT $1, CAST($1 AS text) || ' is text'
$_$
    LANGUAGE sql CONTAINS SQL;


ALTER FUNCTION public.dup(integer, OUT f1 integer, OUT f2 text) OWNER TO pivotal;

--
-- Name: mypre_accum(numeric, numeric); Type: FUNCTION; Schema: public; Owner: pivotal
--

CREATE FUNCTION mypre_accum(numeric, numeric) RETURNS numeric
    AS $_$
select $1 + $2
$_$
    LANGUAGE sql IMMUTABLE STRICT CONTAINS SQL;


ALTER FUNCTION public.mypre_accum(numeric, numeric) OWNER TO pivotal;

--
-- Name: mysfunc_accum(numeric, numeric, numeric); Type: FUNCTION; Schema: public; Owner: pivotal
--

CREATE FUNCTION mysfunc_accum(numeric, numeric, numeric) RETURNS numeric
    AS $_$
select $1 + $2 + $3
$_$
    LANGUAGE sql IMMUTABLE STRICT CONTAINS SQL;


ALTER FUNCTION public.mysfunc_accum(numeric, numeric, numeric) OWNER TO pivotal;

--
-- Name: plusone(text); Type: FUNCTION; Schema: public; Owner: pivotal
--

CREATE FUNCTION plusone(x text) RETURNS text
    AS $$
BEGIN
    RETURN x || 'x';
END;
$$
    LANGUAGE plpgsql NO SQL;


ALTER FUNCTION public.plusone(x text) OWNER TO pivotal;

--
-- Name: plusone(character varying); Type: FUNCTION; Schema: public; Owner: pivotal
--

CREATE FUNCTION plusone(x character varying) RETURNS character varying
    AS $$
BEGIN
    RETURN x || 'a';
END;
$$
    LANGUAGE plpgsql NO SQL
    SET standard_conforming_strings TO 'on'
    SET client_min_messages TO 'notice'
    SET search_path TO public;


ALTER FUNCTION public.plusone(x character varying) OWNER TO pivotal;

--
-- Name: return_enum_as_array(anyenum, anyelement, anyelement); Type: FUNCTION; Schema: public; Owner: pivotal
--

CREATE FUNCTION return_enum_as_array(anyenum, anyelement, anyelement) RETURNS TABLE(ae anyenum, aa anyarray)
    AS $_$
SELECT $1, array[$2, $3]
$_$
    LANGUAGE sql STABLE CONTAINS SQL;


ALTER FUNCTION public.return_enum_as_array(anyenum, anyelement, anyelement) OWNER TO pivotal;

--
-- Name: agg_prefunc(numeric, numeric); Type: AGGREGATE; Schema: public; Owner: pivotal
--

CREATE AGGREGATE agg_prefunc(numeric, numeric) (
    SFUNC = mysfunc_accum,
    STYPE = numeric,
    INITCOND = '0',
    PREFUNC = mypre_accum
);


ALTER AGGREGATE public.agg_prefunc(numeric, numeric) OWNER TO pivotal;

--
-- Name: agg_test(integer); Type: AGGREGATE; Schema: public; Owner: pivotal
--

CREATE AGGREGATE agg_test(integer) (
    SFUNC = int4xor,
    STYPE = integer,
    INITCOND = '0'
);


ALTER AGGREGATE public.agg_test(integer) OWNER TO pivotal;

--
-- Name: ####; Type: OPERATOR; Schema: public; Owner: pivotal
--

CREATE OPERATOR #### (
    PROCEDURE = numeric_fac,
    LEFTARG = bigint
);


ALTER OPERATOR public.#### (bigint, NONE) OWNER TO pivotal;

--
-- Name: test_op_class; Type: OPERATOR CLASS; Schema: public; Owner: pivotal
--

CREATE OPERATOR CLASS test_op_class
    FOR TYPE uuid USING hash AS
    STORAGE uuid;


ALTER OPERATOR CLASS public.test_op_class USING hash OWNER TO pivotal;

--
-- Name: testfam; Type: OPERATOR FAMILY; Schema: public; Owner: pivotal
--

CREATE OPERATOR FAMILY testfam USING gist;


ALTER OPERATOR FAMILY public.testfam USING gist OWNER TO pivotal;

--
-- Name: testclass; Type: OPERATOR CLASS; Schema: public; Owner: pivotal
--

CREATE OPERATOR CLASS testclass
    FOR TYPE uuid USING gist FAMILY testfam AS
    OPERATOR 1 =(uuid,uuid) RECHECK ,
    OPERATOR 2 <(uuid,uuid) ,
    FUNCTION 1 abs(integer) ,
    FUNCTION 2 int4out(integer);


ALTER OPERATOR CLASS public.testclass USING gist OWNER TO pivotal;

SET default_tablespace = '';

--
-- Name: bar; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE bar (
    i integer NOT NULL,
    j text,
    k smallint NOT NULL,
    l character varying(20)
) DISTRIBUTED BY (i);


ALTER TABLE public.bar OWNER TO pivotal;

--
-- Data for Name: bar; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY bar (i, j, k, l) FROM stdin;
\.


--
-- Name: foo; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo (
    k text,
    i integer,
    j text
) DISTRIBUTED RANDOMLY;


ALTER TABLE public.foo OWNER TO pivotal;

--
-- Data for Name: foo; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo (k, i, j) FROM stdin;
\.


--
-- Name: foo2; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo2 (
    k text,
    l character varying(20)
)
INHERITS (foo) DISTRIBUTED RANDOMLY;


ALTER TABLE public.foo2 OWNER TO pivotal;

--
-- Data for Name: foo2; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo2 (k, i, j, l) FROM stdin;
\.


SET search_path = schema2, pg_catalog;

--
-- Name: foo3; Type: TABLE; Schema: schema2; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo3 (
    m double precision
)
INHERITS (public.foo2) DISTRIBUTED RANDOMLY;


ALTER TABLE schema2.foo3 OWNER TO pivotal;

SET search_path = public, pg_catalog;

--
-- Name: foo4; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE foo4 (
    n integer
)
INHERITS (schema2.foo3) DISTRIBUTED RANDOMLY;


ALTER TABLE public.foo4 OWNER TO pivotal;

--
-- Data for Name: foo4; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY foo4 (k, i, j, l, m, n) FROM stdin;
\.


--
-- Name: gpcrondump_history; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE gpcrondump_history (
    rec_date timestamp without time zone,
    start_time character(8),
    end_time character(8),
    options text,
    dump_key character varying(20),
    dump_exit_status smallint,
    script_exit_status smallint,
    exit_text character varying(10)
) DISTRIBUTED BY (rec_date);


ALTER TABLE public.gpcrondump_history OWNER TO pivotal;

--
-- Data for Name: gpcrondump_history; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY gpcrondump_history (rec_date, start_time, end_time, options, dump_key, dump_exit_status, script_exit_status, exit_text) FROM stdin;
\.


SET search_path = schema2, pg_catalog;

--
-- Data for Name: foo3; Type: TABLE DATA; Schema: schema2; Owner: pivotal
--

COPY foo3 (k, i, j, l, m) FROM stdin;
\.


--
-- Name: noatts; Type: TABLE; Schema: schema2; Owner: pivotal; Tablespace: 
--

CREATE TABLE noatts (
) DISTRIBUTED RANDOMLY;


ALTER TABLE schema2.noatts OWNER TO pivotal;

--
-- Data for Name: noatts; Type: TABLE DATA; Schema: schema2; Owner: pivotal
--

COPY noatts  FROM stdin;
\.


SET search_path = public, pg_catalog;

--
-- Name: pk_table; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE pk_table (
    a integer NOT NULL
) DISTRIBUTED BY (a);


ALTER TABLE public.pk_table OWNER TO pivotal;

--
-- Data for Name: pk_table; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY pk_table (a) FROM stdin;
\.


--
-- Name: reference_table; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE reference_table (
    a integer,
    b integer
) DISTRIBUTED BY (a);


ALTER TABLE public.reference_table OWNER TO pivotal;

--
-- Data for Name: reference_table; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY reference_table (a, b) FROM stdin;
\.


SET search_path = schema2, pg_catalog;

--
-- Name: prime; Type: TABLE; Schema: schema2; Owner: pivotal; Tablespace: 
--

CREATE TABLE prime (
    i integer NOT NULL,
    j integer
) DISTRIBUTED BY (i);


ALTER TABLE schema2.prime OWNER TO pivotal;

--
-- Data for Name: prime; Type: TABLE DATA; Schema: schema2; Owner: pivotal
--

COPY prime (i, j) FROM stdin;
\.


SET search_path = public, pg_catalog;

--
-- Name: rule_table1; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE rule_table1 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.rule_table1 OWNER TO pivotal;

--
-- Data for Name: rule_table1; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY rule_table1 (i) FROM stdin;
\.


SET search_path = pg_catalog;

--
-- Name: CAST (text AS integer); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (text AS integer) WITH FUNCTION public.casttoint(text) AS ASSIGNMENT;


--
-- Name: CAST (text AS integer); Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON CAST (text AS integer) IS 'sample cast';


SET search_path = public, pg_catalog;

--
-- Name: trigger_table1; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE trigger_table1 (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.trigger_table1 OWNER TO pivotal;

--
-- Data for Name: trigger_table1; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY trigger_table1 (i) FROM stdin;
\.


--
-- Name: uniq; Type: TABLE; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE TABLE uniq (
    i integer
) DISTRIBUTED BY (i);


ALTER TABLE public.uniq OWNER TO pivotal;

--
-- Data for Name: uniq; Type: TABLE DATA; Schema: public; Owner: pivotal
--

COPY uniq (i) FROM stdin;
\.


SET search_path = schema2, pg_catalog;

--
-- Name: with_multiple_check; Type: TABLE; Schema: schema2; Owner: pivotal; Tablespace: 
--

CREATE TABLE with_multiple_check (
    a integer,
    b character varying(40),
    CONSTRAINT con1 CHECK (((a > 99) AND ((b)::text <> ''::text)))
) DISTRIBUTED BY (a);


ALTER TABLE schema2.with_multiple_check OWNER TO pivotal;

--
-- Data for Name: with_multiple_check; Type: TABLE DATA; Schema: schema2; Owner: pivotal
--

COPY with_multiple_check (a, b) FROM stdin;
\.


SET search_path = public, pg_catalog;

--
-- Name: testconv; Type: CONVERSION; Schema: public; Owner: pivotal
--

CREATE CONVERSION testconv FOR 'LATIN1' TO 'MULE_INTERNAL' FROM latin1_to_mic;


ALTER CONVERSION public.testconv OWNER TO pivotal;

--
-- Name: testdictionary; Type: TEXT SEARCH DICTIONARY; Schema: public; Owner: pivotal
--

CREATE TEXT SEARCH DICTIONARY testdictionary (
    TEMPLATE = pg_catalog.snowball,
    language = 'russian', stopwords = 'russian' );


ALTER TEXT SEARCH DICTIONARY public.testdictionary OWNER TO pivotal;

--
-- Name: testconfiguration; Type: TEXT SEARCH CONFIGURATION; Schema: public; Owner: pivotal
--

CREATE TEXT SEARCH CONFIGURATION testconfiguration (
    PARSER = pg_catalog."default" );


ALTER TEXT SEARCH CONFIGURATION public.testconfiguration OWNER TO pivotal;

--
-- Name: testtemplate; Type: TEXT SEARCH TEMPLATE; Schema: public; Owner: 
--

CREATE TEXT SEARCH TEMPLATE testtemplate (
    LEXIZE = dsimple_lexize );


--
-- Name: test_view; Type: VIEW; Schema: public; Owner: pivotal
--

CREATE VIEW test_view AS
    SELECT pk_table.a FROM pk_table;


ALTER TABLE public.test_view OWNER TO pivotal;

--
-- Name: view_view; Type: VIEW; Schema: public; Owner: pivotal
--

CREATE VIEW view_view AS
    SELECT test_view.a FROM test_view;


ALTER TABLE public.view_view OWNER TO pivotal;

SET search_path = schema2, pg_catalog;

--
-- Name: seq_one; Type: SEQUENCE; Schema: schema2; Owner: pivotal
--

CREATE SEQUENCE seq_one
    START WITH 3
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE schema2.seq_one OWNER TO pivotal;

--
-- Name: seq_one; Type: SEQUENCE OWNED BY; Schema: schema2; Owner: pivotal
--

ALTER SEQUENCE seq_one OWNED BY prime.j;


--
-- Name: seq_one; Type: SEQUENCE SET; Schema: schema2; Owner: pivotal
--

SELECT pg_catalog.setval('seq_one', 3, false);


SET search_path = public, pg_catalog;

--
-- Name: testparser; Type: TEXT SEARCH PARSER; Schema: public; Owner: 
--

CREATE TEXT SEARCH PARSER testparser (
    START = prsd_start,
    GETTOKEN = prsd_nexttoken,
    END = prsd_end,
    LEXTYPES = prsd_lextype );


--
-- Name: pk_table_pkey; Type: CONSTRAINT; Schema: public; Owner: pivotal; Tablespace: 
--

ALTER TABLE ONLY pk_table
    ADD CONSTRAINT pk_table_pkey PRIMARY KEY (a);


--
-- Name: uniq_i_key; Type: CONSTRAINT; Schema: public; Owner: pivotal; Tablespace: 
--

ALTER TABLE ONLY uniq
    ADD CONSTRAINT uniq_i_key UNIQUE (i);


SET search_path = schema2, pg_catalog;

--
-- Name: prime_pkey; Type: CONSTRAINT; Schema: schema2; Owner: pivotal; Tablespace: 
--

ALTER TABLE ONLY prime
    ADD CONSTRAINT prime_pkey PRIMARY KEY (i);


SET search_path = public, pg_catalog;

--
-- Name: simple_table_idx1; Type: INDEX; Schema: public; Owner: pivotal; Tablespace: 
--

CREATE INDEX simple_table_idx1 ON foo4 USING btree (n);


--
-- Name: double_insert; Type: RULE; Schema: public; Owner: pivotal
--

CREATE RULE double_insert AS ON INSERT TO rule_table1 DO INSERT INTO rule_table1 DEFAULT VALUES;


--
-- Name: sync_trigger_table1; Type: TRIGGER; Schema: public; Owner: pivotal
--

CREATE TRIGGER sync_trigger_table1
    AFTER INSERT OR DELETE OR UPDATE ON trigger_table1
    FOR EACH STATEMENT
    EXECUTE PROCEDURE flatfile_update_trigger();


--
-- Name: reference_table_b_fkey; Type: FK CONSTRAINT; Schema: public; Owner: pivotal
--

ALTER TABLE ONLY reference_table
    ADD CONSTRAINT reference_table_b_fkey FOREIGN KEY (b) REFERENCES pk_table(a);


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

