#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_foreign_server.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#include "curl/curl.h"
#include "libjson-0.8/json.h"

PG_MODULE_MAGIC;

/*
 * From apiwiki.twitter.com
{"results":[

     {"text":"@twitterapi  http:\/\/tinyurl.com\/ctrefg",
     "to_user_id":396524,
     "to_user":"TwitterAPI",
     "from_user":"jkoum",
     "metadata":
     {
      "result_type":"popular",
      "recent_retweets": 109
     },
     "id":1478555574,   
     "from_user_id":1833773,
     "iso_language_code":"nl",
     "source":"<a href="http:\/\/twitter.com\/">twitter<\/a>",
     "profile_image_url":"http:\/\/s3.amazonaws.com\/twitter_production\/profile_images\/118412707\/2522215727_a5f07da155_b_normal.jpg",
     "created_at":"Wed, 08 Apr 2009 19:22:10 +0000"},
     ... truncated ...],
     "since_id":0,
     "max_id":1480307926,
     "refresh_url":"?since_id=1480307926&q=%40twitterapi",
     "results_per_page":15,
     "next_page":"?page=2&max_id=1480307926&q=%40twitterapi",
     "completed_in":0.031704,
     "page":1,
     "query":"%40twitterapi"}
}
 */

#define SEARCH_ENDPOINT "http://search.twitter.com/search.json"

#define PROCID_TEXTEQ 67

#if PG_VERSION_NUM < 90200
#define OLD_FDW_API
#else
#undef OLD_FDW_API
#endif

/*
 * The index of each item in fdw_private.
 * Since it needs to be stored as List, we keep all pointers
 * into one List and take them out later.
 */
enum
{
	FDW_PRIVATE_URL = 0,
	FDW_PRIVATE_CLAUSES,
	FDW_PRIVATE_PARAM_Q,
	FDW_PRIVATE_LAST
};

/*
 * Currently, only PUSHDOWN and FILTER_LOCALLY are effective
 */
enum
{
	PUSHDOWN,
	BOTH,
	FILTER_LOCALLY
};

typedef struct ResultRoot
{
	struct ResultArray	   *results;
} ResultRoot;

typedef struct ResultArray
{
	int					index;
	struct Tweet	   *elements[512];
} ResultArray;

typedef struct Tweet
{
	char	   *id;
	char	   *text;
	char	   *from_user;
	char	   *from_user_id;
	char	   *to_user;
	char	   *to_user_id;
	char	   *iso_language_code;
	char	   *source;
	char	   *profile_image_url;
	char	   *created_at;
} Tweet;

typedef struct TwitterReply
{
	ResultRoot	   *root;
	AttInMetadata  *attinmeta;
	int				rownum;
	char		   *q;
} TwitterReply;

extern Datum twitter_fdw_validator(PG_FUNCTION_ARGS);
extern Datum twitter_fdw_handler(PG_FUNCTION_ARGS);

/*
 * FDW callback routines
 */
#ifdef OLD_FDW_API
static FdwPlan *twitterPlan(Oid foreigntableid,
						PlannerInfo *root,
						RelOptInfo *baserel);
#else
static void twitterGetRelSize(PlannerInfo *root, RelOptInfo *baserel,
							Oid foreigntableid);
static void twitterGetPaths(PlannerInfo *root, RelOptInfo *baserel,
							Oid foreigntableid);
static ForeignScan *twitterGetPlan(PlannerInfo *root, RelOptInfo *baserel,
							Oid foreigntableid, ForeignPath *best_path,
							List *tlist, List *scan_clauses);
static bool twitterAnalyze(Relation relation, AcquireSampleRowsFunc *func,
							BlockNumber *totalpages);
#endif
static void twitterExplain(ForeignScanState *node, ExplainState *es);
static void twitterBegin(ForeignScanState *node, int eflags);
static TupleTableSlot *twitterIterate(ForeignScanState *node);
static void twitterReScan(ForeignScanState *node);
static void twitterEnd(ForeignScanState *node);

static size_t write_data(void *buffer, size_t size, size_t nmemb, void *userp);
static void *create_structure(int nesting, int is_object);
static void *create_data(int type, const char *data, uint32_t length);
static int append(void *structure, char *key, uint32_t key_length, void *obj);


PG_FUNCTION_INFO_V1(twitter_fdw_validator);
Datum
twitter_fdw_validator(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(twitter_fdw_handler);
Datum
twitter_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	/*
	 * Anything except Begin/Iterate is blank so far,
	 * but FDW interface assumes all valid function pointers.
	 */
#ifdef OLD_FDW_API
	fdwroutine->PlanForeignScan = twitterPlan;
#else
	fdwroutine->GetForeignRelSize = twitterGetRelSize;
	fdwroutine->GetForeignPaths = twitterGetPaths;
	fdwroutine->GetForeignPlan = twitterGetPlan;
	fdwroutine->AnalyzeForeignTable = twitterAnalyze;
#endif;
	fdwroutine->ExplainForeignScan = twitterExplain;
	fdwroutine->BeginForeignScan = twitterBegin;
	fdwroutine->IterateForeignScan = twitterIterate;
	fdwroutine->ReScanForeignScan = twitterReScan;
	fdwroutine->EndForeignScan = twitterEnd;

	PG_RETURN_POINTER(fdwroutine);
}

static char *
percent_encode(unsigned char *s, int srclen)
{
	unsigned char  *end;
	StringInfoData  buf;
	int				len;

	initStringInfo(&buf);

	if (srclen < 0)
		srclen = strlen((char *) s);

	end = s + srclen;

	for (; s < end; s += len)
	{
		unsigned char  *utf;
		int				ulen;

		len = pg_mblen((const char *) s);

		if (len == 1)
		{
			if (('0' <= s[0] && s[0] <= '9') ||
				('A' <= s[0] && s[0] <= 'Z') ||
				('a' <= s[0] && s[0] <= 'z') ||
				(s[0] == '-') || (s[0] == '.') ||
				(s[0] == '_') || (s[0] == '~'))
			{
				appendStringInfoChar(&buf, s[0]);
				continue;
			}
		}

		utf = pg_do_encoding_conversion(s, len, GetDatabaseEncoding(), PG_UTF8);
		ulen = pg_encoding_mblen(PG_UTF8, (const char *) utf);
		while(ulen--)
		{
			appendStringInfo(&buf, "%%%2X", *utf);
			utf++;
		}
	}

	return buf.data;
}

static char *
twitter_param(Node *node, TupleDesc tupdesc)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) node;
		Node	   *left, *right;
		Index		varattno;
		char	   *key, *val;

		if (list_length(op->args) != 2)
			return NULL;
		left = list_nth(op->args, 0);
		if (!IsA(left, Var))
			return NULL;
		varattno = ((Var *) left)->varattno;
		Assert(0 < varattno && varattno <= tupdesc->natts);
		key = NameStr(tupdesc->attrs[varattno - 1]->attname);

		if (strcmp(key, "q") == 0)
		{
			right = list_nth(op->args, 1);
			if (op->opfuncid != PROCID_TEXTEQ)
				elog(ERROR, "invalid operator");

			if (IsA(right, Const))
			{
				StringInfoData	buf;

				initStringInfo(&buf);
				val = TextDatumGetCString(((Const *) right)->constvalue);
				appendStringInfo(&buf, "q=%s",
					percent_encode((unsigned char *) val, -1));
				return buf.data;
			}
			else
				elog(ERROR, "twitter_fdw: parameter q must be a constant");
		}
	}

	return NULL;
}

/*
 * @return fdw_private data
 */
static List *
extract_twitter_conditions(List *conditions, TupleDesc tupdesc)
{
	List		   *result;
	ListCell	   *l;
	StringInfoData	url;
	char		   *param_q;
	int			   *handle_clauses;
	int				clause_count;
	bool			param_first;

	result = NIL;
	initStringInfo(&url);
	appendStringInfoString(&url, SEARCH_ENDPOINT);
	param_q = NULL;
	handle_clauses = (int *) palloc0(sizeof(int) * list_length(conditions));
	clause_count = -1;
	param_first = true;
	foreach (l, conditions)
	{
		RestrictInfo	   *cond = (RestrictInfo *) lfirst(l);
		char	   *param;

		param = twitter_param((Node *) cond->clause, tupdesc);
		if (param)
		{
			if (param_first)
				appendStringInfoChar(&url, '?');
			else
				appendStringInfoChar(&url, '&');
			appendStringInfoString(&url, param);
			param_first = false;
			if (param[0] == 'q' && param[1] == '=')
			{
				param_q = &param[2];
			}
			/* add more, if any */

			handle_clauses[++clause_count] = PUSHDOWN;
		}
		else
			handle_clauses[++clause_count] = FILTER_LOCALLY;
	}

	result = lappend(result, url.data);
	result = lappend(result, handle_clauses);
	result = lappend(result, param_q);
	Assert(list_length(result) == FDW_PRIVATE_LAST);

	return result;
}

static List *
remove_pushdown(List *scan_clauses, int *handle_clauses)
{
	List	   *keep_clauses;

	if (handle_clauses != NULL)
	{
		int				i;
		ListCell	   *l;

		i = 0;
		keep_clauses = NIL;
		foreach(l, scan_clauses)
		{
			RestrictInfo	   *condition = lfirst(l);

			if (handle_clauses[i] != PUSHDOWN)
				keep_clauses = lappend(keep_clauses, condition);

			i++;
		}
	}
	else
		keep_clauses = scan_clauses;

	return keep_clauses;
}

#ifdef OLD_FDW_API
/*
 * twitterPlan
 *   Create a FdwPlan, which is empty for now.
 */
static FdwPlan *
twitterPlan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel)
{
	FdwPlan	   *fdwplan;
	Relation	relation;
	TupleDesc	tupdesc;
	int		   *handle_clauses;

	fdwplan = makeNode(FdwPlan);
	relation = relation_open(foreigntableid, AccessShareLock);
	tupdesc = relation->rd_att;
	fdwplan->fdw_private = extract_twitter_conditions(baserel->baserestrictinfo, tupdesc);
	relation_close(relation, AccessShareLock);

	handle_clauses = list_nth(fdwplan->fdw_private, FDW_PRIVATE_CLAUSES);
	baserel->baserestrictinfo =
		remove_pushdown(baserel->baserestrictinfo, handle_clauses);

	return fdwplan;
}

#else

static void
twitterGetRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	/* API returns at most 15 results by default */
	baserel->rows = 15;
	baserel->fdw_private = NULL;
}

static void
twitterGetPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	Relation	relation;
	TupleDesc	tupdesc;

	relation = relation_open(foreigntableid, AccessShareLock);
	tupdesc = relation->rd_att;
	baserel->fdw_private = extract_twitter_conditions(baserel->baserestrictinfo, tupdesc);
	relation_close(relation, AccessShareLock);

	/* Create a ForeignPath node and add it as only possible path */
	add_path(baserel,
		(Path *)create_foreignscan_path(root, baserel, baserel->rows,
				10, 1000, NIL, NULL, NIL));
}

static ForeignScan *
twitterGetPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
			   ForeignPath *bast_path, List *tlist, List *scan_clauses)
{
	List	   *keep_clauses;
	int		   *handle_clauses;

	handle_clauses = list_nth(baserel->fdw_private, FDW_PRIVATE_CLAUSES);
	keep_clauses = remove_pushdown(scan_clauses, handle_clauses);

	/* remove the RestrictInfo node from all remaining clauses */
	keep_clauses = extract_actual_clauses(keep_clauses, false);

	return make_foreignscan(tlist, keep_clauses, baserel->relid, NIL, baserel->fdw_private);
}

static bool
twitterAnalyze(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages)
{
	*totalpages = 42;
	return true;
}

#endif   /* OLD_FDW_API */

/*
 * twitterExplain
 *   Produce extra output for EXPLAIN
 */
static void
twitterExplain(ForeignScanState *node, ExplainState *es)
{
#ifdef OLD_FDW_API
	List		   *fdw_private =
		((FdwPlan *) ((ForeignScan *) node->ss.ps.plan)->fdwplan)->fdw_private;
#else
	List		   *fdw_private =
		((ForeignScan *)node->ss.ps.plan)->fdw_private;
#endif
	char		   *url;
	char			buf[256];

	url = list_nth(fdw_private, FDW_PRIVATE_URL);
	snprintf(buf, 256, "Search: %s", url);
	ExplainPropertyText("Twitter API", buf, es);
}

/*
 * twitterBegin
 *   Query search API and setup result
 */
static void
twitterBegin(ForeignScanState *node, int eflags)
{
#ifdef OLD_FDW_API
	List		   *fdw_private =
		((FdwPlan *) ((ForeignScan *) node->ss.ps.plan)->fdwplan)->fdw_private;
#else
	List		   *fdw_private =
		((ForeignScan *)node->ss.ps.plan)->fdw_private;
#endif
	CURL		   *curl;
	int				ret;
	json_parser		parser;
	json_parser_dom helper;
	ResultRoot	   *root;
	Relation		rel;
	AttInMetadata  *attinmeta;
	TwitterReply   *reply;
	char		   *url;
	char		   *param_q = NULL;

	/*
	 * Do nothing in EXPLAIN
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	Assert(list_length(fdw_private) == FDW_PRIVATE_LAST);
	url = list_nth(fdw_private, FDW_PRIVATE_URL);
	param_q = list_nth(fdw_private, FDW_PRIVATE_PARAM_Q);

	json_parser_dom_init(&helper, create_structure, create_data, append);
	json_parser_init(&parser, NULL, json_parser_dom_callback, &helper);

	elog(DEBUG1, "requesting %s", url);
	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &parser);
	ret = curl_easy_perform(curl);
	curl_easy_cleanup(curl);

	rel = node->ss.ss_currentRelation;
	attinmeta = TupleDescGetAttInMetadata(rel->rd_att);

	root = (ResultRoot *) helper.root_structure;

	/* status != 200, or other similar error */
	if (!root)
		elog(INFO, "Failed fetching response from %s", url);

#ifdef NOT_USE
	if (root->results)
	{
		int i;
		for(i = 0; i < root->results->index; i++)
		{
			Tweet *tweet = root->results->elements[i];
			printf("%02d[%s]: %s\n", (i + 1), tweet->from_user, tweet->text);
		}
	}
#endif

	reply = (TwitterReply *) palloc(sizeof(TwitterReply));
	reply->root = root;
	reply->attinmeta = attinmeta;
	reply->rownum = 0;
	reply->q = param_q;
	node->fdw_state = (void *) reply;

	json_parser_free(&parser);
}

/*
 * twitterIterate
 *   Return a twitter per call
 */
static TupleTableSlot *
twitterIterate(ForeignScanState *node)
{
	TupleTableSlot	   *slot = node->ss.ss_ScanTupleSlot;
	TwitterReply	   *reply = (TwitterReply *) node->fdw_state;
	ResultRoot		   *root = reply->root;
	Tweet			   *tweet;
	HeapTuple			tuple;
	Relation			rel = node->ss.ss_currentRelation;
	int					i, natts;
	char			  **values;
	MemoryContext		oldcontext;

	if (!root || !(root->results && reply->rownum < root->results->index))
	{
		ExecClearTuple(slot);
		return slot;
	}
	tweet = root->results->elements[reply->rownum];
	natts = rel->rd_att->natts;
	values = (char **) palloc(sizeof(char *) * natts);
	for (i = 0; i < natts; i++)
	{
		Name	attname = &rel->rd_att->attrs[i]->attname;

		if (namestrcmp(attname, "id") == 0 && tweet->id)
			values[i] = tweet->id;
		else if (namestrcmp(attname, "text") == 0 && tweet->text)
			values[i] = tweet->text;
		else if (namestrcmp(attname, "from_user") == 0 && tweet->from_user)
			values[i] = tweet->from_user;
		else if (namestrcmp(attname, "from_user_id") == 0 && tweet->from_user_id)
			values[i] = tweet->from_user_id;
		else if (namestrcmp(attname, "to_user") == 0 && tweet->to_user)
			values[i] = tweet->to_user;
		else if (namestrcmp(attname, "to_user_id") == 0 && tweet->to_user_id)
			values[i] = tweet->to_user_id;
		else if (namestrcmp(attname, "iso_language_code") == 0 && tweet->iso_language_code)
			values[i] = tweet->iso_language_code;
		else if (namestrcmp(attname, "source") == 0 && tweet->source)
			values[i] = tweet->source;
		else if (namestrcmp(attname, "profile_image_url") == 0 && tweet->profile_image_url)
			values[i] = tweet->profile_image_url;
		else if (namestrcmp(attname, "created_at") == 0 && tweet->created_at)
			values[i] = tweet->created_at;
		else if (namestrcmp(attname, "q") == 0)
			values[i] = reply->q;
		else
			values[i] = NULL;
	}
	oldcontext = MemoryContextSwitchTo(node->ss.ps.ps_ExprContext->ecxt_per_query_memory);
	tuple = BuildTupleFromCStrings(reply->attinmeta, values);
	MemoryContextSwitchTo(oldcontext);
	ExecStoreTuple(tuple, slot, InvalidBuffer, true);
	reply->rownum++;

	return slot;
}

/*
 * twitterReScan
 */
static void
twitterReScan(ForeignScanState *node)
{
	TwitterReply	   *reply = (TwitterReply *) node->fdw_state;

	reply->rownum = 0;
}

static void
twitterEnd(ForeignScanState *node)
{
	/* intentionally left blank */
}

static size_t
write_data(void *buffer, size_t size, size_t nmemb, void *userp)
{
	int			segsize = size * nmemb;
	json_parser *parser = (json_parser *) userp;
	int			ret;

	ret = json_parser_string(parser, buffer, segsize, NULL);
	if (ret){
		elog(ERROR, "json_parser failed");
	}

	return segsize;
}

/*
 * since create_structure() raise error on returning NULL,
 * dummy pointer will be returned if the result can be discarded.
 */
static void *dummy_p = (void *) "dummy";

static void *
create_structure(int nesting, int is_object)
{
	if (is_object)
	{
		if (nesting == 0)
		{
			ResultRoot	   *root;

			root = (ResultRoot *) palloc0(sizeof(ResultRoot));
			return (void *) root;
		}
		else if (nesting == 2)
		{
			Tweet	   *tweet;

			tweet = (Tweet *) palloc0(sizeof(Tweet));
			return (void *) tweet;
		}
		return dummy_p;
	}
	else
	{
		if (nesting == 1)
		{
			ResultArray	   *array;

			array = (ResultArray *) palloc(sizeof(ResultArray));
			array->index = 0;
			return array;
		}
	}

	return dummy_p;
}

static void *
create_data(int type, const char *data, uint32_t length)
{
	switch(type)
	{
	case JSON_STRING:
	case JSON_INT:
	case JSON_FLOAT:
		return (void *) data;

	case JSON_NULL:
	case JSON_TRUE:
	case JSON_FALSE:
		break;
	}

	return NULL;
}

#define TWEETCOPY(structure, key, obj) \
do{ \
	Tweet  *tweet = (Tweet *) (structure); \
	int		len = strlen((char *) (obj)); \
	if (len > 0) \
	{ \
		tweet->key = (char *) palloc(sizeof(char) * (len + 1)); \
		strcpy(tweet->key, (obj)); \
	} \
} while(0)

static int
append(void *structure, char *key, uint32_t key_length, void *obj)
{
	if (key != NULL)
	{
		/* discard any unnecessary data */
		if (structure == dummy_p)
			return 0;
		if (strcmp(key, "results") == 0)
		{
			/* root.results = array; */
			((ResultRoot *) structure)->results = (ResultArray *) obj;
		}
		else if (strcmp(key, "id") == 0 && obj)
			TWEETCOPY(structure, id, obj);
		else if (strcmp(key, "text") == 0 && obj)
			TWEETCOPY(structure, text, obj);
		else if(strcmp(key, "from_user") == 0 && obj)
			TWEETCOPY(structure, from_user, obj);
		else if(strcmp(key, "from_user_id") == 0 && obj)
			TWEETCOPY(structure, from_user_id, obj);
		else if(strcmp(key, "to_user") == 0 && obj)
			TWEETCOPY(structure, to_user, obj);
		else if(strcmp(key, "to_user_id") == 0 && obj)
			TWEETCOPY(structure, to_user_id, obj);
		else if(strcmp(key, "iso_language_code") == 0)
			TWEETCOPY(structure, iso_language_code, obj);
		else if(strcmp(key, "source") == 0 && obj)
			TWEETCOPY(structure, source, obj);
		else if(strcmp(key, "profile_image_url") == 0 && obj)
			TWEETCOPY(structure, profile_image_url, obj);
		else if(strcmp(key, "created_at") == 0 && obj)
			TWEETCOPY(structure, created_at, obj);
	}
	else
	{
		/*
		 * array.push(tweet);
		 * an array that is not dummy_p must be root.results
		 */
		ResultArray *array = (ResultArray *) structure;

		if (array != dummy_p)
			array->elements[array->index++] = (Tweet *) obj;
	}
	return 0;
}
