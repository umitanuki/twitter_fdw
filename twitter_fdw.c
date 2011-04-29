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
#include "parser/parsetree.h"
#include "storage/fd.h"
#include "utils/builtins.h"

#include "curl/curl.h"
#include "libjson/json.h"

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

#define PROCID_TEXTEQ 67

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
static FdwPlan *twitterPlan(Oid foreigntableid,
						PlannerInfo *root,
						RelOptInfo *baserel);
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

	fdwroutine->PlanForeignScan = twitterPlan;
	fdwroutine->ExplainForeignScan = twitterExplain;
	fdwroutine->BeginForeignScan = twitterBegin;
	fdwroutine->IterateForeignScan = twitterIterate;
	fdwroutine->ReScanForeignScan = twitterReScan;
	fdwroutine->EndForeignScan = twitterEnd;
	/* everything else is not needed */

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
 * twitterPlan
 *   Create a FdwPlan, which is empty for now.
 */
static FdwPlan *
twitterPlan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel)
{
	FdwPlan	   *fdwplan;

	fdwplan = makeNode(FdwPlan);
	fdwplan->fdw_private = NIL;

	return fdwplan;
}

/*
 * twitterExplain
 *   Produce extra output for EXPLAIN
 */
static void
twitterExplain(ForeignScanState *node, ExplainState *es)
{
	ExplainPropertyText("Twitter API", "Search", es);
}

/*
 * twitterBegin
 *   Query search API and setup result
 */
static void
twitterBegin(ForeignScanState *node, int eflags)
{
	CURL		   *curl;
	int				ret;
	json_parser		parser;
	json_parser_dom helper;
	ResultRoot	   *root;
	Relation		rel;
	AttInMetadata  *attinmeta;
	TwitterReply   *reply;
	StringInfoData	url;
	char		   *param_q = NULL;

	/*
	 * Do nothing in EXPLAIN
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	initStringInfo(&url);
	appendStringInfoString(&url, "http://search.twitter.com/search.json");

	if (node->ss.ps.plan->qual)
	{
		bool		param_first = true;
		ListCell   *lc;

		foreach (lc, node->ss.ps.qual)
		{
			ExprState	   *state = lfirst(lc);

			char *param = twitter_param((Node *) state->expr,
							node->ss.ss_currentRelation->rd_att);
			if (param)
			{
				if (param_first)
					appendStringInfoChar(&url, '?');
				else
					appendStringInfoChar(&url, '&');
				appendStringInfoString(&url, param);
				if (param[0] == 'q' && param[1] == '=')
					param_q = &param[2];
			}
			else
				elog(ERROR, "Unknown qual");
		}
		node->ss.ps.qual = NIL;
	}

	json_parser_dom_init(&helper, create_structure, create_data, append);
	json_parser_init(&parser, NULL, json_parser_dom_callback, &helper);

	elog(DEBUG1, "requesting %s", url.data);
	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_URL, url.data);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &parser);
	ret = curl_easy_perform(curl);
	curl_easy_cleanup(curl);

	rel = node->ss.ss_currentRelation;
	attinmeta = TupleDescGetAttInMetadata(rel->rd_att);

	root = (ResultRoot *) helper.root_structure;

	/* status != 200, or other similar error */
	if (!root)
		elog(ERROR, "Failed fetching response from %s", url.data);

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
	Tweet			   *tweet = root->results->elements[reply->rownum];
	HeapTuple			tuple;
	Relation			rel = node->ss.ss_currentRelation;
	int					i, natts;
	char			  **values;
	MemoryContext		oldcontext;


	if (!(root->results && reply->rownum < root->results->index))
	{
		ExecClearTuple(slot);
		return slot;
	}

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

			root = (ResultRoot *) palloc(sizeof(ResultRoot));
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
