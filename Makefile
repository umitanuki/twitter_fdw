# contrib/twitter_fdw/Makefile

LIBJSON = libjson-0.8
MODULE_big = twitter_fdw
OBJS	= twitter_fdw.o $(LIBJSON)/json.o
EXTENSION = twitter_fdw
DATA = twitter_fdw--1.1.0.sql

REGRESS = twitter_fdw
SHLIB_LINK = -lcurl

all:all-libjson

all-libjson:
	$(MAKE) -C $(LIBJSON) all

clean: clean-libjson

clean-libjson:
	$(MAKE) -C $(LIBJSON) clean

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
