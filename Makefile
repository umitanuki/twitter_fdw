# contrib/twitter_fdw/Makefile

LIBJSON = libjson-0.8
MODULE_big = twitter_fdw
OBJS	= twitter_fdw.o

DATA_built = twitter_fdw.sql 
DATA = uninstall_twitter_fdw.sql 
REGRESS = twitter_fdw
SHLIB_LINK = -L$(LIBJSON) -ljson -lcurl

all:all-libjson

all-libjson:
	$(MAKE) -C $(LIBJSON) all

clean: clean-libjson

clean-libjson:
	$(MAKE) -C $(LIBJSON) clean

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/twitter_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
