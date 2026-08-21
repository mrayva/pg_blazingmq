# pg_blazingmq Makefile
#
# Phase 1: proof-of-linkage build only (pg_blazingmq_link_check()). Links the
# full BDE/NTF/bmq client dependency chain built by BlazingMQ's own
# bin/build-ubuntu.sh - see BMQ_ROOT below.

MODULE_big = pg_blazingmq
OBJS = pg_blazingmq.o

EXTENSION = pg_blazingmq
DATA = pg_blazingmq--0.1.sql pg_blazingmq--0.2.sql pg_blazingmq--0.3.sql \
       pg_blazingmq--0.4.sql pg_blazingmq--0.1--0.2.sql \
       pg_blazingmq--0.2--0.3.sql pg_blazingmq--0.3--0.4.sql

# Sequential on purpose: pg_regress runs each file as its own fresh psql
# connection (so g_session/g_queues start clean per file, matching what
# Phase 3/4 already assume), and 04_subscribe's sink table depends on
# nothing from earlier files - order here is really just "roughly the
# order the phases were built in", not a real dependency chain.
REGRESS = 01_link_check 02_publish_row 03_consume 04_subscribe

# Root of a BlazingMQ checkout already built via bin/build-ubuntu.sh (BDE/NTF
# installed under $(BMQ_ROOT)/include and $(BMQ_ROOT)/lib64; bmq group built
# under $(BMQ_ROOT)/build/blazingmq - the bmq group has no separate install
# step, so its headers/libs are referenced straight from the source/build
# tree). Override on the command line or via env var for a different path:
#   make BMQ_ROOT=/path/to/blazingmq
BMQ_ROOT ?= $(HOME)/blazingmq
BMQ_SRC  := $(BMQ_ROOT)/src/groups/bmq
BMQ_BLD  := $(BMQ_ROOT)/build/blazingmq/src/groups/bmq

# Client-only bmq group packages (confirmed against bmqa.dep's transitive
# closure - no mqb/ needed at all, that's the broker's own internal group).
BMQ_PKGS = bmqa bmqc bmqeval bmqex bmqimp bmqio bmqma bmqp bmqpi bmqscm \
           bmqst bmqstm bmqt bmqtsk bmqu bmqvt

PG_CPPFLAGS = -std=c++23 -fPIC \
    -isystem $(BMQ_ROOT)/include \
    $(foreach pkg,$(BMQ_PKGS),-I$(BMQ_SRC)/$(pkg)) \
    -Ivendor/zerialize/include

# -Wl,--start-group/--end-group sidesteps static-lib link-order pain across
# this many mutually-referencing archives - confirmed working against the
# exact same library set bmqtool.tsk links (verified via build.ninja).
SHLIB_LINK = -lstdc++ \
    -Wl,--start-group \
    $(foreach pkg,$(BMQ_PKGS),$(BMQ_BLD)/lib$(pkg).a) \
    $(BMQ_BLD)/libbmq.a \
    $(BMQ_ROOT)/lib64/opt_exc_mt/libntc.a \
    $(BMQ_ROOT)/lib64/opt_exc_mt/libnts.a \
    $(BMQ_ROOT)/lib64/libbal.a \
    $(BMQ_ROOT)/lib64/libbdl.a \
    $(BMQ_ROOT)/lib64/libbsl.a \
    $(BMQ_ROOT)/lib64/libbbryu.a \
    $(BMQ_ROOT)/lib64/libinteldfp.a \
    $(BMQ_ROOT)/lib64/libpcre2.a \
    -Wl,--end-group \
    -lssl -lcrypto -lz -lzstd -lpthread -ldl -lrt

# Use C++ compiler
CC = g++
CXX = g++

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# PGXS links MODULE_big with the C driver; avoid passing C-only warning flags
# from PostgreSQL's build into that link. C++ compilation uses CXXFLAGS below.
override CFLAGS :=

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<

# `make installcheck` (PGXS's own target, above) assumes a broker is
# already running on tcp://localhost:30114 and pg_blazingmq is already
# `make install`'d. `make test` is the one-command version: starts a
# scratch broker, runs installcheck, always stops the broker after -
# even if installcheck fails, so a failing test run doesn't leak a
# background broker process.
.PHONY: test
test:
	BMQ_ROOT=$(BMQ_ROOT) test/manage_broker.sh start
	$(MAKE) installcheck; status=$$?; \
	BMQ_ROOT=$(BMQ_ROOT) test/manage_broker.sh stop; \
	exit $$status
