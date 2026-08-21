/*
 * pg_blazingmq.cpp
 * Phase 1: proof-of-linkage only. pg_blazingmq_link_check() constructs a
 * real bmqa::Session (via bmqt::SessionOptions) without calling start(), so
 * it exercises the full BDE/NTF/bmq symbol chain without requiring a live
 * broker. No publish/consume functionality yet.
 */

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "varatt.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif
}

#include <bmqa_session.h>
#include <bmqt_sessionoptions.h>

#include <sstream>
#include <string>

extern "C" {

PG_FUNCTION_INFO_V1(pg_blazingmq_link_check);

Datum pg_blazingmq_link_check(PG_FUNCTION_ARGS)
{
    text* broker_uri_text = PG_GETARG_TEXT_PP(0);
    std::string broker_uri(VARDATA_ANY(broker_uri_text), VARSIZE_ANY_EXHDR(broker_uri_text));

    BloombergLP::bmqt::SessionOptions options;
    options.setBrokerUri(broker_uri);

    // Constructing (not starting) a real Session exercises the full
    // bmqa/bmqimp/bmqp/.../BDE/NTF symbol chain without connecting to
    // anything.
    BloombergLP::bmqa::Session session(options);

    std::ostringstream out;
    out << "pg_blazingmq link OK: brokerUri=" << options.brokerUri()
        << " numProcessingThreads=" << options.numProcessingThreads();

    PG_RETURN_TEXT_P(cstring_to_text(out.str().c_str()));
}

} // extern "C"
