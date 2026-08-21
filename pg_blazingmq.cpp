/*
 * pg_blazingmq.cpp
 *
 * Phase 1: pg_blazingmq_link_check() - proof-of-linkage only, see README.
 *
 * Phase 2: bmq_publish_row(queue_uri, row, attr_columns) - publishes a
 * Postgres row to a BlazingMQ queue. Chosen columns (or, if attr_columns is
 * NULL, every bool/int2/int4/int8/text-family column) are promoted to
 * typed bmqa::MessageProperties for server-side subscriber filtering via
 * BlazingMQ's own bmqeval expression language; the full row is packed as
 * the message payload via zerialize (msgpack).
 */

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "storage/ipc.h"
#include "utils/tuplestore.h"
#include "nodes/execnodes.h"
#include "miscadmin.h"
#include "varatt.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/dsm.h"
#include "storage/latch.h"
#include "executor/spi.h"
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "catalog/pg_proc.h"
#include "libpq/pqsignal.h"
#include <signal.h>
#include <unistd.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

void _PG_init(void);
PGDLLEXPORT void bmq_subscriber_main(Datum main_arg);
}

#include <bmqa_session.h>
#include <bmqa_queueid.h>
#include <bmqa_openqueuestatus.h>
#include <bmqa_configurequeuestatus.h>
#include <bmqa_messageeventbuilder.h>
#include <bmqa_message.h>
#include <bmqa_messageproperties.h>
#include <bmqa_event.h>
#include <bmqa_messageevent.h>
#include <bmqa_messageiterator.h>
#include <bmqt_sessionoptions.h>
#include <bmqt_uri.h>
#include <bmqt_queueflags.h>
#include <bmqt_queueoptions.h>
#include <bmqt_subscription.h>
#include <bmqt_correlationid.h>
#include <bmqt_messageeventtype.h>
#include <bsls_timeinterval.h>
#include <bdlbb_blob.h>
#include <bdlbb_blobutil.h>

#include <zerialize/zerialize.hpp>
#include <zerialize/dynamic.hpp>
#include <zerialize/protocols/msgpack.hpp>

#include <chrono>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <stdexcept>

namespace z = zerialize;
namespace bmqa = BloombergLP::bmqa;
namespace bmqt = BloombergLP::bmqt;
namespace bsls = BloombergLP::bsls;
namespace bdlbb = BloombergLP::bdlbb;

// --- GUC -------------------------------------------------------------------

static char* g_broker_uri = nullptr;

void _PG_init(void)
{
    DefineCustomStringVariable(
        "blazingmq.broker_uri",
        "BlazingMQ broker URI used by pg_blazingmq functions.",
        NULL,
        &g_broker_uri,
        "tcp://localhost:30114",
        PGC_USERSET,
        0,
        NULL, NULL, NULL);
}

// --- Session / queue-handle lifecycle --------------------------------------
//
// One Session per backend process, lazily started on first use and torn
// down via on_proc_exit. QueueId handles are cached per URI for the life of
// the backend - BlazingMQ owns queue existence/state server-side, so this
// is purely a local handle cache, not something that needs rebuilding.

// BlazingMQ allows only one open handle per (session, queue URI) at all -
// discovered the hard way: opening the same URI a second time from the
// same session, even with different flags (e.g. WRITE then READ), fails
// with ALREADY_OPENED. So there is exactly one queue-handle cache, not
// separate read/write ones: every handle is opened with combined
// e_READ | e_WRITE flags regardless of which entry point (publish or
// consume) touches a given URI first, so either direction can follow.
//
// The subscription expression (if any) is fixed at open time, so the
// cache also remembers which expression each handle was opened with.
// Calling bmq_consume() again on the same URI with a *different*
// expression from the same backend is a clear error rather than silently
// reusing the old filter or paying for a reconfigure round-trip; this is
// a documented first-cut limitation, not a fundamental one.
struct QueueEntry {
    bmqa::QueueId queueId;
    std::string subscriptionExpr; // empty if opened without one
};

static bmqa::Session* g_session = nullptr;
static std::unordered_map<std::string, QueueEntry>* g_queues = nullptr;
static bool g_exit_hook_registered = false;

static void pg_blazingmq_exit_hook(int /*code*/, Datum /*arg*/)
{
    if (g_queues) {
        delete g_queues;
        g_queues = nullptr;
    }
    if (g_session) {
        g_session->stop();
        delete g_session;
        g_session = nullptr;
    }
}

static bmqa::Session& get_session()
{
    if (g_session) return *g_session;

    bmqt::SessionOptions options;
    options.setBrokerUri(g_broker_uri);

    auto session = std::make_unique<bmqa::Session>(options);
    int rc = session->start(bsls::TimeInterval(10));
    if (rc != 0) {
        throw std::runtime_error(
            "failed to start BlazingMQ session against '" +
            std::string(g_broker_uri) + "' (rc=" + std::to_string(rc) + ")");
    }

    g_session = session.release();
    if (!g_exit_hook_registered) {
        on_proc_exit(pg_blazingmq_exit_hook, 0);
        g_exit_hook_registered = true;
    }
    return *g_session;
}

static bmqt::QueueOptions build_queue_options(const std::string& subscriptionExpr)
{
    bmqt::QueueOptions queueOptions;
    if (!subscriptionExpr.empty()) {
        bmqt::Subscription sub;
        sub.setExpression(bmqt::SubscriptionExpression(
            subscriptionExpr, bmqt::SubscriptionExpression::e_VERSION_1));
        bmqt::SubscriptionHandle handle(bmqt::CorrelationId::autoValue());
        bsl::string err;
        if (!queueOptions.addOrUpdateSubscription(&err, handle, sub)) {
            throw std::runtime_error(
                "invalid subscription_expr '" + subscriptionExpr + "': " +
                std::string(err.c_str()));
        }
    }
    return queueOptions;
}

// subscriptionExpr is only meaningful for the read side; pass "" from the
// publish path, which doesn't have one. If a URI already has a cached
// handle open with a *different* subscription_expr, the handle is
// reconfigured (BlazingMQ's own addOrUpdateSubscription semantics) rather
// than erroring - this is what makes "publish, then consume with a
// filter" work within one backend/session, since BlazingMQ only allows
// one open handle per (session, queue URI) at all, discovered the hard
// way (ALREADY_OPENED trying to open the same URI twice with different
// flags). An empty subscriptionExpr request never triggers a
// reconfigure - it just reuses whatever's already there.
static bmqa::QueueId& get_queue(
    bmqa::Session& session, const std::string& uri, const std::string& subscriptionExpr)
{
    if (!g_queues) {
        g_queues = new std::unordered_map<std::string, QueueEntry>();
    }

    auto it = g_queues->find(uri);
    if (it != g_queues->end()) {
        if (!subscriptionExpr.empty() && it->second.subscriptionExpr != subscriptionExpr) {
            bmqa::ConfigureQueueStatus status = session.configureQueueSync(
                &it->second.queueId, build_queue_options(subscriptionExpr));
            if (status.result() != bmqt::ConfigureQueueResult::e_SUCCESS) {
                std::ostringstream err;
                err << "failed to reconfigure BlazingMQ queue '" << uri
                    << "' with subscription_expr '" << subscriptionExpr << "': " << status;
                throw std::runtime_error(err.str());
            }
            it->second.subscriptionExpr = subscriptionExpr;
        }
        return it->second.queueId;
    }

    bmqa::QueueId queueId;
    bsls::Types::Uint64 flags = bmqt::QueueFlags::e_READ | bmqt::QueueFlags::e_WRITE;
    bmqa::OpenQueueStatus status = session.openQueueSync(
        &queueId, bmqt::Uri(uri.c_str()), flags, build_queue_options(subscriptionExpr));
    if (status.result() != bmqt::OpenQueueResult::e_SUCCESS) {
        std::ostringstream err;
        err << "failed to open BlazingMQ queue '" << uri << "': " << status;
        throw std::runtime_error(err.str());
    }

    auto [inserted_it, inserted] = g_queues->emplace(
        uri, QueueEntry{queueId, subscriptionExpr});
    (void)inserted;
    return inserted_it->second.queueId;
}

// --- Row -> {payload, properties} ------------------------------------------

// Attribute (MessageProperty) mapping is intentionally strict: only types
// BlazingMQ's own property/expression system can actually filter on. See
// README's Expression Syntax notes - no float/double, no lists.
static bool set_message_property(
    bmqa::MessageProperties& props, const std::string& name,
    Oid typid, Datum value, bool isnull)
{
    if (isnull) return false; // BlazingMQ properties have no null variant; omit.

    switch (typid) {
        case BOOLOID:
            props.setPropertyAsBool(name, DatumGetBool(value));
            return true;
        case INT2OID:
            props.setPropertyAsShort(name, DatumGetInt16(value));
            return true;
        case INT4OID:
            props.setPropertyAsInt32(name, DatumGetInt32(value));
            return true;
        case INT8OID:
            props.setPropertyAsInt64(name, DatumGetInt64(value));
            return true;
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID: {
            text* t = DatumGetTextPP(value);
            props.setPropertyAsString(
                name, std::string_view(VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t)));
            return true;
        }
        default:
            return false; // caller decides whether "ineligible" is an error
    }
}

// Payload mapping is permissive - the full row, not just filterable
// columns, goes into the message body. Anything without a direct zerialize
// mapping falls back to the column's own text output function.
static z::dyn::Value datum_to_dyn_value(Oid typid, Oid typoutput, Datum value, bool isnull)
{
    if (isnull) return z::dyn::Value(z::dyn::Null{});

    switch (typid) {
        case BOOLOID:
            return z::dyn::Value(DatumGetBool(value));
        case INT2OID:
            return z::dyn::Value(static_cast<int64_t>(DatumGetInt16(value)));
        case INT4OID:
            return z::dyn::Value(static_cast<int64_t>(DatumGetInt32(value)));
        case INT8OID:
            return z::dyn::Value(static_cast<int64_t>(DatumGetInt64(value)));
        case FLOAT4OID:
            return z::dyn::Value(static_cast<double>(DatumGetFloat4(value)));
        case FLOAT8OID:
            return z::dyn::Value(DatumGetFloat8(value));
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID: {
            text* t = DatumGetTextPP(value);
            return z::dyn::Value(std::string_view(VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t)));
        }
        default: {
            char* out = OidOutputFunctionCall(typoutput, value);
            z::dyn::Value v{std::string(out)};
            pfree(out);
            return v;
        }
    }
}

extern "C" {

PG_FUNCTION_INFO_V1(pg_blazingmq_link_check);

Datum pg_blazingmq_link_check(PG_FUNCTION_ARGS)
{
    text* broker_uri_text = PG_GETARG_TEXT_PP(0);
    std::string broker_uri(VARDATA_ANY(broker_uri_text), VARSIZE_ANY_EXHDR(broker_uri_text));

    bmqt::SessionOptions options;
    options.setBrokerUri(broker_uri);

    // Constructing (not starting) a real Session exercises the full
    // bmqa/bmqimp/bmqp/.../BDE/NTF symbol chain without connecting to
    // anything.
    bmqa::Session session(options);

    std::ostringstream out;
    out << "pg_blazingmq link OK: brokerUri=" << options.brokerUri()
        << " numProcessingThreads=" << options.numProcessingThreads();

    PG_RETURN_TEXT_P(cstring_to_text(out.str().c_str()));
}

PG_FUNCTION_INFO_V1(bmq_publish_row);

Datum bmq_publish_row(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) ereport(ERROR, (errmsg("queue_uri must not be null")));
    if (PG_ARGISNULL(1)) ereport(ERROR, (errmsg("row must not be null")));

    text* queue_uri_text = PG_GETARG_TEXT_PP(0);
    std::string queue_uri(VARDATA_ANY(queue_uri_text), VARSIZE_ANY_EXHDR(queue_uri_text));

    HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(1);

    // Explicit attribute list, if given; NULL means "every eligible column".
    std::unordered_set<std::string> requested_attrs;
    bool attrs_explicit = !PG_ARGISNULL(2);
    if (attrs_explicit) {
        ArrayType* arr = PG_GETARG_ARRAYTYPE_P(2);
        Datum* elems;
        bool* nulls;
        int nelems;
        deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT, &elems, &nulls, &nelems);
        for (int i = 0; i < nelems; ++i) {
            if (nulls[i]) continue;
            text* t = DatumGetTextPP(elems[i]);
            requested_attrs.emplace(VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
        }
    }

    Oid tupType = HeapTupleHeaderGetTypeId(rec);
    int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
    TupleDesc tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

    HeapTupleData tmptup;
    tmptup.t_len = HeapTupleHeaderGetDatumLength(rec);
    ItemPointerSetInvalid(&tmptup.t_self);
    tmptup.t_tableOid = InvalidOid;
    tmptup.t_data = rec;

    int natts = tupdesc->natts;
    std::vector<Datum> values(natts);
    std::vector<uint8_t> isnulls_storage(natts);
    bool* isnulls = reinterpret_cast<bool*>(isnulls_storage.data());
    heap_deform_tuple(&tmptup, tupdesc, values.data(), isnulls);

    try {
        bmqa::MessageProperties props;
        z::dyn::Value::Map row_map;
        row_map.reserve(natts);

        for (int i = 0; i < natts; ++i) {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            if (attr->attisdropped) continue;

            std::string colname(NameStr(attr->attname));
            Datum value = values[i];
            bool isnull = isnulls[i];

            bool want_attr = attrs_explicit
                ? requested_attrs.contains(colname)
                : true;

            if (want_attr) {
                bool eligible = set_message_property(props, colname, attr->atttypid, value, isnull);
                if (!eligible && attrs_explicit) {
                    ReleaseTupleDesc(tupdesc);
                    ereport(ERROR,
                            (errmsg("column \"%s\" (type %u) cannot be a BlazingMQ message "
                                    "attribute - only bool/int2/int4/int8/text-family columns "
                                    "are supported (see README's Expression Syntax notes)",
                                    colname.c_str(), attr->atttypid)));
                }
            }

            Oid typoutput; bool typisvarlena;
            getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);
            row_map.emplace_back(colname, datum_to_dyn_value(attr->atttypid, typoutput, value, isnull));
        }

        z::ZBuffer payload = z::serialize<z::MsgPack>(z::dyn::Value::map(std::move(row_map)));

        bmqa::Session& session = get_session();
        bmqa::QueueId& queueId = get_queue(session, queue_uri, "");

        bmqa::MessageEventBuilder builder;
        session.loadMessageEventBuilder(&builder);

        bmqa::Message& msg = builder.startMessage();
        msg.setDataRef(reinterpret_cast<const char*>(payload.data()),
                        static_cast<size_t>(payload.size()));
        if (props.numProperties() > 0) {
            msg.setPropertiesRef(&props);
        }

        bmqt::EventBuilderResult::Enum pack_rc = builder.packMessage(queueId);
        if (pack_rc != bmqt::EventBuilderResult::e_SUCCESS) {
            ReleaseTupleDesc(tupdesc);
            ereport(ERROR, (errmsg("failed to pack BlazingMQ message for queue '%s' (rc=%d)",
                                    queue_uri.c_str(), static_cast<int>(pack_rc))));
        }

        int post_rc = session.post(builder.messageEvent());
        if (post_rc != 0) {
            ReleaseTupleDesc(tupdesc);
            ereport(ERROR, (errmsg("failed to post BlazingMQ message to queue '%s' (rc=%d)",
                                    queue_uri.c_str(), post_rc)));
        }
    } catch (const std::exception& ex) {
        ReleaseTupleDesc(tupdesc);
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                 errmsg("bmq_publish_row failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        ReleaseTupleDesc(tupdesc);
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                 errmsg("bmq_publish_row failed with unknown exception")));
    }

    ReleaseTupleDesc(tupdesc);
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(bmq_consume);

Datum bmq_consume(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) ereport(ERROR, (errmsg("queue_uri must not be null")));

    text* queue_uri_text = PG_GETARG_TEXT_PP(0);
    std::string queue_uri(VARDATA_ANY(queue_uri_text), VARSIZE_ANY_EXHDR(queue_uri_text));

    std::string subscription_expr;
    if (!PG_ARGISNULL(1)) {
        text* t = PG_GETARG_TEXT_PP(1);
        subscription_expr.assign(VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
    }

    int32 max_messages = PG_ARGISNULL(2) ? 1 : PG_GETARG_INT32(2);
    int32 timeout_ms = PG_ARGISNULL(3) ? 1000 : PG_GETARG_INT32(3);
    if (max_messages < 1) ereport(ERROR, (errmsg("max_messages must be >= 1")));
    if (timeout_ms < 0) ereport(ERROR, (errmsg("timeout_ms must be >= 0")));

    ReturnSetInfo* rsinfo = (ReturnSetInfo*) fcinfo->resultinfo;
    if (!rsinfo || !(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errmsg("bmq_consume called in a context that cannot accept a set")));
    }
    rsinfo->returnMode = SFRM_Materialize;

    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    Tuplestorestate* tupstore = tuplestore_begin_heap(false, false, work_mem);
    TupleDesc tupdesc = CreateTemplateTupleDesc(1);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "payload", BYTEAOID, -1, 0);
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    MemoryContextSwitchTo(oldcontext);

    try {
        bmqa::Session& session = get_session();
        bmqa::QueueId& queueId = get_queue(session, queue_uri, subscription_expr);

        auto deadline = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(timeout_ms);
        int32 received = 0;

        while (received < max_messages) {
            auto remaining = deadline - std::chrono::steady_clock::now();
            auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(remaining).count();
            if (remaining_ms <= 0) break;

            bmqa::Event event = session.nextEvent(
                bsls::TimeInterval(static_cast<double>(remaining_ms) / 1000.0));

            if (!event.isMessageEvent()) continue; // session/timeout/other event - keep polling

            bmqa::MessageEvent msgEvent = event.messageEvent();
            if (msgEvent.type() != bmqt::MessageEventType::e_PUSH) continue;

            bmqa::MessageIterator msgIter = msgEvent.messageIterator();
            while (msgIter.nextMessage() && received < max_messages) {
                const bmqa::Message& msg = msgIter.message();

                int dataSize = msg.dataSize();
                bytea* result = (bytea*) palloc(VARHDRSZ + dataSize);
                SET_VARSIZE(result, VARHDRSZ + dataSize);
                if (dataSize > 0) {
                    bdlbb::Blob blob;
                    msg.getData(&blob);
                    bdlbb::BlobUtil::copy(VARDATA(result), blob, 0, dataSize);
                }

                Datum values[1] = {PointerGetDatum(result)};
                bool nulls[1] = {false};
                tuplestore_putvalues(tupstore, tupdesc, values, nulls);
                ++received;

                int confirm_rc = session.confirmMessage(msg);
                if (confirm_rc != 0) {
                    ereport(WARNING,
                            (errmsg("failed to confirm BlazingMQ message on queue '%s' (rc=%d)",
                                    queue_uri.c_str(), confirm_rc)));
                }
            }
            (void)queueId;
        }
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                 errmsg("bmq_consume failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                 errmsg("bmq_consume failed with unknown exception")));
    }

    return (Datum) 0;
}

// --- Phase 4: push-consume via a dynamic background worker -----------------
//
// bmq_subscribe() hands off a small fixed-size config struct to a freshly
// registered dynamic background worker via a Dynamic Shared Memory (DSM)
// segment - deliberately *not* a custom shmem_request_hook-managed
// structure, since that requires shared_preload_libraries plus a server
// restart to take effect. DSM segments need neither: they're created at
// runtime by any backend and (once dsm_pin_segment()'d) outlive the
// creating backend, which is exactly the "one-shot handoff to a worker
// that then runs independently" shape this needs.
//
// The worker's PID doubles as the subscription handle - no separate
// registry is needed since Postgres already exposes every background
// worker in pg_stat_activity (as backend_type = bgw_type, set below),
// which is also what bmq_unsubscribe() uses to confirm a PID is actually
// one of this extension's workers before signaling it.

struct SubscriberConfig {
    Oid dbid;
    Oid roleid;
    Oid callback_fn;
    char queue_uri[512];
    char subscription_expr[512]; // empty = no filter
};

static const char* kSubscriberBgwType = "pg_blazingmq subscriber";

// Checks callback_fn exists and takes exactly one bytea argument. Doesn't
// check the return type - it's discarded either way (OidFunctionCall1's
// result is ignored in the worker), so any return type is harmless.
static void validate_callback_fn(Oid callback_fn)
{
    HeapTuple tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(callback_fn));
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errmsg("callback function with OID %u does not exist", callback_fn)));
    }
    Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(tup);
    bool ok = (proc->pronargs == 1) && (proc->proargtypes.values[0] == BYTEAOID);
    ReleaseSysCache(tup);
    if (!ok) {
        ereport(ERROR,
                (errmsg("callback function must take exactly one \"bytea\" argument")));
    }
}

extern "C" {

PG_FUNCTION_INFO_V1(bmq_subscribe);

Datum bmq_subscribe(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) ereport(ERROR, (errmsg("queue_uri must not be null")));
    if (PG_ARGISNULL(1)) ereport(ERROR, (errmsg("callback_fn must not be null")));

    text* queue_uri_text = PG_GETARG_TEXT_PP(0);
    std::string queue_uri(VARDATA_ANY(queue_uri_text), VARSIZE_ANY_EXHDR(queue_uri_text));
    Oid callback_fn = PG_GETARG_OID(1);

    std::string subscription_expr;
    if (!PG_ARGISNULL(2)) {
        text* t = PG_GETARG_TEXT_PP(2);
        subscription_expr.assign(VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
    }

    validate_callback_fn(callback_fn);

    if (queue_uri.size() >= sizeof(SubscriberConfig::queue_uri)) {
        ereport(ERROR, (errmsg("queue_uri is too long (max %zu bytes)",
                                sizeof(SubscriberConfig::queue_uri) - 1)));
    }
    if (subscription_expr.size() >= sizeof(SubscriberConfig::subscription_expr)) {
        ereport(ERROR, (errmsg("subscription_expr is too long (max %zu bytes)",
                                sizeof(SubscriberConfig::subscription_expr) - 1)));
    }

    dsm_segment* seg = dsm_create(sizeof(SubscriberConfig), 0);
    SubscriberConfig* cfg = (SubscriberConfig*) dsm_segment_address(seg);
    cfg->dbid = MyDatabaseId;
    cfg->roleid = GetUserId();
    cfg->callback_fn = callback_fn;
    strcpy(cfg->queue_uri, queue_uri.c_str());
    strcpy(cfg->subscription_expr, subscription_expr.c_str());

    // Outlive this backend - the worker attaches independently and this
    // call returns well before the subscription itself ends.
    dsm_pin_segment(seg);

    BackgroundWorker worker;
    memset(&worker, 0, sizeof(worker));
    snprintf(worker.bgw_name, BGW_MAXLEN, "%s", kSubscriberBgwType);
    snprintf(worker.bgw_type, BGW_MAXLEN, "%s", kSubscriberBgwType);
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    snprintf(worker.bgw_library_name, MAXPGPATH, "pg_blazingmq");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "bmq_subscriber_main");
    worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
    worker.bgw_notify_pid = MyProcPid;

    BackgroundWorkerHandle* handle;
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
        dsm_detach(seg);
        ereport(ERROR,
                (errmsg("failed to register BlazingMQ subscriber background worker "
                        "(max_worker_processes may be exhausted)")));
    }

    pid_t pid;
    BgwHandleStatus status = WaitForBackgroundWorkerStartup(handle, &pid);
    if (status != BGWH_STARTED) {
        dsm_detach(seg);
        ereport(ERROR,
                (errmsg("BlazingMQ subscriber background worker failed to start "
                        "(status=%d) - check the server log", (int) status)));
    }

    dsm_detach(seg); // the worker has its own attachment now; pinned, so this is safe
    PG_RETURN_INT32((int32) pid);
}

PG_FUNCTION_INFO_V1(bmq_unsubscribe);

Datum bmq_unsubscribe(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) ereport(ERROR, (errmsg("worker_pid must not be null")));
    int32 pid = PG_GETARG_INT32(0);

    bool is_ours = false;
    SPI_connect();
    Oid argtypes[2] = {INT4OID, TEXTOID};
    Datum argvalues[2] = {Int32GetDatum(pid), CStringGetTextDatum(kSubscriberBgwType)};
    int rc = SPI_execute_with_args(
        "SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND backend_type = $2",
        2, argtypes, argvalues, nullptr, true, 1);
    if (rc == SPI_OK_SELECT && SPI_processed > 0) is_ours = true;
    SPI_finish();

    if (!is_ours) {
        ereport(ERROR,
                (errmsg("pid %d is not an active pg_blazingmq subscriber worker", pid)));
    }

    if (kill(pid, SIGTERM) != 0) {
        ereport(WARNING, (errmsg("failed to signal worker pid %d: %m", pid)));
        PG_RETURN_BOOL(false);
    }
    PG_RETURN_BOOL(true);
}

void bmq_subscriber_main(Datum main_arg)
{
    dsm_segment* seg = dsm_attach(DatumGetUInt32(main_arg));
    if (!seg) {
        ereport(FATAL, (errmsg("pg_blazingmq subscriber: failed to attach DSM segment")));
    }
    SubscriberConfig cfg = *(SubscriberConfig*) dsm_segment_address(seg);
    dsm_detach(seg); // config is copied onto our own stack; done with the segment

    std::string queue_uri(cfg.queue_uri);
    std::string subscription_expr(cfg.subscription_expr);
    Oid callback_fn = cfg.callback_fn;

    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnectionByOid(cfg.dbid, cfg.roleid, 0);

    elog(LOG, "pg_blazingmq subscriber started for queue '%s'", queue_uri.c_str());

    try {
        bmqa::Session& session = get_session();
        get_queue(session, queue_uri, subscription_expr);

        while (!ShutdownRequestPending) {
            CHECK_FOR_INTERRUPTS();

            // Short poll so SIGTERM is noticed promptly - session.nextEvent()
            // itself doesn't know about Postgres's shutdown signal.
            bmqa::Event event = session.nextEvent(bsls::TimeInterval(1, 0));
            if (!event.isMessageEvent()) continue;

            bmqa::MessageEvent msgEvent = event.messageEvent();
            if (msgEvent.type() != bmqt::MessageEventType::e_PUSH) continue;

            bmqa::MessageIterator msgIter = msgEvent.messageIterator();
            while (msgIter.nextMessage()) {
                const bmqa::Message& msg = msgIter.message();

                int dataSize = msg.dataSize();
                bytea* payload = (bytea*) palloc(VARHDRSZ + dataSize);
                SET_VARSIZE(payload, VARHDRSZ + dataSize);
                if (dataSize > 0) {
                    bdlbb::Blob blob;
                    msg.getData(&blob);
                    bdlbb::BlobUtil::copy(VARDATA(payload), blob, 0, dataSize);
                }

                bool callback_ok = true;
                SetCurrentStatementStartTimestamp();
                StartTransactionCommand();
                SPI_connect();
                PushActiveSnapshot(GetTransactionSnapshot());

                PG_TRY();
                {
                    OidFunctionCall1(callback_fn, PointerGetDatum(payload));
                }
                PG_CATCH();
                {
                    ErrorData* edata = CopyErrorData();
                    FlushErrorState();
                    callback_ok = false;
                    PopActiveSnapshot();
                    SPI_finish();
                    AbortCurrentTransaction();
                    ereport(WARNING,
                            (errmsg("pg_blazingmq subscriber: callback failed for queue "
                                    "'%s', message left unconfirmed: %s",
                                    queue_uri.c_str(), edata->message)));
                    FreeErrorData(edata);
                }
                PG_END_TRY();

                if (callback_ok) {
                    PopActiveSnapshot();
                    SPI_finish();
                    CommitTransactionCommand();
                    // Only confirm on success - at-least-once delivery: a
                    // failed callback leaves the message unconfirmed, so
                    // BlazingMQ will redeliver it.
                    session.confirmMessage(msg);
                }

                if (ShutdownRequestPending) break;
            }
        }
    } catch (const std::exception& ex) {
        ereport(LOG,
                (errmsg("pg_blazingmq subscriber for queue '%s' exiting on error: %s",
                        queue_uri.c_str(), ex.what())));
    }

    elog(LOG, "pg_blazingmq subscriber stopping for queue '%s'", queue_uri.c_str());
    proc_exit(0);
}

} // extern "C" (Phase 4 block, opened above validate_callback_fn)

} // extern "C" (main block, opened before pg_blazingmq_link_check)
