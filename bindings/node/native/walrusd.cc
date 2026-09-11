// walrusd Node-API addon: a thin, data-oriented shim over the walrusd C ABI.
// It exposes exactly the runtime surface (read/write/version/close) with
// JSON envelopes. No raw SQLite pointers cross into JavaScript (spec §11,
// §18).
//
// write/read/readDsn are ASYNC (napi_async_work returning a Promise): the Go
// core blocks for lease acquisition, SQLite, and LTX flush — running that on
// the JS thread would stall the event loop for seconds. init/version/close/
// load stay synchronous (fast, non-blocking).
#include <node_api.h>
#include <cstring>
#include <string>
#include <dlfcn.h>

// ABI symbols resolved from the shared library at load time.
typedef const char* (*walrusd_call_t)(uint64_t handle, const char* req, int n, long long deadline_ms);
typedef const char* (*walrusd_version_t)();
typedef const char* (*walrusd_close_t)(uint64_t handle);
typedef void (*walrusd_free_t)(char* p);
typedef uint64_t (*walrusd_init_t)(const char* req, int n);

static walrusd_call_t walrusd_write_sym;
static walrusd_call_t walrusd_read_sym;
static walrusd_call_t walrusd_read_dsn_sym;
static walrusd_version_t walrusd_version_sym;
static walrusd_close_t walrusd_close_sym;
static walrusd_free_t walrusd_free_sym;
static walrusd_init_t walrusd_init_sym;

static napi_value walrusd_throw(napi_env env, const char* message) {
  napi_value err;
  napi_create_string_utf8(env, message, NAPI_AUTO_LENGTH, &err /* reuse as msg */);
  napi_value msg;
  napi_create_string_utf8(env, message, NAPI_AUTO_LENGTH, &msg);
  napi_create_error(env, nullptr, msg, &err);
  napi_throw(env, err);
  return nullptr;
}

static napi_value to_js_string_and_free(napi_env env, const char* res) {
  if (res == nullptr) {
    return walrusd_throw(env, "walrusd: null response");
  }
  napi_value out;
  napi_create_string_utf8(env, res, NAPI_AUTO_LENGTH, &out);
  walrusd_free_sym(const_cast<char*>(res));
  return out;
}

// ---- async call machinery (write/read/readDsn) ----

struct CallWork {
  napi_async_work work = nullptr;
  napi_deferred deferred;
  walrusd_call_t sym = nullptr;
  uint64_t handle = 0;
  std::string req;
  long long deadline_ms = 0;
  std::string result;
  std::string error;
};

static void call_execute(napi_env env, void* data) {
  (void)env;
  CallWork* w = static_cast<CallWork*>(data);
  const char* res = w->sym(w->handle, w->req.data(), static_cast<int>(w->req.size()), w->deadline_ms);
  if (res == nullptr) {
    w->error = "walrusd: null response from core";
    return;
  }
  w->result.assign(res);
  walrusd_free_sym(const_cast<char*>(res));
}

static void call_complete(napi_env env, napi_status status, void* data) {
  CallWork* w = static_cast<CallWork*>(data);
  if (status != napi_ok) {
    napi_value err;
    napi_create_string_utf8(env, "walrusd: async work failed", NAPI_AUTO_LENGTH, &err);
    napi_value rej;
    napi_create_error(env, nullptr, err, &rej);
    napi_reject_deferred(env, w->deferred, rej);
  } else if (!w->error.empty()) {
    napi_value msg;
    napi_create_string_utf8(env, w->error.c_str(), NAPI_AUTO_LENGTH, &msg);
    napi_value rej;
    napi_create_error(env, nullptr, msg, &rej);
    napi_reject_deferred(env, w->deferred, rej);
  } else {
    napi_value out;
    napi_create_string_utf8(env, w->result.c_str(), NAPI_AUTO_LENGTH, &out);
    napi_resolve_deferred(env, w->deferred, out);
  }
  napi_delete_async_work(env, w->work);
  delete w;
}

static napi_value walrusd_call_async(napi_env env, napi_callback_info info, walrusd_call_t sym, const char* resource) {
  size_t argc = 3;
  napi_value args[3];
  napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
  if (argc < 3) {
    return walrusd_throw(env, "walrusd: call requires (handle, request, deadlineMs)");
  }
  int64_t handleSigned = 0;
  napi_get_value_int64(env, args[0], &handleSigned);
  size_t len = 0;
  napi_get_value_string_utf8(env, args[1], nullptr, 0, &len);
  std::string req(len, '\0');
  napi_get_value_string_utf8(env, args[1], req.data(), len + 1, &len);
  double deadline = 0;
  napi_get_value_double(env, args[2], &deadline);

  CallWork* w = new (std::nothrow) CallWork();
  if (w == nullptr) {
    return walrusd_throw(env, "walrusd: out of memory");
  }
  w->sym = sym;
  w->handle = static_cast<uint64_t>(handleSigned);
  w->req = req;
  w->deadline_ms = static_cast<long long>(deadline);

  napi_value promise;
  if (napi_create_promise(env, &w->deferred, &promise) != napi_ok) {
    delete w;
    return walrusd_throw(env, "walrusd: cannot create promise");
  }
  napi_value resource_name;
  napi_create_string_utf8(env, resource, NAPI_AUTO_LENGTH, &resource_name);
  if (napi_create_async_work(env, nullptr, resource_name, call_execute, call_complete, w, &w->work) != napi_ok) {
    delete w;
    return walrusd_throw(env, "walrusd: cannot create async work");
  }
  if (napi_queue_async_work(env, w->work) != napi_ok) {
    napi_delete_async_work(env, w->work);
    delete w;
    return walrusd_throw(env, "walrusd: cannot queue async work");
  }
  return promise;
}

static napi_value walrusd_version(napi_env env, napi_callback_info info) {
  const char* res = walrusd_version_sym();
  return to_js_string_and_free(env, res);
}

static napi_value walrusd_init(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
  if (argc < 1) {
    return walrusd_throw(env, "walrusd: init requires a config string");
  }
  size_t len = 0;
  napi_get_value_string_utf8(env, args[0], nullptr, 0, &len);
  std::string req(len, '\0');
  napi_get_value_string_utf8(env, args[0], req.data(), len + 1, &len);
  uint64_t h = walrusd_init_sym(req.data(), static_cast<int>(req.size()));
  if (h == 0) {
    return walrusd_throw(env, "walrusd: init failed");
  }
  napi_value out;
  napi_create_int64(env, static_cast<int64_t>(h), &out);
  return out;
}

static napi_value walrusd_write(napi_env env, napi_callback_info info) {
  return walrusd_call_async(env, info, walrusd_write_sym, "walrusd_write");
}

static napi_value walrusd_read(napi_env env, napi_callback_info info) {
  return walrusd_call_async(env, info, walrusd_read_sym, "walrusd_read");
}

static napi_value walrusd_read_dsn(napi_env env, napi_callback_info info) {
  return walrusd_call_async(env, info, walrusd_read_dsn_sym, "walrusd_read_dsn");
}

static napi_value walrusd_close(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
  if (argc < 1) {
    return walrusd_throw(env, "walrusd: close requires a handle");
  }
  int64_t handleSigned = 0;
  napi_get_value_int64(env, args[0], &handleSigned);
  uint64_t handle = static_cast<uint64_t>(handleSigned);
  const char* res = walrusd_close_sym(handle);
  return to_js_string_and_free(env, res);
}

static napi_value walrusd_load(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
  if (argc < 1) {
    return walrusd_throw(env, "walrusd: load requires the shared library path");
  }
  size_t len = 0;
  napi_get_value_string_utf8(env, args[0], nullptr, 0, &len);
  std::string path(len, '\0');
  napi_get_value_string_utf8(env, args[0], path.data(), len + 1, &len);

  void* lib = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!lib) {
    return walrusd_throw(env, dlerror());
  }
  walrusd_write_sym = reinterpret_cast<walrusd_call_t>(dlsym(lib, "walrusd_runtime_write"));
  walrusd_read_sym = reinterpret_cast<walrusd_call_t>(dlsym(lib, "walrusd_runtime_read"));
  walrusd_read_dsn_sym = reinterpret_cast<walrusd_call_t>(dlsym(lib, "walrusd_runtime_read_dsn"));
  walrusd_version_sym = reinterpret_cast<walrusd_version_t>(dlsym(lib, "walrusd_runtime_version"));
  walrusd_close_sym = reinterpret_cast<walrusd_close_t>(dlsym(lib, "walrusd_runtime_close"));
  walrusd_free_sym = reinterpret_cast<walrusd_free_t>(dlsym(lib, "walrusd_free"));
  walrusd_init_sym = reinterpret_cast<walrusd_init_t>(dlsym(lib, "walrusd_runtime_init"));
  if (!walrusd_write_sym || !walrusd_read_sym || !walrusd_read_dsn_sym || !walrusd_version_sym || !walrusd_close_sym || !walrusd_free_sym || !walrusd_init_sym) {
    return walrusd_throw(env, "walrusd: shared library missing required symbols");
  }
  napi_value out;
  napi_get_undefined(env, &out);
  return out;
}

static napi_value Init(napi_env env, napi_value exports) {
  napi_value fn;
#define WALRUSD_EXPORT(name, impl)                       \
  napi_create_function(env, name, NAPI_AUTO_LENGTH, impl, nullptr, &fn); \
  napi_set_named_property(env, exports, name, fn);
  WALRUSD_EXPORT("load", walrusd_load)
  WALRUSD_EXPORT("version", walrusd_version)
  WALRUSD_EXPORT("init", walrusd_init)
  WALRUSD_EXPORT("write", walrusd_write)
  WALRUSD_EXPORT("read", walrusd_read)
  WALRUSD_EXPORT("readDsn", walrusd_read_dsn)
  WALRUSD_EXPORT("close", walrusd_close)
#undef WALRUSD_EXPORT
  return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
