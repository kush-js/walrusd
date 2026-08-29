// WALrus Node-API addon: a thin, data-oriented shim over the WALrus C ABI.
// It exposes exactly the runtime surface (read/write/version/close) with
// JSON envelopes. No raw SQLite pointers cross into JavaScript (spec §11,
// §18).
#include <node_api.h>
#include <cstring>
#include <string>
#include <dlfcn.h>

// ABI symbols resolved from the shared library at load time.
typedef const char* (*walrus_call_t)(uint64_t handle, const char* req, int n, long long deadline_ms);
typedef const char* (*walrus_version_t)();
typedef const char* (*walrus_close_t)(uint64_t handle);
typedef void (*walrus_free_t)(char* p);
typedef uint64_t (*walrus_init_t)(const char* req, int n);

static walrus_call_t walrus_write_sym;
static walrus_call_t walrus_read_sym;
static walrus_version_t walrus_version_sym;
static walrus_close_t walrus_close_sym;
static walrus_free_t walrus_free_sym;
static walrus_init_t walrus_init_sym;

static napi_value walrus_throw(napi_env env, const char* message) {
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
    return walrus_throw(env, "walrus: null response");
  }
  napi_value out;
  napi_create_string_utf8(env, res, NAPI_AUTO_LENGTH, &out);
  walrus_free_sym(const_cast<char*>(res));
  return out;
}

static napi_value walrus_version(napi_env env, napi_callback_info info) {
  const char* res = walrus_version_sym();
  return to_js_string_and_free(env, res);
}

static napi_value walrus_init(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
  if (argc < 1) {
    return walrus_throw(env, "walrus: init requires a config string");
  }
  size_t len = 0;
  napi_get_value_string_utf8(env, args[0], nullptr, 0, &len);
  std::string req(len, '\0');
  napi_get_value_string_utf8(env, args[0], req.data(), len + 1, &len);
  uint64_t h = walrus_init_sym(req.data(), static_cast<int>(req.size()));
  if (h == 0) {
    return walrus_throw(env, "walrus: init failed");
  }
  napi_value out;
  napi_create_int64(env, static_cast<int64_t>(h), &out);
  return out;
}

static napi_value walrus_call(napi_env env, napi_callback_info info, walrus_call_t sym) {
  size_t argc = 3;
  napi_value args[3];
  napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
  if (argc < 3) {
    return walrus_throw(env, "walrus: call requires (handle, request, deadlineMs)");
  }
  int64_t handleSigned = 0;
  napi_get_value_int64(env, args[0], &handleSigned);
  uint64_t handle = static_cast<uint64_t>(handleSigned);
  size_t len = 0;
  napi_get_value_string_utf8(env, args[1], nullptr, 0, &len);
  std::string req(len, '\0');
  napi_get_value_string_utf8(env, args[1], req.data(), len + 1, &len);
  double deadline = 0;
  napi_get_value_double(env, args[2], &deadline);
  const char* res = sym(handle, req.data(), static_cast<int>(req.size()), static_cast<long long>(deadline));
  return to_js_string_and_free(env, res);
}

static napi_value walrus_write(napi_env env, napi_callback_info info) {
  return walrus_call(env, info, walrus_write_sym);
}

static napi_value walrus_read(napi_env env, napi_callback_info info) {
  return walrus_call(env, info, walrus_read_sym);
}

static napi_value walrus_close(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
  if (argc < 1) {
    return walrus_throw(env, "walrus: close requires a handle");
  }
  int64_t handleSigned = 0;
  napi_get_value_int64(env, args[0], &handleSigned);
  uint64_t handle = static_cast<uint64_t>(handleSigned);
  const char* res = walrus_close_sym(handle);
  return to_js_string_and_free(env, res);
}

static napi_value walrus_load(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, nullptr, nullptr);
  if (argc < 1) {
    return walrus_throw(env, "walrus: load requires the shared library path");
  }
  size_t len = 0;
  napi_get_value_string_utf8(env, args[0], nullptr, 0, &len);
  std::string path(len, '\0');
  napi_get_value_string_utf8(env, args[0], path.data(), len + 1, &len);

  void* lib = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!lib) {
    return walrus_throw(env, dlerror());
  }
  walrus_write_sym = reinterpret_cast<walrus_call_t>(dlsym(lib, "walrus_runtime_write"));
  walrus_read_sym = reinterpret_cast<walrus_call_t>(dlsym(lib, "walrus_runtime_read"));
  walrus_version_sym = reinterpret_cast<walrus_version_t>(dlsym(lib, "walrus_runtime_version"));
  walrus_close_sym = reinterpret_cast<walrus_close_t>(dlsym(lib, "walrus_runtime_close"));
  walrus_free_sym = reinterpret_cast<walrus_free_t>(dlsym(lib, "walrus_free"));
  walrus_init_sym = reinterpret_cast<walrus_init_t>(dlsym(lib, "walrus_runtime_init"));
  if (!walrus_write_sym || !walrus_read_sym || !walrus_version_sym || !walrus_close_sym || !walrus_free_sym || !walrus_init_sym) {
    return walrus_throw(env, "walrus: shared library missing required symbols");
  }
  napi_value out;
  napi_get_undefined(env, &out);
  return out;
}

static napi_value Init(napi_env env, napi_value exports) {
  napi_value fn;
#define WALRUS_EXPORT(name, impl)                       \
  napi_create_function(env, name, NAPI_AUTO_LENGTH, impl, nullptr, &fn); \
  napi_set_named_property(env, exports, name, fn);
  WALRUS_EXPORT("load", walrus_load)
  WALRUS_EXPORT("version", walrus_version)
  WALRUS_EXPORT("init", walrus_init)
  WALRUS_EXPORT("write", walrus_write)
  WALRUS_EXPORT("read", walrus_read)
  WALRUS_EXPORT("close", walrus_close)
#undef WALRUS_EXPORT
  return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
