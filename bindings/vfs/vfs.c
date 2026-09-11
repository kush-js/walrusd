/* walrusd loadable VFS extension: registers per-database litestream read
 * VFSes on demand via walrusd_vfs_attach(name, url, key, secret) — a plain
 * SQL-callable function usable from any host SQLite (bun:sqlite, CLI). */
#include "binding/sqlite3.h"
#include "binding/sqlite3ext.h"
#include <string.h>
#include <stdlib.h>

/* sqlite3vfs.c (from the psanford module, compiled via cgo) defines sqlite3_api */
extern const sqlite3_api_routines *sqlite3_api;

extern char* WalrusdVFSAttach(const char* name, const char* url, const char* key, const char* secret);

static void walrusd_vfs_attach_impl(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
  if (argc != 4) {
    sqlite3_result_error(ctx, "walrusd_vfs_attach(name, replica_url, access_key_id, secret_access_key) requires 4 arguments", -1);
    return;
  }
  const char* name = (const char*)sqlite3_value_text(argv[0]);
  const char* url = (const char*)sqlite3_value_text(argv[1]);
  const char* key = (const char*)sqlite3_value_text(argv[2]);
  const char* secret = (const char*)sqlite3_value_text(argv[3]);
  if (!name || !url) {
    sqlite3_result_error(ctx, "walrusd_vfs_attach: name and replica_url required", -1);
    return;
  }
  char* err = WalrusdVFSAttach(name, url, key ? key : "", secret ? secret : "");
  if (err) {
    sqlite3_result_error(ctx, err, -1);
    free(err);
    return;
  }
  sqlite3_result_null(ctx);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_walrusdvfs_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;
  rc = sqlite3_create_function(db, "walrusd_vfs_attach", 4, SQLITE_UTF8 | SQLITE_DIRECTONLY, 0, walrusd_vfs_attach_impl, 0, 0);
  if (rc == SQLITE_OK) rc = SQLITE_OK_LOAD_PERMANENTLY;
  return rc;
}
