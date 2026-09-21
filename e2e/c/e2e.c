/* walrusd end-to-end check for the raw C ABI against real object storage
 * (MinIO/S3) and a real Redis/Valkey lease store.
 *
 * The C ABI is what every other binding loads: JSON envelopes in, JSON
 * envelopes out, six exported functions (spec §11). The full loop for one
 * fresh database: a read before any write (no lease), a schema write, an
 * insert write, an idempotent retry, and read-after-write. Around every step
 * the lease key is inspected directly in Redis (plain RESP over a socket) to
 * prove the runtime alone acquires and releases the lease and that reads
 * never touch it.
 *
 * Build and run (the shared library is the one the Node addon loads):
 *
 *   zig cc e2e/c/e2e.c -o /tmp/walrusd-e2e-c \
 *     -L bindings/node/lib -lwalrusd -Wl,-rpath,$PWD/bindings/node/lib
 *   /tmp/walrusd-e2e-c
 *
 * Configuration (all optional):
 *
 *   WALRUSD_E2E_ENDPOINT, WALRUSD_E2E_REGION, WALRUSD_E2E_BUCKET,
 *   WALRUSD_E2E_ACCESS_KEY, WALRUSD_E2E_SECRET_KEY, WALRUSD_E2E_REDIS,
 *   WALRUSD_E2E_ROOT_PREFIX (object-storage namespace, default "walrusd-e2e")
 *
 * WALRUSD_E2E_ROOT_PREFIX is the only per-deployment knob: the per-run
 * database ID lives under it as "users/<library>_<random>" and never repeats
 * the prefix. Every language's e2e script follows this layout.
 */

#include <netdb.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* C ABI (spec §11). */
extern const char *walrusd_runtime_version(void);
extern uint64_t walrusd_runtime_init(const char *req, int n);
extern const char *walrusd_runtime_write(uint64_t h, const char *req, int n, long long deadline_ms);
extern const char *walrusd_runtime_read(uint64_t h, const char *req, int n, long long deadline_ms);
extern const char *walrusd_runtime_close(uint64_t h);
extern void walrusd_free(char *p);

#define REQUEST_TIMEOUT_MS 20000

static char endpoint[256];
static char region[64];
static char bucket[128];
static char access_key[128];
static char secret_key[128];
static char redis_host[128];
static char redis_port[16];
static char root_prefix[128];
static char database_id[192];
static char lease_key[384];
static char descriptor[1024];

static void fail(const char *step, const char *detail) {
    fprintf(stderr, "FAIL c: %s: %s\n", step, detail);
    exit(1);
}

static void copy_env(char *dest, size_t size, const char *name, const char *fallback) {
    const char *value = getenv(name);
    snprintf(dest, size, "%s", value && *value ? value : fallback);
}

static long long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* ---- envelope helpers (no JSON parser here: the envelope is flat) ---- */

static int envelope_ok(const char *response, const char *step) {
    if (response == NULL) {
        fail(step, "null response from the core");
    }
    if (strstr(response, "\"ok\":true") == NULL) {
        fprintf(stderr, "FAIL c: %s: %s\n", step, response);
        exit(1);
    }
    return 1;
}

/* Extracts the value of a flat numeric field: "key":value. */
static long extract_number(const char *json, const char *key, const char *step) {
    char needle[128];
    snprintf(needle, sizeof needle, "\"%s\":", key);
    const char *start = strstr(json, needle);
    if (start == NULL) {
        fail(step, "field not found in response");
    }
    return strtol(start + strlen(needle), NULL, 10);
}

/* Extracts the value of a flat string field: "key":"value". */
static void extract_string(const char *json, const char *key, char *out, size_t size, const char *step) {
    char needle[128];
    snprintf(needle, sizeof needle, "\"%s\":\"", key);
    const char *start = strstr(json, needle);
    if (start == NULL) {
        fail(step, "field not found in response");
    }
    start += strlen(needle);
    const char *end = strchr(start, '"');
    if (end == NULL) {
        fail(step, "unterminated field in response");
    }
    size_t length = (size_t)(end - start);
    if (length >= size) {
        fail(step, "field too long for buffer");
    }
    memcpy(out, start, length);
    out[length] = '\0';
}

/* ---- RESP over a socket (inline commands; replies are simple/bulk) ---- */

static int redis_connect(void) {
    struct addrinfo hints;
    struct addrinfo *res = NULL;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(redis_host, redis_port, &hints, &res) != 0) {
        return -1;
    }
    int fd = -1;
    for (struct addrinfo *ai = res; ai != NULL; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) {
            continue;
        }
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
            break;
        }
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    return fd;
}

static int write_all(int fd, const char *buffer, size_t length) {
    size_t written = 0;
    while (written < length) {
        ssize_t n = write(fd, buffer + written, length - written);
        if (n <= 0) {
            return -1;
        }
        written += (size_t)n;
    }
    return 0;
}

static int read_exact(int fd, char *buffer, size_t length) {
    size_t read_bytes = 0;
    while (read_bytes < length) {
        ssize_t n = read(fd, buffer + read_bytes, length - read_bytes);
        if (n <= 0) {
            return -1;
        }
        read_bytes += (size_t)n;
    }
    return 0;
}

static int read_line(int fd, char *buffer, size_t size) {
    size_t length = 0;
    for (;;) {
        char c;
        if (read_exact(fd, &c, 1) < 0) {
            return -1;
        }
        if (c == '\n') {
            break;
        }
        if (length + 1 < size) {
            buffer[length++] = c;
        }
    }
    if (length > 0 && buffer[length - 1] == '\r') {
        length--;
    }
    buffer[length] = '\0';
    return 0;
}

/* Sends one inline command and returns its reply (bulk payload for '$'). */
static char *redis_command(const char *command) {
    int fd = redis_connect();
    if (fd < 0) {
        return NULL;
    }
    char line[512];
    snprintf(line, sizeof line, "%s\r\n", command);
    if (write_all(fd, line, strlen(line)) < 0) {
        close(fd);
        return NULL;
    }
    char header[256];
    if (read_line(fd, header, sizeof header) < 0) {
        close(fd);
        return NULL;
    }
    if (header[0] == '-') {
        fprintf(stderr, "redis error: %s\n", header + 1);
        close(fd);
        return NULL;
    }
    char *reply = NULL;
    if (header[0] == '$') {
        long length = strtol(header + 1, NULL, 10);
        if (length < 0) {
            reply = strdup("");
        } else {
            reply = malloc((size_t)length + 1);
            if (reply != NULL && read_exact(fd, reply, (size_t)length + 2) == 0) {
                reply[length] = '\0';
            }
        }
    } else {
        reply = strdup(header);
    }
    close(fd);
    return reply;
}

/* ---- lease assertions ---- */

static char *lease_data(const char *step) {
    char command[512];
    snprintf(command, sizeof command, "HGET %s data", lease_key);
    char *reply = redis_command(command);
    if (reply == NULL || reply[0] == '\0') {
        fail(step, "lease record not found in redis");
    }
    return reply;
}

static void require_lease_absent(const char *step) {
    char command[512];
    snprintf(command, sizeof command, "EXISTS %s", lease_key);
    char *reply = redis_command(command);
    if (reply == NULL || strcmp(reply, ":0") != 0) {
        fail(step, "lease key exists; the operation took a lease it should not have");
    }
    free(reply);
}

static void require_lease_released(const char *step) {
    char *data = lease_data(step);
    if (strstr(data, "\"state\":\"released\"") == NULL) {
        printf("%s\n", data);
        fail(step, "lease state is not released");
    }
    free(data);
}

static void require_lease_unchanged(const char *step, const char *before) {
    char *after = lease_data(step);
    if (strcmp(after, before) != 0) {
        fail(step, "lease record changed; reads must not touch leases");
    }
    free(after);
}

/* ---- ABI calls ---- */

static char *call(int is_write, uint64_t handle, char *request, const char *step) {
    char *response = is_write
        ? (char *)walrusd_runtime_write(handle, request, (int)strlen(request), now_ms() + REQUEST_TIMEOUT_MS)
        : (char *)walrusd_runtime_read(handle, request, (int)strlen(request), now_ms() + REQUEST_TIMEOUT_MS);
    envelope_ok(response, step);
    return response;
}

static void write_request(char *out, size_t size, const char *key, const char *statements) {
    snprintf(out, size, "{\"descriptor\":%s,\"idempotency_key\":\"%s\",\"statements\":[%s]}",
             descriptor, key, statements);
}

static void read_request(char *out, size_t size, const char *sql, const char *params) {
    snprintf(out, size, "{\"descriptor\":%s,\"sql\":\"%s\",\"params\":[%s]}", descriptor, sql, params);
}

int main(void) {
    copy_env(endpoint, sizeof endpoint, "WALRUSD_E2E_ENDPOINT", "http://127.0.0.1:9000");
    copy_env(region, sizeof region, "WALRUSD_E2E_REGION", "us-east-1");
    copy_env(bucket, sizeof bucket, "WALRUSD_E2E_BUCKET", "walrusd-e2e");
    copy_env(access_key, sizeof access_key, "WALRUSD_E2E_ACCESS_KEY", "walrusd");
    copy_env(secret_key, sizeof secret_key, "WALRUSD_E2E_SECRET_KEY", "walrusdsecret");
    copy_env(root_prefix, sizeof root_prefix, "WALRUSD_E2E_ROOT_PREFIX", "walrusd-e2e");

    char redis_addr[160];
    copy_env(redis_addr, sizeof redis_addr, "WALRUSD_E2E_REDIS", "127.0.0.1:6379");
    char *colon = strrchr(redis_addr, ':');
    if (colon == NULL) {
        fail("config", "WALRUSD_E2E_REDIS must be host:port");
    }
    *colon = '\0';
    snprintf(redis_host, sizeof redis_host, "%s", redis_addr);
    snprintf(redis_port, sizeof redis_port, "%s", colon + 1);

    srand((unsigned)time(NULL) ^ (unsigned)getpid());
    snprintf(database_id, sizeof database_id, "users/c_%d%d", rand(), rand());
    snprintf(lease_key, sizeof lease_key, "%s/%s/lease.json", root_prefix, database_id);
    snprintf(descriptor, sizeof descriptor,
             "{\"database_id\":\"%s\",\"storage\":{\"provider\":\"s3\",\"endpoint\":\"%s\","
             "\"region\":\"%s\",\"bucket\":\"%s\",\"root_prefix\":\"%s\"},"
             "\"credentials\":{\"access_key_id\":\"%s\",\"secret_access_key\":\"%s\"}}",
             database_id, endpoint, region, bucket, root_prefix, access_key, secret_key);

    /* Version is the cheapest proof the ABI resolves and the core is alive. */
    const char *version = walrusd_runtime_version();
    envelope_ok(version, "version");
    walrusd_free((char *)version);

    char init_request[512];
    snprintf(init_request, sizeof init_request,
             "{\"owner\":\"e2e-c-owner\",\"config\":{\"request_timeout_ms\":%d,"
             "\"redis_address\":\"%s:%s\"}}",
             REQUEST_TIMEOUT_MS, redis_host, redis_port);
    uint64_t handle = walrusd_runtime_init(init_request, (int)strlen(init_request));
    if (handle == 0) {
        fail("init", "walrusd_runtime_init returned 0");
    }

    /* 1. A read on a database with no flushed state must not take a lease. */
    char request[2048];
    read_request(request, sizeof request, "SELECT count(*) AS n FROM sqlite_master", "");
    char *response = call(0, handle, request, "read before write");
    if (strstr(response, "\"n\":0") == NULL) {
        fail("read before write", "expected an empty database");
    }
    walrusd_free(response);
    require_lease_absent("read before write");
    printf("ok read-before-write: empty database served, no lease created\n");

    /* 2. Schema write: the runtime acquires the lease, commits, flushes to
     * object storage, releases the lease, and only then acknowledges. */
    char schema_txid[64];
    write_request(request, sizeof request, "e2e-schema",
                  "{\"sql\":\"CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)\"}");
    response = call(1, handle, request, "schema write");
    extract_string(response, "txid", schema_txid, sizeof schema_txid, "schema write");
    walrusd_free(response);
    require_lease_released("schema write");
    printf("ok schema write: txid=%s lease released\n", schema_txid);

    /* 3. Insert write with a fresh idempotency key. */
    char value[64];
    char insert_key[64];
    snprintf(value, sizeof value, "value-%d-%d", rand(), rand());
    snprintf(insert_key, sizeof insert_key, "e2e-insert-%d", rand());
    char statements[512];
    snprintf(statements, sizeof statements,
             "{\"sql\":\"INSERT INTO kv (k, v) VALUES (?, ?)\",\"params\":[\"greeting\",\"%s\"]}", value);
    write_request(request, sizeof request, insert_key, statements);
    char insert_txid[64];
    response = call(1, handle, request, "insert write");
    extract_string(response, "txid", insert_txid, sizeof insert_txid, "insert write");
    walrusd_free(response);
    if (strcmp(insert_txid, schema_txid) <= 0) {
        fail("insert write", "txid did not advance past the schema write");
    }
    char *released = lease_data("insert write");
    if (strstr(released, "\"state\":\"released\"") == NULL) {
        fail("insert write", "lease state is not released");
    }
    if (extract_number(released, "epoch", "insert write") < 2) {
        fail("insert write", "epoch did not advance past the schema write");
    }
    printf("ok insert write: txid=%s epoch=%ld lease released\n", insert_txid,
           extract_number(released, "epoch", "insert write"));
    free(released);

    /* 4. Retrying the same idempotency key is deduplicated: same TXID, no
     * second mutation, lease still ends released. */
    char retry_txid[64];
    write_request(request, sizeof request, insert_key, statements);
    response = call(1, handle, request, "idempotent retry");
    extract_string(response, "txid", retry_txid, sizeof retry_txid, "idempotent retry");
    walrusd_free(response);
    if (strcmp(retry_txid, insert_txid) != 0) {
        fail("idempotent retry", "txid differs from the original write");
    }
    require_lease_released("idempotent retry");
    printf("ok idempotent retry: txid=%s deduplicated\n", retry_txid);

    /* 5. Reads see the flushed state and leave the lease record untouched. */
    char *before = lease_data("read after write");
    read_request(request, sizeof request, "SELECT v FROM kv WHERE k = ?", "\"greeting\"");
    response = call(0, handle, request, "read after write");
    char read_value[64];
    extract_string(response, "v", read_value, sizeof read_value, "read after write");
    walrusd_free(response);
    if (strcmp(read_value, value) != 0) {
        fail("read after write", "read value differs from the written value");
    }
    require_lease_unchanged("read after write", before);
    printf("ok read after write: value=%s lease untouched\n", read_value);

    /* 6. A second read is served from the cached read session; still no lease. */
    read_request(request, sizeof request, "SELECT count(*) AS n FROM kv", "");
    response = call(0, handle, request, "second read");
    if (strstr(response, "\"n\":1") == NULL) {
        fail("second read", "expected exactly one row");
    }
    walrusd_free(response);
    require_lease_unchanged("second read", before);
    free(before);
    printf("ok second read: cached session, lease untouched\n");

    response = (char *)walrusd_runtime_close(handle);
    if (response != NULL) {
        walrusd_free(response);
    }

    printf("PASS c\n");
    return 0;
}
