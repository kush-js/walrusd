#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

const char *walrusd_runtime_version(void);
uint64_t walrusd_runtime_init(const char *req, int n);
const char *walrusd_runtime_write(uint64_t h, const char *req, int n,
                                  long long deadline_ms);
const char *walrusd_runtime_read(uint64_t h, const char *req, int n,
                                 long long deadline_ms);
const char *walrusd_runtime_close(uint64_t h);
void walrusd_free(char *p);

// walrusd_runtime_read_dsn is also exported for native SQLite readers.
static void check_ok(const char *json) {
    if (strstr(json, "\"ok\":true") != NULL) return;

    const char *class_start = strstr(json, "\"class\":\"");
    if (class_start != NULL) {
        class_start += strlen("\"class\":\"");
        const char *class_end = strchr(class_start, '"');
        fprintf(stderr, "walrusd error class: %.*s\n",
                (int)(class_end - class_start), class_start);
    } else {
        fprintf(stderr, "walrusd response without ok=true: %s\n", json);
    }
    exit(EXIT_FAILURE);
}

static char *write_request(const char *root) {
    const char *format =
        "{\"descriptor\":{\"database_id\":\"users/user_1a4b\","
        "\"storage\":{\"provider\":\"file\",\"file_root\":\"%s\"},"
        "\"credentials\":{}},\"idempotency_key\":\"create-event-42\","
        "\"statements\":["
        "{\"sql\":\"CREATE TABLE IF NOT EXISTS events "
        "(id INTEGER PRIMARY KEY, body TEXT)\"},"
        "{\"sql\":\"INSERT INTO events (id, body) VALUES (?, ?)\","
        "\"params\":[42,\"hello from walrusd\"]}]}";
    int size = snprintf(NULL, 0, format, root);
    char *json = malloc((size_t)size + 1);
    if (json == NULL) exit(EXIT_FAILURE);
    snprintf(json, (size_t)size + 1, format, root);
    return json;
}

static char *read_request(const char *root) {
    const char *format =
        "{\"descriptor\":{\"database_id\":\"users/user_1a4b\","
        "\"storage\":{\"provider\":\"file\",\"file_root\":\"%s\"},"
        "\"credentials\":{}},\"sql\":\"SELECT body FROM events WHERE id = 42\"}";
    int size = snprintf(NULL, 0, format, root);
    char *json = malloc((size_t)size + 1);
    if (json == NULL) exit(EXIT_FAILURE);
    snprintf(json, (size_t)size + 1, format, root);
    return json;
}

static char *txid_from(const char *json) {
    const char *start = strstr(json, "\"txid\":\"");
    if (start == NULL) return NULL;
    start += strlen("\"txid\":\"");
    const char *end = strchr(start, '"');
    if (end == NULL) return NULL;
    size_t size = (size_t)(end - start);
    char *txid = malloc(size + 1);
    if (txid == NULL) exit(EXIT_FAILURE);
    memcpy(txid, start, size);
    txid[size] = '\0';
    return txid;
}

int main(void) {
    const char *root = getenv("WALRUSD_EXAMPLE_ROOT");
    const char *buffer_root = getenv("WALRUSD_BUFFER_ROOT");
    if (root == NULL) root = "/tmp/walrusd-example";
    if (buffer_root == NULL) buffer_root = "/tmp/walrusd-buffers";

    const char *version = walrusd_runtime_version();
    check_ok(version);
    walrusd_free((char *)version);

    char init_request[1024];
    snprintf(init_request, sizeof(init_request),
             "{\"owner\":\"api-pod-7\",\"config\":{"
             "\"request_timeout_ms\":20000,"
             "\"write_buffer_root_path\":\"%s\"}}",
             buffer_root);
    uint64_t handle = walrusd_runtime_init(
        init_request, (int)strlen(init_request));
    if (handle == 0) {
        fprintf(stderr, "walrusd_runtime_init failed\n");
        return EXIT_FAILURE;
    }

    long long deadline = (long long)time(NULL) * 1000 + 20000;
    char *write_body = write_request(root);
    const char *write_response = walrusd_runtime_write(
        handle, write_body, (int)strlen(write_body), deadline);
    check_ok(write_response);
    char *txid = txid_from(write_response);
    walrusd_free(write_body);
    walrusd_free((char *)write_response);

    char *read_body = read_request(root);
    const char *read_response = walrusd_runtime_read(
        handle, read_body, (int)strlen(read_body), deadline);
    check_ok(read_response);
    if (strstr(read_response, "\"body\":\"hello from walrusd\"") == NULL) {
        fprintf(stderr, "unexpected read response: %s\n", read_response);
        return EXIT_FAILURE;
    }
    printf("C ABI durable at txid %s\n", txid);
    printf("C ABI read: hello from walrusd\n");
    walrusd_free(read_body);
    walrusd_free((char *)read_response);
    free(txid);

    const char *close_response = walrusd_runtime_close(handle);
    check_ok(close_response);
    walrusd_free((char *)close_response);
    return EXIT_SUCCESS;
}
