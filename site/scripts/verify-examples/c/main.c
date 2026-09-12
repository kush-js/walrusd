#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const char *walrusd_runtime_version(void);
uint64_t walrusd_runtime_init(const char *req, int n);
const char *walrusd_runtime_write(uint64_t h, const char *req, int n,
                                  long long deadline_ms);
const char *walrusd_runtime_read(uint64_t h, const char *req, int n,
                                 long long deadline_ms);
const char *walrusd_runtime_close(uint64_t h);
void walrusd_free(char *p);

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

uint64_t create_runtime(const char *buffer_root) {
    const char *version = walrusd_runtime_version();
    check_ok(version);
    walrusd_free((char *)version);

    char request[1024];
    snprintf(request, sizeof(request),
             "{\"owner\":\"api-pod-7\",\"config\":{"
             "\"request_timeout_ms\":20000,"
             "\"write_buffer_root_path\":\"%s\"}}",
             buffer_root);
    uint64_t handle = walrusd_runtime_init(
        request, (int)strlen(request));
    if (handle == 0) {
        fprintf(stderr, "walrusd_runtime_init failed\n");
        exit(EXIT_FAILURE);
    }
    return handle;
}

void close_runtime(uint64_t handle) {
    const char *response = walrusd_runtime_close(handle);
    check_ok(response);
    walrusd_free((char *)response);
}

#include <time.h>

char *descriptor_json(const char *root) {
    const char *format =
        "{\"database_id\":\"users/user_1a4b\","
        "\"storage\":{\"provider\":\"file\",\"file_root\":\"%s\"},"
        "\"credentials\":{}}";
    int size = snprintf(NULL, 0, format, root);
    char *json = malloc((size_t)size + 1);
    if (json == NULL) exit(EXIT_FAILURE);
    snprintf(json, (size_t)size + 1, format, root);
    return json;
}

char *read_request(const char *descriptor) {
    const char *format =
        "{\"descriptor\":%s,\"sql\":"
        "\"SELECT body FROM events WHERE id = 1\"}";
    int size = snprintf(NULL, 0, format, descriptor);
    char *json = malloc((size_t)size + 1);
    if (json == NULL) exit(EXIT_FAILURE);
    snprintf(json, (size_t)size + 1, format, descriptor);
    return json;
}

char *read_value(uint64_t handle, const char *descriptor,
                 long long deadline) {
    char *body = read_request(descriptor);
    const char *response = walrusd_runtime_read(
        handle, body, (int)strlen(body), deadline);
    check_ok(response);

    const char *value_start = strstr(response, "\"body\":\"");
    if (value_start == NULL) {
        fprintf(stderr, "walrusd read response has no body: %s\n", response);
        exit(EXIT_FAILURE);
    }
    value_start += strlen("\"body\":\"");
    const char *value_end = strchr(value_start, '"');
    if (value_end == NULL) exit(EXIT_FAILURE);
    size_t size = (size_t)(value_end - value_start);
    char *value = malloc(size + 1);
    if (value == NULL) exit(EXIT_FAILURE);
    memcpy(value, value_start, size);
    value[size] = '\0';

    walrusd_free(body);
    walrusd_free((char *)response);
    return value;
}

char *write_request(const char *descriptor) {
    const char *format =
        "{\"descriptor\":%s,\"idempotency_key\":\"create-event-42\","
        "\"statements\":["
        "{\"sql\":\"CREATE TABLE IF NOT EXISTS events "
        "(id INTEGER PRIMARY KEY, body TEXT)\"},"
        "{\"sql\":\"INSERT INTO events (id, body) VALUES (?, ?)\","
        "\"params\":[1,\"hello from walrusd\"]}]}";
    int size = snprintf(NULL, 0, format, descriptor);
    char *json = malloc((size_t)size + 1);
    if (json == NULL) exit(EXIT_FAILURE);
    snprintf(json, (size_t)size + 1, format, descriptor);
    return json;
}

char *write_txid(uint64_t handle, const char *descriptor,
                 long long deadline) {
    char *body = write_request(descriptor);
    const char *response = walrusd_runtime_write(
        handle, body, (int)strlen(body), deadline);
    check_ok(response);

    const char *txid_start = strstr(response, "\"txid\":\"");
    if (txid_start == NULL) {
        fprintf(stderr, "walrusd write response has no txid: %s\n", response);
        exit(EXIT_FAILURE);
    }
    txid_start += strlen("\"txid\":\"");
    const char *txid_end = strchr(txid_start, '"');
    if (txid_end == NULL) exit(EXIT_FAILURE);
    size_t size = (size_t)(txid_end - txid_start);
    char *txid = malloc(size + 1);
    if (txid == NULL) exit(EXIT_FAILURE);
    memcpy(txid, txid_start, size);
    txid[size] = '\0';

    walrusd_free(body);
    walrusd_free((char *)response);
    return txid;
}

void report_retry(const char *json) {
    if (strstr(json, "\"ok\":false") == NULL) return;

    const char *class_start = strstr(json, "\"class\":\"");
    const char *retry_start = strstr(json, "\"retry_after_ms\":");
    if (class_start != NULL && retry_start != NULL) {
        class_start += strlen("\"class\":\"");
        const char *class_end = strchr(class_start, '"');
        retry_start += strlen("\"retry_after_ms\":");
        fprintf(stderr, "walrusd error class: %.*s (retry after %ld ms)\n",
                (int)(class_end - class_start), class_start,
                strtol(retry_start, NULL, 10));
    }
}

int main(void) {
    const char *root = getenv("WALRUSD_EXAMPLE_ROOT");
    const char *buffer_root = getenv("WALRUSD_BUFFER_ROOT");
    if (root == NULL) root = "/tmp/walrusd-example";
    if (buffer_root == NULL) buffer_root = "/tmp/walrusd-buffers";

    uint64_t handle = create_runtime(buffer_root);
    char *descriptor = descriptor_json(root);
    long long deadline = (long long)time(NULL) * 1000 + 20000;
    char *txid = write_txid(handle, descriptor, deadline);
    char *value = read_value(handle, descriptor, deadline);

    printf("C ABI durable at txid %s\n", txid);
    printf("C ABI read: %s\n", value);

    free(descriptor);
    free(txid);
    free(value);
    close_runtime(handle);
    return EXIT_SUCCESS;
}
