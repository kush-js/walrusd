#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

const char *walrusd_runtime_read(uint64_t h, const char *req, int n,
                                 long long deadline_ms);
const char *walrusd_runtime_write(uint64_t h, const char *req, int n,
                                  long long deadline_ms);
const char *walrusd_runtime_close(uint64_t h);
void walrusd_free(char *p);

static uint64_t create_runtime_example(void) {
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const char *walrusd_runtime_version(void);
uint64_t walrusd_runtime_init(const char *req, int n);
void walrusd_free(char *p);

const char *version = walrusd_runtime_version();
if (strstr(version, "\"ok\":true") == NULL) {
    fprintf(stderr, "walrusd version failed: %s\n", version);
    exit(EXIT_FAILURE);
}
walrusd_free((char *)version);

const char *init_request =
    "{\"owner\":\"api-pod-7\",\"config\":{"
    "\"request_timeout_ms\":20000,"
    "\"write_buffer_root_path\":\"/tmp/walrusd-example/buffers\"}}";
uint64_t handle = walrusd_runtime_init(
    init_request, (int)strlen(init_request));
if (handle == 0) exit(EXIT_FAILURE);
return handle;
}

static const char *describe_database_example(void) {
const char *descriptor =
    "{\"database_id\":\"users/user_1a4b\","
    "\"storage\":{\"provider\":\"file\","
    "\"file_root\":\"/tmp/walrusd-example\"},"
    "\"credentials\":{}}";
return descriptor;
}

static void read_example(uint64_t handle, const char *descriptor,
                         long long deadline_ms) {
#include <stdint.h>
#include <stdio.h>
#include <string.h>

const char *walrusd_runtime_read(uint64_t h, const char *req, int n,
                                 long long deadline_ms);
void walrusd_free(char *p);

char read_request[2048];
snprintf(read_request, sizeof(read_request),
    "{\"descriptor\":%s,\"sql\":"
    "\"SELECT body FROM events WHERE id = 1\"}",
    descriptor);
const char *response = walrusd_runtime_read(
    handle, read_request, (int)strlen(read_request), deadline_ms);
if (strstr(response, "\"ok\":true") == NULL) { /* read the error envelope */ }
const char *body = strstr(response, "\"body\":\"") + strlen("\"body\":\"");
printf("C ABI read: %.*s\n", (int)(strchr(body, '"') - body), body);
walrusd_free((char *)response);
}

static void write_example(uint64_t handle, const char *descriptor,
                          long long deadline_ms) {
#include <stdint.h>
#include <stdio.h>
#include <string.h>

const char *walrusd_runtime_write(uint64_t h, const char *req, int n,
                                  long long deadline_ms);
void walrusd_free(char *p);

char write_request[4096];
snprintf(write_request, sizeof(write_request),
    "{\"descriptor\":%s,\"idempotency_key\":\"create-event-42\","
    "\"statements\":[{\"sql\":\"CREATE TABLE IF NOT EXISTS events "
    "(id INTEGER PRIMARY KEY, body TEXT)\"},{\"sql\":\"INSERT INTO events "
    "(id, body) VALUES (?, ?)\",\"params\":[1,\"hello from walrusd\"]}]}",
    descriptor);
const char *response = walrusd_runtime_write(
    handle, write_request, (int)strlen(write_request), deadline_ms);
if (strstr(response, "\"ok\":true") == NULL) { /* read the error envelope */ }
const char *txid = strstr(response, "\"txid\":\"") + strlen("\"txid\":\"");
printf("C ABI durable at txid %.*s\n", (int)(strchr(txid, '"') - txid), txid);
walrusd_free((char *)response);

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const char *error_response = walrusd_runtime_write(
    handle, write_request, (int)strlen(write_request), deadline_ms);
if (strstr(error_response, "\"ok\":false") != NULL) {
    const char *class_start =
        strstr(error_response, "\"class\":\"") + strlen("\"class\":\"");
    const char *retry_after =
        strstr(error_response, "\"retry_after_ms\":");
    fprintf(stderr, "class: %.*s, retry_after_ms: %ld\n",
            (int)(strchr(class_start, '"') - class_start), class_start,
            retry_after == NULL ? 0 :
                strtol(retry_after + strlen("\"retry_after_ms\":"), NULL, 10));
}
walrusd_free((char *)error_response);
}

int main(void) {
    uint64_t handle = create_runtime_example();
    const char *descriptor = describe_database_example();
    long long deadline_ms = (long long)time(NULL) * 1000 + 20000;

    write_example(handle, descriptor, deadline_ms);
    read_example(handle, descriptor, deadline_ms);

    const char *close_response = walrusd_runtime_close(handle);
    if (strstr(close_response, "\"ok\":true") == NULL) {
        fprintf(stderr, "walrusd_runtime_close failed: %s\n", close_response);
        return EXIT_FAILURE;
    }
    walrusd_free((char *)close_response);
    return EXIT_SUCCESS;
}
