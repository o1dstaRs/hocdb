/* Foreign threads independently use the shared synchronous Zig I/O backend. */
#include "hocdb.h"
#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

typedef struct { int64_t timestamp; double price; } Record;

static void *exercise(void *arg) {
    char ticker[32];
    snprintf(ticker, sizeof(ticker), "THREAD_%ld", (long)(intptr_t)arg);
    CField schema[] = {{"timestamp", HOCDB_TYPE_I64}, {"price", HOCDB_TYPE_F64}};
    HOCDBHandle writer = hocdb_init(ticker, "b_c_test_data/threads", schema, 2, 0, 0, 0, 0);
    assert(writer);
    HOCDBHandle reader = hocdb_open_reader(ticker, "b_c_test_data/threads", schema, 2);
    assert(reader);
    for (int64_t i = 0; i < 10000; i++) {
        Record record = {i, i + 0.25};
        assert(hocdb_append(writer, &record, sizeof(record)) == 0);
        if (i % 257 == 0 || i == 9999) {
            assert(hocdb_flush(writer) == 0);
            size_t bytes;
            Record *rows = hocdb_load(reader, &bytes);
            assert(rows && bytes == (size_t)(i + 1) * sizeof(Record));
            assert(rows[i].timestamp == i && rows[i].price == i + 0.25);
            hocdb_free(rows);
        }
    }
    assert(hocdb_verify(writer) == 1);
    hocdb_close(reader);
    hocdb_drop(writer);
    return NULL;
}

int main(void) {
    pthread_t threads[4];
    for (intptr_t i = 0; i < 4; i++) assert(pthread_create(&threads[i], NULL, exercise, (void *)i) == 0);
    for (int i = 0; i < 4; i++) assert(pthread_join(threads[i], NULL) == 0);
    puts("C foreign-thread I/O test passed");
    return 0;
}
