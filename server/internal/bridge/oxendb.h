#ifndef OXENDB_H
#define OXENDB_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -----------------------------------------------------------------------
 * Opaque handle — points to a Rust-allocated OxenHandle struct.
 * ----------------------------------------------------------------------- */
typedef struct OxenHandle OxenHandle;

/* -----------------------------------------------------------------------
 * A (ptr, len) pair used to return byte slices from oxen_scan.
 * ----------------------------------------------------------------------- */
typedef struct {
    uint8_t *ptr;
    size_t   len;
} OxenByteSlice;

/* -----------------------------------------------------------------------
 * Return codes — 0 on success, negative on error.
 * ----------------------------------------------------------------------- */
#define OXEN_OK              0
#define OXEN_ERR_IO         -1
#define OXEN_ERR_NOT_FOUND  -2
#define OXEN_ERR_CORRUPTION -3
#define OXEN_ERR_INVALID_ARG -4
#define OXEN_ERR_CLOSED     -5
#define OXEN_ERR_NULL_PTR   -6
#define OXEN_ERR_UNKNOWN    -99

/* -----------------------------------------------------------------------
 * Database lifecycle
 * ----------------------------------------------------------------------- */

/**
 * Open or create a database at `data_dir`.
 * On success writes an allocated OxenHandle* into *out_handle (caller owns it).
 * Returns OXEN_OK or a negative error code.
 */
int oxen_open(const char *data_dir, OxenHandle **out_handle);

/**
 * Close the database and free the handle.
 * After this call the handle must not be used.
 */
int oxen_close(OxenHandle *handle);

/* -----------------------------------------------------------------------
 * Key-value operations
 * ----------------------------------------------------------------------- */

/**
 * Write key[0..key_len) -> value[0..value_len).
 */
int oxen_put(OxenHandle *handle,
             const uint8_t *key_ptr, size_t key_len,
             const uint8_t *value_ptr, size_t value_len);

/**
 * Read the value for key[0..key_len).
 * On success: *out_value points to a Rust-allocated buffer of *out_len bytes.
 * The caller must free it with oxen_free(*out_value, *out_len).
 * Returns OXEN_ERR_NOT_FOUND if the key does not exist.
 */
int oxen_get(OxenHandle *handle,
             const uint8_t *key_ptr, size_t key_len,
             uint8_t **out_value, size_t *out_len);

/**
 * Delete the key (writes a tombstone).
 */
int oxen_delete(OxenHandle *handle,
                const uint8_t *key_ptr, size_t key_len);

/* -----------------------------------------------------------------------
 * Range scan
 * ----------------------------------------------------------------------- */

/**
 * Scan the half-open range [start_key, end_key).
 * Either bound can be NULL to indicate open-ended.
 *
 * On success:
 *   *out_keys   — pointer to an array of OxenByteSlice[*out_count]
 *   *out_values — pointer to a parallel OxenByteSlice[*out_count]
 *
 * The caller must free each individual buffer with oxen_free(), then call
 * oxen_free_scan(out_keys, out_values, count) to free the outer arrays.
 */
int oxen_scan(OxenHandle  *handle,
              const uint8_t *start_ptr, size_t start_len,
              const uint8_t *end_ptr,   size_t end_len,
              OxenByteSlice **out_keys,
              OxenByteSlice **out_values,
              size_t        *out_count);

/* -----------------------------------------------------------------------
 * Memory management
 * ----------------------------------------------------------------------- */

/** Free a buffer previously returned by oxen_get or an element of oxen_scan. */
void oxen_free(uint8_t *ptr, size_t len);

/** Free the outer arrays returned by oxen_scan (after individual buffers freed). */
void oxen_free_scan(OxenByteSlice *keys, OxenByteSlice *values, size_t count);

/* -----------------------------------------------------------------------
 * Administrative
 * ----------------------------------------------------------------------- */

/** Trigger a synchronous compaction round. */
int oxen_compact(OxenHandle *handle);

#ifdef __cplusplus
}
#endif

#endif /* OXENDB_H */
