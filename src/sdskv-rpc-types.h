#ifndef SDSKV_RPC_TYPE_H
#define SDSKV_RPC_TYPE_H

#include <stdint.h>
#include <assert.h>

#include <mercury.h>
#include <mercury_macros.h>
#include <mercury_proc_string.h>

#include <margo.h>

#include "sdskv-common.h"

// setup to support opaque type handling
typedef char* kv_data_t;

typedef struct {
  uint64_t  db_id;
  kv_data_t key;
  hg_size_t ksize;
  kv_data_t value;
  hg_size_t vsize;
} put_in_t;

typedef struct {
  uint64_t  db_id;
  kv_data_t key;
  hg_size_t ksize;
  hg_size_t vsize;
} get_in_t;

typedef struct {
  kv_data_t value;
  hg_size_t vsize;
  int32_t ret;
} get_out_t;

typedef struct {
    uint64_t  db_id;
    kv_data_t key;
    hg_size_t ksize;
} length_in_t;

static inline hg_return_t hg_proc_put_in_t(hg_proc_t proc, void *data)
{
  hg_return_t ret;
  put_in_t *in = (put_in_t*)data;

  ret = hg_proc_uint64_t(proc, &in->db_id);
  if(ret != HG_SUCCESS) return ret;

  ret = hg_proc_hg_size_t(proc, &in->ksize);
  if(ret != HG_SUCCESS) return ret;
  if (in->ksize) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      ret = hg_proc_raw(proc, in->key, in->ksize);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_DECODE:
      in->key = (kv_data_t)malloc(in->ksize);
      ret = hg_proc_raw(proc, in->key, in->ksize);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_FREE:
      free(in->key);
      break;
    default:
      break;
    }
  }
  ret = hg_proc_hg_size_t(proc, &in->vsize);
  if(ret != HG_SUCCESS) return ret;
  if (in->vsize) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      ret = hg_proc_raw(proc, in->value, in->vsize);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_DECODE:
      in->value = (kv_data_t)malloc(in->vsize);
      ret = hg_proc_raw(proc, in->value, in->vsize);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_FREE:
      free(in->value);
      break;
    default:
      break;
    }
  }

  return HG_SUCCESS;
}

static inline hg_return_t hg_proc_length_in_t(hg_proc_t proc, void *data)
{
  hg_return_t ret;
  length_in_t *in = (length_in_t*)data;

  ret = hg_proc_uint64_t(proc, &in->db_id);
  if(ret != HG_SUCCESS) return ret;

  ret = hg_proc_hg_size_t(proc, &in->ksize);
  if(ret != HG_SUCCESS) return ret;
  if (in->ksize) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      ret = hg_proc_raw(proc, in->key, in->ksize);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_DECODE:
      in->key = (kv_data_t)malloc(in->ksize);
      ret = hg_proc_raw(proc, in->key, in->ksize);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_FREE:
      free(in->key);
      break;
    default:
      break;
    }
  }
  return HG_SUCCESS;
}

static inline hg_return_t hg_proc_get_in_t(hg_proc_t proc, void *data)
{
  hg_return_t ret;
  get_in_t *in = (get_in_t*)data;

  ret = hg_proc_uint64_t(proc, &in->db_id);
  if(ret != HG_SUCCESS) return ret;

  ret = hg_proc_hg_size_t(proc, &in->ksize);
  if(ret != HG_SUCCESS) return ret;
  if (in->ksize) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      ret = hg_proc_raw(proc, in->key, in->ksize);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_DECODE:
      in->key = (kv_data_t)malloc(in->ksize);
      ret = hg_proc_raw(proc, in->key, in->ksize);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_FREE:
      free(in->key);
      break;
    default:
      break;
    }
  }
  ret = hg_proc_hg_size_t(proc, &in->vsize);
  if(ret != HG_SUCCESS) return ret;

  return HG_SUCCESS;
}

static inline hg_return_t hg_proc_get_out_t(hg_proc_t proc, void *data)
{
  hg_return_t ret;
  get_out_t *out = (get_out_t*)data;

  ret = hg_proc_hg_size_t(proc, &out->vsize);
  if(ret != HG_SUCCESS) return ret;
  if (out->vsize) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      ret = hg_proc_raw(proc, out->value, out->vsize);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_DECODE:
      out->value = (kv_data_t)malloc(out->vsize);
      ret = hg_proc_raw(proc, out->value, out->vsize);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_FREE:
      free(out->value);
      break;
    default:
      break;
    }
  }
  ret = hg_proc_int32_t(proc, &out->ret);
  if(ret != HG_SUCCESS) return ret;

  return HG_SUCCESS;
}

typedef struct {
  uint64_t  db_id;
  kv_data_t key;
  hg_size_t ksize;
} erase_in_t;

static inline hg_return_t hg_proc_erase_in_t(hg_proc_t proc, void *data)
{
  hg_return_t ret;
  erase_in_t *in = (erase_in_t*)data;

  ret = hg_proc_uint64_t(proc, &in->db_id);
  if(ret != HG_SUCCESS) return ret;

  ret = hg_proc_hg_size_t(proc, &in->ksize);
  if(ret != HG_SUCCESS) return ret;
  if (in->ksize) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      ret = hg_proc_raw(proc, in->key, in->ksize);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_DECODE:
      in->key = (kv_data_t)malloc(in->ksize);
      ret = hg_proc_raw(proc, in->key, in->ksize);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_FREE:
      free(in->key);
      break;
    default:
      break;
    }
  }

  return HG_SUCCESS;
}

MERCURY_GEN_PROC(erase_out_t, ((int32_t)(ret)))

typedef struct {
    uint64_t  db_id;
    kv_data_t start_key;
    hg_size_t start_ksize;
    hg_size_t max_keys;
} list_in_t;

typedef struct {
    hg_size_t nkeys;
    kv_data_t *keys;
    hg_size_t *ksizes;
    int32_t ret;
} list_out_t;

static inline hg_return_t hg_proc_list_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    list_in_t *in = (list_in_t*)data;

    ret = hg_proc_uint64_t(proc, &in->db_id);
    if(ret != HG_SUCCESS) return ret;

    ret = hg_proc_hg_size_t(proc, &in->start_ksize);
    if(ret != HG_SUCCESS) return ret;
    if (in->start_ksize) {
	switch(hg_proc_get_op(proc)) {
	    case HG_ENCODE:
		ret = hg_proc_raw(proc, in->start_key, in->start_ksize);
		if(ret != HG_SUCCESS) return ret;
		break;
	    case HG_DECODE:
		in->start_key = (kv_data_t)malloc(in->start_ksize);
		ret = hg_proc_raw(proc, in->start_key, in->start_ksize);
		if(ret != HG_SUCCESS) return ret;
		break;
	    case HG_FREE:
		free(in->start_key);
	    default:
		break;
	}
    }
    ret = hg_proc_hg_size_t(proc, &in->max_keys);
    return ret;
}

static inline hg_return_t hg_proc_list_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    unsigned int i;
    list_out_t *out = (list_out_t*)data;
    ret = hg_proc_hg_size_t(proc, &out->nkeys);
    if(ret != HG_SUCCESS) return ret;
    if (out->nkeys) {
	switch(hg_proc_get_op(proc)) {
	    case HG_ENCODE:
		for (i=0; i<out->nkeys; i++) {
		    ret = hg_proc_raw(proc, &(out->ksizes[i]),
			    sizeof(*(out->ksizes)) );
            if(ret != HG_SUCCESS) return ret;
		}
		for (i=0; i<out->nkeys; i++) {
		    ret = hg_proc_raw(proc, out->keys[i], out->ksizes[i]);
		    if(ret != HG_SUCCESS) return ret;
		}
		break;
	    case HG_DECODE:
		out->ksizes =
		    (hg_size_t*)malloc(out->nkeys*sizeof(*out->ksizes));
		for (i=0; i<out->nkeys; i++) {
		    ret = hg_proc_raw(proc, &(out->ksizes[i]),
			    sizeof(*out->ksizes));
            if(ret != HG_SUCCESS) return ret;
		}
		out->keys = (kv_data_t *)malloc(out->nkeys*sizeof(kv_data_t));
		for (i=0; i<out->nkeys; i++) {
		    out->keys[i] = (kv_data_t)malloc(out->ksizes[i]);
		    ret = hg_proc_raw(proc, out->keys[i], out->ksizes[i]);
		    if(ret != HG_SUCCESS) return ret;
		}
		break;
	    case HG_FREE:
		for (i=0; i<out->nkeys; i++) {
		    free(out->keys[i]);
		}
		free(out->keys);
		free(out->ksizes);
		break;
	    default:
		break;
	}
    }
    ret = hg_proc_int32_t(proc, &out->ret);
    return ret;
}

MERCURY_GEN_PROC(put_out_t, ((int32_t)(ret)))

MERCURY_GEN_PROC(length_out_t, ((hg_size_t)(size)) ((int32_t)(ret)))

MERCURY_GEN_PROC(open_in_t, 
        ((hg_string_t)(name)))
MERCURY_GEN_PROC(open_out_t, ((uint64_t)(db_id)) ((int32_t)(ret)))

// for handling bulk puts/gets (e.g. for ParSplice use case)
typedef struct {
  uint64_t  db_id;
  kv_data_t key;
  hg_size_t ksize;
  hg_size_t vsize;
  hg_bulk_t handle;
} kv_bulk_t;

static inline hg_return_t hg_proc_kv_bulk_t(hg_proc_t proc, void *data)
{
  hg_return_t ret;
  kv_bulk_t *bulk = (kv_bulk_t*)data;

  ret = hg_proc_uint64_t(proc, &bulk->db_id);
  if(ret != HG_SUCCESS) return ret;

  ret = hg_proc_hg_size_t(proc, &bulk->ksize);
  if(ret != HG_SUCCESS) return ret;
  if (bulk->ksize) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      ret = hg_proc_raw(proc, bulk->key, bulk->ksize);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_DECODE:
      bulk->key = (kv_data_t)malloc(bulk->ksize);
      ret = hg_proc_raw(proc, bulk->key, bulk->ksize);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_FREE:
      free(bulk->key);
      break;
    default:
      break;
    }
  }
  ret = hg_proc_hg_size_t(proc, &bulk->vsize);
  if(ret != HG_SUCCESS) return ret;
  ret = hg_proc_hg_bulk_t(proc, &bulk->handle);
  if(ret != HG_SUCCESS) return ret;

  return HG_SUCCESS;
}

MERCURY_GEN_PROC(bulk_put_in_t, ((kv_bulk_t)(bulk)))
MERCURY_GEN_PROC(bulk_put_out_t, ((int32_t)(ret)))

MERCURY_GEN_PROC(bulk_get_in_t, ((kv_bulk_t)(bulk)))
MERCURY_GEN_PROC(bulk_get_out_t, ((hg_size_t)(size)) ((int32_t)(ret)))

static inline hg_return_t hg_proc_double(hg_proc_t proc, void *data)
{
  hg_return_t ret;
  hg_size_t size = sizeof(double);

  ret = hg_proc_raw(proc, data, size);
  assert(ret == HG_SUCCESS);

  return HG_SUCCESS;
}

#endif
