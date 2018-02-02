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
typedef char* kv_ptr_t;

typedef struct {
    hg_size_t size;
    kv_ptr_t  data;
} kv_data_t;

static inline hg_return_t hg_proc_kv_data_t(hg_proc_t proc, void *arg)
{
  hg_return_t ret;
  kv_data_t *in = (kv_data_t*)arg;

  ret = hg_proc_hg_size_t(proc, &in->size);
  if(ret != HG_SUCCESS) return ret;
  if (in->size) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
      ret = hg_proc_raw(proc, in->data, in->size);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_DECODE:
      in->data = (kv_ptr_t)malloc(in->size);
      ret = hg_proc_raw(proc, in->data, in->size);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_FREE:
      free(in->data);
      break;
    default:
      break;
    }
  }
  return HG_SUCCESS;
}

// ------------- OPEN ------------- //
MERCURY_GEN_PROC(open_in_t, 
        ((hg_string_t)(name)))
MERCURY_GEN_PROC(open_out_t, ((uint64_t)(db_id)) ((int32_t)(ret)))

// ------------- PUT ------------- //
MERCURY_GEN_PROC(put_in_t, ((uint64_t)(db_id))\
        ((kv_data_t)(key))\
        ((kv_data_t)(value)))
MERCURY_GEN_PROC(put_out_t, ((int32_t)(ret)))

// ------------- GET ------------- //
MERCURY_GEN_PROC(get_in_t, ((uint64_t)(db_id))\
        ((kv_data_t)(key))\
        ((hg_size_t)(vsize)))

MERCURY_GEN_PROC(get_out_t, ((int32_t)(ret))\
        ((kv_data_t)(value)))

// ------------- LENGTH ------------- //
MERCURY_GEN_PROC(length_in_t, ((uint64_t)(db_id))((kv_data_t)(key)))
MERCURY_GEN_PROC(length_out_t, ((hg_size_t)(size)) ((int32_t)(ret)))

// ------------- ERASE ------------- //
MERCURY_GEN_PROC(erase_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(erase_in_t, ((uint64_t)(db_id))((kv_data_t)(key)))

// ------------- LIST KEYS ------------- //
MERCURY_GEN_PROC(list_keys_in_t, ((uint64_t)(db_id))\
        ((kv_data_t)(start_key))\
        ((hg_size_t)(max_keys))\
        ((hg_bulk_t)(ksizes_bulk_handle))\
        ((hg_bulk_t)(keys_bulk_handle)))
MERCURY_GEN_PROC(list_keys_out_t, ((hg_size_t)(nkeys)) ((int32_t)(ret)))
// ------------- LIST KEYVALS ------------- //
MERCURY_GEN_PROC(list_keyvals_in_t, ((uint64_t)(db_id))\
        ((kv_data_t)(start_key))\
        ((hg_size_t)(max_keys))\
        ((hg_bulk_t)(ksizes_bulk_handle))\
        ((hg_bulk_t)(keys_bulk_handle))\
        ((hg_bulk_t)(vsizes_bulk_handle))\
        ((hg_bulk_t)(vals_bulk_handle)))
MERCURY_GEN_PROC(list_keyvals_out_t, ((hg_size_t)(nkeys)) ((int32_t)(ret)))

// ------------- BULK PUT ------------- //
MERCURY_GEN_PROC(bulk_put_in_t, ((uint64_t)(db_id))\
        ((kv_data_t)(key))\
        ((hg_size_t)(vsize))\
        ((hg_bulk_t)(handle)))
MERCURY_GEN_PROC(bulk_put_out_t, ((int32_t)(ret)))

// ------------- BULK GET ------------- //
MERCURY_GEN_PROC(bulk_get_in_t, ((uint64_t)(db_id))\
        ((kv_data_t)(key))\
        ((hg_size_t)(vsize))\
        ((hg_bulk_t)(handle)))
MERCURY_GEN_PROC(bulk_get_out_t, ((hg_size_t)(size)) ((int32_t)(ret)))

#endif
