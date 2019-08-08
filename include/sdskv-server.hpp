#ifndef __SDSKV_SERVER_HPP
#define __SDSKV_SERVER_HPP

#include <sdskv-server.h>
#include <sdskv-common.hpp>

#define _CHECK_RET(__ret) \
            if(__ret != BAKE_SUCCESS) throw exception(__ret)

namespace sdskv {

class provider {

    margo_instance_id m_mid     = MARGO_INSTANCE_NULL;
    sdskv_provider_t m_provider = SDSKV_PROVIDER_NULL;

    provider(margo_instance_id mid,
             uint16_t provider_id = 0,
             ABT_pool pool = SDSKV_ABT_POOL_DEFAULT)
    : m_mid(mid)
    {
        int ret = sdskv_provider_register(mid, provider_id, pool, &m_provider);
        _CHECK_RET(ret);
    }

    static void finalize_callback(void* args) {
        auto* p = static_cast<provider*>(args);
        delete p;
    }

    public:

    static provider* create(margo_instance_id mid,
                            uint16_t provider_id = 0,
                            ABT_pool pool = SDSKV_ABT_POOL_DEFAULT)
    {
        auto p = new provider(mid, provider_id, pool);
        margo_provider_push_finalize_callback(mid, this, &finalize_callback, this);
    }

    provider(const provider&) = delete;

    provider(provider&& other) = delete;

    provider& operator=(const provider&) = delete;

    provider& operator=(provider&&) = delete;

    ~provider() {
        margo_provider_pop_finalize_callback(mid, this);
        sdskv_provider_destroy(m_provider);
    }

    void add_comparison_function(const std::string& name,
                                 sdskv_compare_fn comp_fn) {
        int ret = sdskv_provider_add_comparison_function(m_provider, name.c_str(), comp_fn);
        _CHECK_RET(ret);
    }

    sdskv_database_id_t attach_database(const sdskv_config_t& config) {
        sdskv_database_id_t db_id;
        int ret = sdskv_provider_attach_database(m_provider, &config, &db_id);
        _CHECK_RET(ret);
        return db_id;
    }

    void remove_database(sdskv_database_id_t db_id) {
        int ret = sdskv_provider_remove_database(m_provider, db_id);
        _CHECK_RET(ret);
    }

    void remove_all_databases() {
        int ret = sdskv_provider_remove_all_databases(m_provider);
        _CHECK_RET(ret);
    }

    std::vector<sdskv_database_id_t> databases() const {
        std::vector<sdskv_database_id_t> dbs;
        uint64_t num_dbs;
        int ret = sdskv_provider_count_databases(m_provider, &num_dbs);
        _CHECK_RET(ret);
        dbs.resize(num_dbs);
        ret = sdskv_provider_list_databases(m_provider, dbs.data());
        _CHECK_RET(ret);
    }

    size_t compute_database_size(sdskv_database_id_t db_id) const {
        size_t size;
        int ret = sdskv_provider_compute_database_size(
                    m_provider,
                    db_id,
                    &size);
        _CHECK_RET(ret);
        return size;
    }
    
    void set_migration_callbacks(sdskv_pre_migration_callback_fn pre_cb,
                                 sdskv_post_migration_callback_fn post_cb,
                                 void* uargs) {
        int ret = sdskv_provider_set_migration_callbacks(m_provider,
                        pre_cb, post_cb, uargs);
        _CHECK_RET(ret);
    }

    void set_abtio_instance(abt_io_instance_id abtio) {
        int ret = sdskv_provider_set_abtio_instance(m_provider, abtio);
        _CHECK_RET(ret);
    }
};

}

#endif
