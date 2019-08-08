#ifndef __SDSKV_CLIENT_HPP
#define __SDSKV_CLIENT_HPP

#include <stdexcept>
#include <vector>
#include <string>
#include <sdskv-client.h>
#include <sdskv-commong.hpp>

#define _CHECK_RET(__ret) \
        if(__ret != SDSKV_SUCCESS) throw exception(__ret)

namespace sdskv {

class provider_handle;
class database;

class client {

    friend class provider_handle;

    margo_instance_id m_mid = MARGO_INSTANCE_NULL;
    sdskv_client_t m_client = SDSKV_CLIENT_NULL;
    bool           m_owns_client = true;

    public:

    client(margo_instance_id mid)
    : m_mid(mid) {
        sdskv_client_register(mid, &m_client);
    }

    client(margo_instance_id mid, sdskv_client_t c, bool transfer_ownership=false)
    : m_mid(mid)
    , m_client(c)
    , m_owns_client(transfer_ownership) {}

    client(const client& c) = delete;

    client(client&& c)
    : m_mid(c.m_mid)
    , m_client(c.m_client)
    , m_owns_client = c.m_owns_client {
        c.m_client = SDSKV_CLIENT_NULL;
    }

    client& operator=(const client& c) = delete;

    client& operator=(client&& c) {
        if(this == &c) return *this;
        if(m_client && m_owns_client) {
            sdskv_client_finalize(m_client);
        }
        m_mid           = c.m_mid;
        m_client        = c.m_client;
        m_owns_client   = c.m_owns_client;
        c.m_client      = SDSKV_CLIENT_NULL;
        c.m_owns_client = true;
        return *this;
    }

    ~client() {
        if(m_owns_client)
            sdskv_client_finalize(m_client);
    }

    operator bool() const {
        return m_client != SDSKV_CLIENT_NULL;
    }

    database open(const provider_handle& ph, const std::string& db_name) const;

    std::vector<database> open(const provider_handle& ph) const;

    //////////////////////////
    // PUT methods
    //////////////////////////

    void put(const database& db,
             const void *key, size_t ksize,
             const void *value, size_t vsize) const; // XXX

    template<typename K, typename V>
    void put(const database& db,
             const K& key, const V& value) const {
        put(db, key.data(), key.size(), value.data(), value.size());
    }

    void put(const database& db,
             size_t count, const void* const* keys, const hg_size_t* ksizes,
             const void* const* values, const hg_size_t *vsizes) const; // XXX

    //////////////////////////
    // PUT_MULTI methods
    //////////////////////////

    void put(const database& db,
             const std::vector<const void*>& keys, const std::vector<size_t>& ksizes,
             const std::vector<const void*>& values, const  std::vector<size_t>& vsizes) const {
        if(keys.size() != ksizes.size()
        || keys.size() != values.size()
        || keys.size() != vsizes.size()) {
            throw std::length_error("Provided vectors should have the same size");
        }
        put(db, keys.size(), keys.data(),  ksizes.data(), values.data(), vsizes.data());
    }

    template<typename K, typename V>
    void put(const database& db,
             const std::vector<K>& keys, const std::vector<V>& values) {
        if(keys.size() != values.size()) {
            throw std::length_error("Provided vectors should have the same size");
        }
        std::vector<const void*> kdata; kdata.reserve(keys.size());
        std::vector<const void*> vdata; vdata.reserve(values.sizes());
        std::vector<size_t> ksizes; ksizes.reserve(keys.size());
        std::vector<size_t> vsizes; vsizes.reserve(values.size());
        for(const auto& k : keys) {
            ksizes.push_back(k.size());
            kdata.push_back(k.data());
        }
        for(const auto& v : values) {
            vsizes.push_back(v.size());
            vdata.push_back(v.data());
        }
        put(db, kdata, ksizes, vdata, vsizes);
    }

    template<typename IK, typename IV>
    void put(const database& db,
             const IK& kbegin, const IK& kend,
             const IV& vbegin, const IV& vend) const {
        size_t count = std::distance(kbegin, kend);
        if(count != std::distance(vbegin, vend)) {
            throw std::length_error("Provided iterators should point to the same number of objects");
        }
        std::vector<const void*> kdata; kdata.reserve(count);
        std::vector<const void*> vdata; vdata.reserve(count);
        std::vector<size_t> ksizes; ksizes.reserve(count);
        std::vector<size_t> vsizes; vsizes.reserve(count);
        for(auto it = kbegin; it != kend; it++) {
            ksizes.push_back(it->size());
            kdata.push_back(it->data());
        }
        for(auto it = vbegin; it != vend; it++) {
            vsizes.push_back(it->size());
            vdata.push_back(it->data());
        }
        put(db, kdata, ksizes, vdata, vsizes);
    }

    //////////////////////////
    // EXISTS methods
    //////////////////////////

    bool exists(const database& db, const void* key, size_t ksize) const; // XXX

    template<typename K>
    bool exists(const database& db, const K& key) const {
        return exists(db, key.data(), key.size());
    }

    //////////////////////////
    // LENGTH methods
    //////////////////////////

    size_t length(const database& db,
            const void* key, size_t ksize) const; // XXX

    template<typename K>
    size_t length(const database& db,
            const K& key) const {
        return length(db, key.data(), key.size());
    }

    //////////////////////////
    // LENGTH_MULTI methods
    //////////////////////////

    bool length(const database& db,
            size_t num, const void* const* keys,
            const size_t* ksizes, size_t* vsizes) const; // XXX

    template<typename K>
    bool length(const database& db,
            const std::vector<K>& keys,
            std::vector<size_t>& vsizes) const {
        vsizes.resize(keys.size());
        std::vector<const void*> kdata; kdata.reserve(keys.size());
        std::vector<size_t> ksize; ksizes.reserve(keys.size());
        for(const auto& k : keys) {
            kdata.push_back(k.data());
            ksizes.push_back(k.size());
        }
        return length(db, keys.size(), kdata.data(), ksizes.data(), vsizes.data());
    }

    template<typename K>
    std::vector<size_t> length(const database& db, const std::vector<K>& keys) const {
        std::vector<size_t> vsizes(keys.size());
        std::vector<const void*> kdata; kdata.reserve(keys.size());
        std::vector<size_t> ksize; ksizes.reserve(keys.size());
        for(const auto& k : keys) {
            kdata.push_back(k.data());
            ksizes.push_back(k.size());
        }
        length(db, keys.size(), kdata.data(), ksizes.data(), vsizes.data());
        return vsizes;
    }

    //////////////////////////
    // GET methods
    //////////////////////////

    bool get(const database& db,
             const void* key, size_t ksize,
             void* value, size_t* vsize) const; // XXX

    template<typename K, typename V>
    bool get(const database& db,
             const K& key, V& value) const {
        size_t s = value.size();
        get(db, key.data(), key.size(), value.data(), &s);
        value.resize(s);
        return true;
    }

    template<typename K, typename V>
    V get(const database& db,
          const K& key) const {
        size_t vsize = length(ph, db, key);
        V value(vsize, 0);
        get(db, key, value);
        return value;
    }

    //////////////////////////
    // GET_MULTI methods
    //////////////////////////

    bool get(const database& db,
             size_t count, const void* const* keys, const size_t* ksizes,
             void** values, hg_size_t *vsizes) const; // XXX

    bool get(const database& db,
             const std::vector<const void*>& keys, const std::vector<size_t>& ksizes,
             const std::vector<void*>& values, std::vector<size_t>& vsizes) const {
        if(keys.size() != ksizes.size()
        || keys.size() != values.size()
        || keys.size() != vsizes.size()) {
            throw std::length_error("Provided vectors should have the same size");
        }
        return get(db, keys.size(), keys.data(),  ksizes.data(), values.data(), vsizes.data());
    }

    template<typename K, typename V>
    bool get(const database& db,
             const std::vector<K>& keys, std::vector<V>& values) const {
        if(keys.size() != values.size()) {
            throw std::length_error("Provided vectors should have the same size");
        }
        std::vector<const void*> kdata; kdata.reserve(keys.size());
        std::vector<void*> vdata; vdata.reserve(values.sizes());
        std::vector<size_t> ksizes; ksizes.reserve(keys.size());
        std::vector<size_t> vsizes; vsizes.reserve(values.size());
        for(const auto& k : keys) {
            ksizes.push_back(k.size());
            kdata.push_back(k.data());
        }
        for(auto& v : values) {
            vsizes.push_back(v.size());
            vdata.push_back(v.data());
        }
        return get(db, kdata, ksizes, vdata, vsizes);
    }

    template<typename IK, typename IV>
    void get(const database& db,
             const IK& kbegin, const IK& kend,
             const IV& vbegin, const IV& vend) const {
        size_t count = std::distance(kbegin, kend);
        if(count != std::distance(vbegin, vend)) {
            throw std::length_error("Provided iterators should point to the same number of objects");
        }
        std::vector<const void*> kdata; kdata.reserve(count);
        std::vector<const void*> vdata; vdata.reserve(count);
        std::vector<size_t> ksizes; ksizes.reserve(count);
        std::vector<size_t> vsizes; vsizes.reserve(count);
        for(auto it = kbegin; it != kend; it++) {
            ksizes.push_back(it->size());
            kdata.push_back(it->data());
        }
        for(auto it = vbegin; it != vend; it++) {
            vsizes.push_back(it->size());
            vdata.push_back(it->data());
        }
        return get(db, kdata, ksizes, vdata, vsizes);
    }

    template<typename K>
    std::vector<std::vector<char>> get(
            const database& db,
            const std::vector<K>& keys) {
        size_t num = keys.size();
        std::vector<size_t> vsizes(num);
        length(db, keys, vsizes);
        std::vector<std::vector<char>> values(num);
        for(unsigned i=0 ; i < num; i++) {
            values[i].resize(vsizes[i]);
        }
        get(db, keys.begin(), keys.end(), values.begin(), values.end());
        return values;
    }

    //////////////////////////
    // ERASE methods
    //////////////////////////
    
    void erase(const database& db,
               const void* key,
               size_t ksize) const; // XXX

    template<typename K>
    void erase( const database& db,
               const K& key) const {
        erase(db, key.data(), key.size());
    }

    //////////////////////////
    // ERASE_MULTI methods
    //////////////////////////

    void erase(const database& db,
            size_t num, const void* const* keys,
            const size_t* ksizes) const; // XXX

    template<typename K>
    void erase(const database& db,
            const std::vector<K>& keys) const {
        std::vector<const void*> kdata; kdata.reserve(keys.size());
        std::vector<size_t> ksize; ksizes.reserve(keys.size());
        for(const auto& k : keys) {
            kdata.push_back(k.data());
            ksizes.push_back(k.size());
        }
        return erase(db, keys.size(), kdata.data(), ksizes.data());
    }

    //////////////////////////
    // LIST_KEYS methods
    //////////////////////////
    
    void list(const database& db,
            const void *start_key, size_t start_ksize,
            const void *prefix, size_t prefix_size,
            void** keys, size_t* ksizes, size_t* max_keys) const; // XXX

    void list(const database& db,
            const void *start_key, size_t start_ksize,
            void** keys, size_t* ksizes, size_t* max_keys) const {
        list(db, start_key, start_ksize, NULL, 0, keys, ksizes, max_keys;
    }

    template<typename K>
    void list(const database& db,
                const K& start_key,
                std::vector<K>& keys) {
        size_t max_keys = keys.size();
        std::vector<void*> kdata; kdata.reserve(keys.size());
        std::vector<size_t> ksizes; ksizes.reserve(keys.size());
        for(auto& k : keys) {
            kdata.push_back(k.data());
            ksizes.push_back(k.size());
        }
        try {
            list(db, start_key.data(), start_key.size(),
                kdata.data(), ksizes.data(), &max_keys);
        } catch(exception& e) {
            if(e.error() == SDSKV_ERR_SIZE) {
                for(unsigned i=0; i < max_keys; i++) {
                    keys[i].resize(ksizes[i]);
                    kdata[i] = keys[i].data();
                }
                list(db, start_key.data(), start_key.size(),
                        kdata.data(), ksizes.data(), &max_keys);
            } else {
                throw;
            }
        }
        for(unsigned i=0; i < max_keys; i++) {
            keys[i].resize(ksizes[i]);
        }
        keys.resize(max_keys);
    }

    template<typename K>
    void list(const database& db,
                const K& start_key,
                const K& prefix,
                std::vector<K>& keys) {
        size_t max_keys = keys.size();
        std::vector<void*> kdata; kdata.reserve(keys.size());
        std::vector<size_t> ksizes; ksizes.reserve(keys.size());
        for(auto& k : keys) {
            kdata.push_back(k.data());
            ksizes.push_back(k.size());
        }
        try {
            list(db, start_key.data(), start_key.size(),
                prefix.data(), prefix.size(),
                kdata.data(), ksizes.data(), &max_keys);
        } catch(exception& e) {
            if(e.error() == SDSKV_ERR_SIZE) {
                for(unsigned i=0; i < max_keys; i++) {
                    keys[i].resize(ksizes[i]);
                    kdata[i] = keys[i].data();
                }
                list(db, start_key.data(), start_key.size(),
                        prefix.data(), prefix.size(),
                        kdata.data(), ksizes.data(), &max_keys);
            } else {
                throw;
            }
        }
        for(unsigned i=0; i < max_keys; i++) {
            keys[i].resize(ksizes[i]);
        }
        keys.resize(max_keys);
    }

    //////////////////////////
    // LIST_KEYVALS methods
    //////////////////////////

    void list(const database& db,
            const void *start_key, size_t start_ksize,
            const void *prefix, size_t prefix_size,
            void** keys, size_t* ksizes, 
            void** values, size_t* vsizes,
            size_t* max_items) const; // XXX

    void list(const database& db,
            const void *start_key, size_t start_ksize,
            void** keys, size_t* ksizes,
            void** values, size_t* vsizes,
            size_t* max_keys) const {
        list(db, start_key, start_ksize,
                NULL, 0, keys, ksizes, values, vsizes, max_items);
    }

    template<typename K, typename V>
    void list(const database& db,
                const K& start_key,
                std::vector<K>& keys,
                std::vector<V>& values) const {

        size_t max_keys = std::min(keys.size(), values.size());
        std::vector<void*> kdata; kdata.reserve(keys.size());
        std::vector<size_t> ksizes; ksizes.reserve(keys.size());
        std::vector<void*> vdata; vdata.reserve(values.size());
        std::vector<size_t> vsizes; vsizes.reserve(values.size());
        for(auto& k : keys) {
            kdata.push_back(k.data());
            ksizes.push_back(k.size());
        }
        for(auto& v : values) {
            vdata.push_back(v.data());
            vsizes.push_back(v.size());
        }
        try {
            list(db, start_key.data(), start_key.size(),
                kdata.data(), ksizes.data(), 
                vdata.data(), vsizes.data(), &max_keys);
        } catch(exception& e) {
            if(e.error() == SDSKV_ERR_SIZE) {
                for(unsigned i=0; i < max_keys; i++) {
                    keys[i].resize(ksizes[i]);
                    kdata[i] = keys[i].data();
                    values[i].resize(vsizes[i]);
                    sdata[i] = values[i].data();
                }
                list(db, start_key.data(), start_key.size(),
                    kdata.data(), ksizes.data(), 
                    vdata.data(), vsizes.data(), &max_keys);
            } else {
                throw;
            }
        }
        for(unsigned i=0; i < max_keys; i++) {
            keys[i].resize(ksizes[i]);
            values[i].resize(vsizes[i]);
        }
        keys.resize(max_keys);
        values.resize(max_keys);
    }

    template<typename K, typename V>
    void list(const database& db,
                const K& start_key,
                const K& prefix,
                std::vector<K>& keys,
                std::vector<V>& values) const {

        size_t max_keys = std::min(keys.size(), values.size());
        std::vector<void*> kdata; kdata.reserve(keys.size());
        std::vector<size_t> ksizes; ksizes.reserve(keys.size());
        std::vector<void*> vdata; vdata.reserve(values.size());
        std::vector<size_t> vsizes; vsizes.reserve(values.size());
        for(auto& k : keys) {
            kdata.push_back(k.data());
            ksizes.push_back(k.size());
        }
        for(auto& v : values) {
            vdata.push_back(v.data());
            vsizes.push_back(v.size());
        }
        try {
            list(db, start_key.data(), start_key.size(),
                prefix.data(), prefix.size(),
                kdata.data(), ksizes.data(), 
                vdata.data(), vsizes.data(), &max_keys);
        } catch(exception& e) {
            if(e.error() == SDSKV_ERR_SIZE) {
                for(unsigned i=0; i < max_keys; i++) {
                    keys[i].resize(ksizes[i]);
                    kdata[i] = keys[i].data();
                    values[i].resize(vsizes[i]);
                    vdata[i] = values[i].data();
                }
                list(db, start_key.data(), start_key.size(),
                        prefix.data(), prefix.size(),
                        kdata.data(), ksizes.data(), 
                        vdata.data(), vsizes.data(), &max_keys);
            } else {
                throw;
            }
        }
        for(unsigned i=0; i < max_keys; i++) {
            keys[i].resize(ksizes[i]);
            values[i].resize(vsizes[i]);
        }
        keys.resize(max_keys);
        values.resize(max_keys);
    }

    //////////////////////////
    // MIGRATE_KEYS methods
    //////////////////////////
    
    void migrate(const database& source_db, const database& dest_db,
            size_t num_items, const void* const* keys, const size_t* key_sizes,
            int flag = SDSKV_KEEP_ORIGINAL) const; // XXX

    void migrate(const database& source_db, const database& dest_db,
            const std::vector<const void*> keys, 
            const std::vector<size_t> ksizes,
            int flag = SDSKV_KEEP_ORIGINAL) const {
        if(keys.size() != ksizes.size()) {
            throw std::length_error("Provided vectors should have the same size");
        }
        migrate(source_db, dest_db, keys.size(), keys.data(), ksizes.data(), flag);
    }

    template<typename K>
    void migrate(const database& source_db, const database& dest_db,
            const std::vector<K> keys, int flag = SDSKV_KEEP_ORIGINAL) const {
        std::vector<size_t> ksizes; ksizes.reserve(keys.size());
        std::vector<const void*> kdata; kdata.reserve(keys.size());
        for(const auto& k : keys) {
            ksizes.push_back(k.size());
            kdata.push_back(k.data());
        }
        migrate(source_db, dest_db,
                kdata, ksizes, flag);
    }

    void migrate(const database& source_db, const database& dest_db,
            const void* key_range[2], const size_t key_sizes[2],
            int flag = SDSKV_KEEP_ORIGINAL) const; // XXX

    template<typename K>
    void migrate(const database& source_db, const database& dest_db,
                 const std::pair<K,K>& key_range,
                 int flag = SDSKV_KEEP_ORIGINAL) const {
        const void* key_range_arr[2] = { key_range.first.data(), key_range.second.data() };
        const size_t key_sizes_arr[2] = { key_range.first.size(), key_range.second.size() };
        migrate(source_db, dest_db, key_range_arr, key_sizes_arr, flag);
    }

    void migrate(const database& source_db, const database& dest_db,
            const void* prefix, size_t prefix_size,
            int flag = SDSKV_KEEP_ORIGINAL) const; // XXX

    template<typename K>
    void migrate(const database& source_db, const database& dest_db,
                 const K& prefix, int flag = SDSKV_KEEP_ORIGINAL) const {
        migrate(source_db, dest_db, prefix.data(), prefix.size(), flag);
    }

    void migrate(const database& source_db, const database& dest_db, int flag = SDSKV_KEEP_ORIGINAL) const; // XXX

    //////////////////////////
    // MIGRATE_DB method
    //////////////////////////

    void migrate(database& source_db,
            const std::string& dest_provider_addr,
            uint16_t dest_provider_id,
            const std::string& dest_root,
            int flag = SDSKV_KEEP_ORIGINAL) const; // XXX

    //////////////////////////
    // SHUTDOWN method
    //////////////////////////
    
    void shutdown(hg_addr_t addr) const {
        int ret = sdskv_shutdown_service(m_client, addr);
        _CHECK_RET(ret);
    }
};

class provider_handle {

    friend class client;
    friend class database;

    sdskv_provider_handle_t m_ph = SDSKV_PROVIDER_HANDLE_NULL;

    public:

    provider_handle() = default;

    provider_handle(
            const client& c,
            hg_addr_t addr,
            uint16_t provider_id=0)
    {
        int ret = sdskv_provider_handle_create(c.m_client, addr, provider_id, &m_ph);
        _CHECK_RET(ret);
    }

    provider_handle(const provider_handle& other)
    : m_ph(other.m_ph) {
        if(m_ph != SDSKV_PROVIDER_HANDLE_NULL) {
            int ret = bake_provider_handle_ref_incr(m_ph);
            _CHECK_RET(ret);
        }
    }

    provider_handle(provider_handle&& other)
    : m_ph(other.m_ph) {
        other.m_ph = SDSKV_PROVIDER_HANDLE_NULL;
    }

    provider_handle& operator=(const provider_handle& other) {
        if(&other == this) return *this;
        if(m_ph != SDSKV_PROVIDER_HANDLE_NULL) {
            int ret = sdskv_provider_handle_release(m_ph);
            _CHECK_RET(ret);
        }
        m_ph = other.m_ph;
        int ret = sdskv_provider_handle_ref_incr(m_ph);
        _CHECK_RET(ret);
        return *this;
    }

    provider_handle& operator=(provider_handle& other) {
        if(&other == this) return *this;
        if(m_ph != SDSKV_PROVIDER_HANDLE_NULL) {
            int ret = sdskv_provider_handle_release(m_ph);
            _CHECK_RET(ret);
        }
        m_ph = other.m_ph;
        other.m_ph = SDSKV_PROVIDER_HANDLE_NULL;
        return *this;
    }

    ~provider_handle() {
        if(m_ph != SDSKV_PROVIDER_HANDLE_NULL)
            sdskv_provider_handle_release(m_ph);
    }

};

class database {
    
    friend class client;
    friend class provider_handle;

    provider_handle     m_ph;
    sdskv_database_id_t m_db_id;

    public:

    template<typename ... T>
    void put(T&& ... args) const {
        m_ph.m_client.put(*this, std::forward<T>(args)...);
    }

    template<typename ... T>
    decltype(auto) length(T&& ... args) const {
        return m_ph.m_client.length(*this, std::forward<T>(args)...);
    }

    template<typename ... T>
    decltype(auto) get(T&& ... args) const {
        return m_ph.m_client.get(*this, std::forward<T>(args)...);
    }

    template<typename ... T>
    void erase(T&& ... args) const {
        m_ph.m_client.erase(*this, std::forward<T>(args)...);
    }

    template<typename ... T>
    decltype(auto) list(T&& ... args) const {
        return m_ph.m_client.list(*this, std::forward<T>(args)...);
    }

    template<typename ... T>
    void migrate(const database& dest_db, T&& ... args) const {
        m_ph.m_client.list(*this, dest_db, std::forward<T>(args)...);
    }

    template<typename ... T>
    void migrate(const std::string& dest_provider_addr, uint16_t dest_provider_id, T&& ... args) {
        m_ph.m_client.migrate(*this, dest_provider_addr, dest_provider_id, std::forward<T>(args)...);
    }

};

inline void client::put(const database& db,
        const void *key, size_t ksize,
        const void *value, size_t vsize) const {
    int ret = sdskv_put(db.m_ph.m_ph, db.m_db_id,
            key, ksize, value, vsize);
    _CHECK_RET(ret);
}

inline void client::put(const database& db,
        size_t count, const void* const* keys, const hg_size_t* ksizes,
        const void* const* values, const hg_size_t *vsizes) const {
    int ret = sdskv_put_multi(db.m_ph.m_ph, db.m_db_id,
            count, keys, ksizes, values, vsizes);
     _CHECK_RET(ret);
}

inline size_t client::length(const database& db,
        const void* key, size_t ksize) const {
    size_t vsize;
    int ret = sdskv_length(db.m_ph.m_ph, db.m_db_id,
            key, ksize, &vsize);
    _CHECK_RET(ret);
    return vsize;
}

inline bool client::exists(const database& db, const void* key, size_t ksize) const {
    int flag;
    int ret = sdskv_exists(db.m_ph.m_ph, db.m_db_id, key, ksize, &flag);
    _CHECK_RET(ret);
    return flag;
}

inline bool client::length(const database& db,
        size_t num, const void* const* keys,
        const size_t* ksizes, size_t* vsizes) const {
    int ret = sdskv_length_multi(db.m_ph.m_ph, db.m_db_id,
            num, keys, ksizes, vsizes);
    _CHECK_RET(ret);
    return true;
}

inline bool client::get(const database& db,
        const void* key, size_t ksize,
        void* value, size_t* vsize) const {
    int ret = sdskv_get(db.m_ph.m_ph, db.m_db_id,
            key, ksize, value, vsize);
    _CHECK_RET(ret);
    return true;
}

inline bool client::get(const database& db,
        size_t count, const void* const* keys, const size_t* ksizes,
        void** values, hg_size_t *vsizes) const {
    int ret = sdskv_get_multi(db.m_ph.m_ph, db.m_db_id,
            count, keys, ksizes, values, vsizes);
    _CHECK_RET(ret);
    return true;
}

inline void client::erase(const database& db,
        const void* key,
        size_t ksize) const {
    int ret = sdskv_erase(db.m_ph.m_ph, db.m_db_id,
            key, ksize);
    _CHECK_RET(ret);
}

inline void client::erase(const database& db,
        size_t num, const void* const* keys,
        const size_t* ksizes) const {
    int ret = sdskv_erase_multi(db.m_ph.m_ph, db.m_db_id,
            num, keys, ksizes);
    _CHECK_RET(ret);
}

inline void client::list(const database& db,
        const void *start_key, size_t start_ksize,
        const void *prefix, size_t prefix_size,
        void** keys, size_t* ksizes, size_t* max_keys) const {
    int ret = sdskv_list_keys_with_prefix(db.m_ph.m_ph, db.m_db_id,
            start_key, start_ksize, prefix, prefix_size,
            keys, ksizes, max_keys);
    _CHECK_RET(ret);
}

inline void client::list(const database& db,
        const void *start_key, size_t start_ksize,
        const void *prefix, size_t prefix_size,
        void** keys, size_t* ksizes,
        void** values, size_t* vsizes,
        size_t* max_items) const {
    int ret = sdskv_list_keyvals_with_prefix(db.m_ph.m_ph, db.m_db_id,
            start_key, start_ksize, prefix, prefix_size,
            keys, ksizes,
            values, vsizes,
            max_items);
    _CHECK_RET(ret);
}

inline void client::migrate(const database& source_db, const database& dest_db,
        size_t num_items, const void* const* keys, const size_t* key_sizes,
        int flag) const {
    hg_addr_t addr;
    uint16_t target_pr_id;
    sdskv_provider_handle_get_info(dest_db.m_ph.m_ph, NULL, &addr, &target_pr_id);

    margo_instance_id mid = dest_db.m_ph.m_client.m_mid;

    std::vector<char> target_addr_str(256);
    size_t addr_size = 256;
    hg_return_t hret = margo_addr_to_string(mid, addr, addr_str.data(), &addr_size);

    if(hret != 0) throw std::runtime_error("margo_addr_to_string failed in client::migrate");

    int ret = sdskv_migrate_keys(source_db.m_ph.m_ph, source_db.m_db_id, 
            target_addr_str.data(), target_pr_id, num_items, keys, key_sizes, flag);
    _CHECK_RET(ret);
}

inline void client::migrate(const database& source_db, const database& dest_db,
        const void* key_range[2], const size_t key_sizes[2],
        int flag) const {
    hg_addr_t addr;
    uint16_t target_pr_id;
    sdskv_provider_handle_get_info(dest_db.m_ph.m_ph, NULL, &addr, &target_pr_id);

    margo_instance_id mid = dest_db.m_ph.m_client.m_mid;

    std::vector<char> target_addr_str(256);
    size_t addr_size = 256;
    hg_return_t hret = margo_addr_to_string(mid, addr, addr_str.data(), &addr_size);

    if(hret != 0) throw std::runtime_error("margo_addr_to_string failed in client::migrate");

    int ret = sdskv_migrate_range(source_db.m_ph.m_ph, source_db.m_db_id, 
            target_addr_str.data(), target_pr_id, key_range, key_sizes, flag);
    _CHECK_RET(ret);
}

inline void client::migrate(const database& source_db, const database& dest_db,
        const void* prefix, size_t prefix_size,
        int flag) const {
    hg_addr_t addr;
    uint16_t target_pr_id;
    sdskv_provider_handle_get_info(dest_db.m_ph.m_ph, NULL, &addr, &target_pr_id);

    margo_instance_id mid = dest_db.m_ph.m_client.m_mid;

    std::vector<char> target_addr_str(256);
    size_t addr_size = 256;
    hg_return_t hret = margo_addr_to_string(mid, addr, addr_str.data(), &addr_size);

    if(hret != 0) throw std::runtime_error("margo_addr_to_string failed in client::migrate");

    int ret = sdskv_migrate_keys_prefixed(source_db.m_ph.m_ph, source_db.m_db_id, 
            target_addr_str.data(), target_pr_id, prefix, prefix_sizes, flag);
    _CHECK_RET(ret);
}

inline void client::migrate(const database& source_db, const database& dest_db, int flag) const {
    hg_addr_t addr;
    uint16_t target_pr_id;
    sdskv_provider_handle_get_info(dest_db.m_ph.m_ph, NULL, &addr, &target_pr_id);

    margo_instance_id mid = dest_db.m_ph.m_client.m_mid;

    std::vector<char> target_addr_str(256);
    size_t addr_size = 256;
    hg_return_t hret = margo_addr_to_string(mid, addr, addr_str.data(), &addr_size);

    if(hret != HG_SUCCESS) throw std::runtime_error("margo_addr_to_string failed in client::migrate");

    int ret = sdskv_migrate_all_keys(source_db.m_ph.m_ph, source_db.m_db_id, 
            target_addr_str.data(), target_pr_id, dest_db.m_db_id, flag);
    _CHECK_RET(ret);
}

inline void client::migrate(database& db,
        const std::string& dest_provider_addr,
        uint16_t dest_provider_id,
        const std::string& dest_root,
        int flag) const {
    hg_addr_t addr;
    margo_instance_id mid = db.m_ph.m_client.m_mid;
    hg_return_t hret = margo_addr_lookup(mid, dest_provider_addr.c_str(), &addr);
    if(hret != HG_SUCCESS) throw std::runtime_error("margo_addr_lookup failed in client::migrate");

    provider_handle dest_ph(addr, dest_provider_id);

    int ret = sdskv_migrate_database(db.m_ph.m_ph, db.m_db_id,
                dest_provider_addr.c_str(),
                dest_provider_id,
                dest_root.c_str(),
                flag);
    _CHECK_RET(ret);

    db.m_ph = std::move(dest_ph);
}

} // namespace sdskv

#undef _CHECK_RET

#endif
