// #pragma once
// #include "hash.h"
// #include "search.h"
// #include "aiordma.h"
// #include "alloc.h"
// #include "config.h"
// #include "kv_trait.h"
// #include "perf.h"
// #include <cassert>
// #include <chrono>
// #include <fcntl.h>
// #include <map>
// #include <math.h>
// #include <tuple>
// #include <vector>

// namespace DHash
// {

// constexpr uint64_t SLOT_PER_BUCKET = 64;
// constexpr uint64_t BUCKET_BITS = 6;
// constexpr uint64_t BUCKET_PER_SEGMENT = (1 << BUCKET_BITS);
// constexpr uint64_t INIT_DEPTH = 4;
// constexpr uint64_t MAX_DEPTH = 22;
// constexpr uint64_t DIR_SIZE = (1 << MAX_DEPTH);

// struct Slot
// {
//     uint8_t fp : 8;
//     uint8_t len : 4; // 按照64bytes对其读取
//     uint8_t depinfo : 4;
//     uint64_t offset : 48;
//     operator uint64_t()
//     {
//         return *(uint64_t *)this;
//     }
//     Slot(uint64_t u)
//     {
//         *this = *(Slot *)(&u);
//     }
// } __attribute__((aligned(8)));

// struct Slice
// {
//     uint64_t len;
//     char *data;
// };

// struct KVBlock
// {
//     uint64_t k_len;
//     uint64_t v_len;
//     char data[0]; //变长数组，用来保证KVBlock空间上的连续性，便于RDMA操作
// };

// template <typename Alloc>
// requires Alloc_Trait<Alloc, uint64_t> KVBlock *InitKVBlock(Slice *key, Slice *value, Alloc *alloc)
// {
//     KVBlock *kv_block = (KVBlock *)alloc->alloc(2 * sizeof(uint64_t) + key->len + value->len);
//     kv_block->k_len = key->len;
//     kv_block->v_len = value->len;
//     memcpy(kv_block->data, key->data, key->len);
//     memcpy(kv_block->data + key->len, value->data, value->len);
//     return kv_block;
// }

// struct Bucket
// {
//     uint32_t local_depth;
//     uint32_t suffix;
//     Slot slots[SLOT_PER_BUCKET];
// } __attribute__((aligned(8)));

// struct Segment
// {
//     struct Bucket buckets[BUCKET_PER_SEGMENT];
// } __attribute__((aligned(8)));

// struct DirEntry
// {
//     uint64_t split_lock;
//     uintptr_t seg_ptr;
//     uint64_t local_depth;
// } __attribute__((aligned(8)));

// struct Directory
// {
//     uint64_t resize_lock; //最后位为global-split lock，后续为local-split count
//     uint64_t global_depth;
//     struct DirEntry segs[DIR_SIZE]; // Directory use MSB and is allocated enough space in advance.
//     uint64_t start_cnt;             // 为多客户端同步保留的字段，不影响原有空间布局
// } __attribute__((aligned(8)));

// constexpr uintptr_t MAIN_DIR_PTR = 0;
// constexpr uintptr_t CUR_DIR_PTR = sizeof(Directory);

// class DHashClient : public BasicDB
// {
//   public:
//     DHashClient(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, uint64_t _machine_id,
//                 uint64_t _cli_id, uint64_t _coro_id);

//     DHashClient(const DHashClient &) = delete;

//     ~DHashClient();

//     // Used for sync operation and test
//     task<> start(uint64_t total);
//     task<> stop();
//     task<> reset_remote();

//     task<> insert(Slice *key, Slice *value);
//     task<std::tuple<uintptr_t, uint64_t>> search(Slice *key, Slice *value); // return slotptr
//     task<> update(Slice *key, Slice *value);
//     task<> remove(Slice *key);

//   private:
//     task<> sync_dir(uintptr_t dir_ptr,Directory* dir);
//     bool IsCorrectBucket(uint64_t segloc, Bucket *buc, uint64_t pattern);
    
//     task<> Split(uint64_t segloc,uintptr_t seg_ptr);
//     task<int> LockDir();
//     task<> UnlockDir();

//     // rdma structs
//     rdma_client *cli;
//     rdma_conn *conn;
//     rdma_rmr rmr;
//     struct ibv_mr *lmr;

//     Alloc alloc;
//     RAlloc ralloc;
//     uint64_t machine_id;
//     uint64_t cli_id;
//     uint64_t coro_id;

//     // Statistic
//     Perf perf;

//     // Data part
//     Directory *cur_dir;
//     Directory *main_dir;
// };

// class DHashServer : public BasicDB
// {
//   public:
//     DHashServer(Config &config);
//     ~DHashServer();

//   private:
//     void Init(Directory *dir);

//     rdma_dev dev;
//     rdma_server ser;
//     struct ibv_mr *lmr;
//     char *mem_buf;

//     Alloc alloc;
//     Directory *cur_dir;
//     Directory *main_dir;
// };

// }; // namespace DHash