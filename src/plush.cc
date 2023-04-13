#include "plush.h"
namespace Plush
{

inline __attribute__((always_inline)) uint64_t fp(uint64_t pattern)
{
    return ((uint64_t)((pattern) >> 32) & ((1 << 8) - 1));
}

inline __attribute__((always_inline)) uint64_t get_seg_loc(uint64_t pattern, uint64_t global_depth)
{
    return ((pattern) & ((1 << global_depth) - 1));
}

inline __attribute__((always_inline)) uint64_t get_free_bucket_idx(uint64_t size)
{
    uint64_t free_bucket_idx = size / entry_per_bucket;
    return free_bucket_idx < bucket_per_group ? free_bucket_idx : -1;
}

inline __attribute__((always_inline)) uint64_t get_size_of_last_bucket(uint64_t size)
{
    return size & (entry_per_bucket - 1);
}

inline __attribute__((always_inline)) uint64_t get_size_of_bucket(uint64_t size, uint64_t bucket_idx)
{
    uint64_t first_free_bucket_idx = get_free_bucket_idx(size);

    if (first_free_bucket_idx == -1 || bucket_idx < first_free_bucket_idx)
    {
        return entry_per_bucket;
    }
    else if (bucket_idx == first_free_bucket_idx)
    {
        return get_size_of_last_bucket(size);
    }
    else
    {
        return 0;
    }
}

inline __uint128_t cal_filter(const uint32_t hash) noexcept
{
    // //Source: https://github.com/FastFilter/fastfilter_cpp/blob/master/src/bloom/simd-block.h
    // 这部分开销很小，为了避免部署环境麻烦直接把SIMD换成普通指令了
    uint32_t ones[4] = {1, 1, 1, 1};
    uint32_t rehash[4] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU,
                            0xa2b7289dU};
    uint32_t hash_data[4] = {hash, hash, hash, hash};
    hash_data[0] = rehash[0] * hash_data[0];
    hash_data[1] = rehash[1] * hash_data[1];
    hash_data[2] = rehash[2] * hash_data[2];
    hash_data[3] = rehash[3] * hash_data[3];
    hash_data[0] = hash_data[0] >> 27;
    hash_data[1] = hash_data[1] >> 27;
    hash_data[2] = hash_data[2] >> 27;
    hash_data[3] = hash_data[3] >> 27;
    ones[0] = ones[0] << hash_data[0];
    ones[1] = ones[1] << hash_data[1];
    ones[2] = ones[2] << hash_data[2];
    ones[3] = ones[3] << hash_data[3];
    return (((__uint128_t)ones[3]) << (32 * 3)) | (((__uint128_t)ones[2]) << (32 * 2)) | (((__uint128_t)ones[1]) << 32) | ((__uint128_t)ones[0]);
}

Server::Server(Config &config) : dev(nullptr, 1, config.roce_flag), ser(dev)
{
    seg_mr = dev.reg_mr(233, config.mem_size);
    auto [dm, mr] = dev.reg_dmmr(234, dev_mem_size);
    lock_dm = dm;
    lock_mr = mr;

    alloc.Set((char *)seg_mr->addr, seg_mr->length);
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    Init();
    log_err("init");

    // Init locks
    char tmp[dev_mem_size] = {}; // init locks to zero
    lock_dm->memcpy_to_dm(lock_dm, 0, tmp, dev_mem_size);
    log_err("memset");

    ser.start_serve();
}

void Server::Init()
{
    dir->cur_level = 0;
    // other parts of top-pointer is zeor

    // no need to zero top buckets
}

Server::~Server()
{
    rdma_free_mr(seg_mr);
    rdma_free_dmmr({lock_dm, lock_mr});
}

Client::Client(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, rdma_conn *_wowait_conn,
                uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id)
{
    // id info
    machine_id = _machine_id;
    cli_id = _cli_id;
    coro_id = _coro_id;

    // rdma utils
    cli = _cli;
    conn = _conn;
    wo_wait_conn = _wowait_conn;
    lmr = _lmr;

    // alloc info
    alloc.Set((char *)lmr->addr, lmr->length);
    seg_rmr = cli->run(conn->query_remote_mr(233));
    lock_rmr = cli->run(conn->query_remote_mr(234));
    // uint64_t rbuf_size = (seg_rmr.rlen - (1ul << 20) * 100) /
    //                         (config.num_machine * config.num_cli * config.num_coro); // 头部保留5GB，其他的留给client
    // 对于Cluster Hash，其头部空间全部留着用来作为Table的空间，ralloc仅用来写入KV Block
    // 11000000 * ( 8*2 + 8 + 32) = 588 MB
    uint64_t rbuf_size = ((1ul << 20) * 700) / (config.num_machine * config.num_cli * config.num_coro);
    uint64_t buf_id = config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id;
    uintptr_t remote_ptr = seg_rmr.raddr + seg_rmr.rlen - rbuf_size * buf_id; // 从尾部开始分配
    ralloc.SetRemote(remote_ptr, rbuf_size, seg_rmr.raddr, seg_rmr.rlen);
    ralloc.alloc(ALIGNED_SIZE); // 提前分配ALIGNED_SIZE，免得读取的时候越界
    // log_err("ralloc start_addr:%lx offset_max:%lx", ralloc.raddr, ralloc.rsize);

    // util variable
    op_cnt = 0;
    miss_cnt = 0;

    // sync dir
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
}

Client::~Client()
{
    // log_err("[%lu:%lu] miss_cnt:%lu", cli_id, coro_id, miss_cnt);
}

task<> Client::reset_remote()
{
    // 模拟远端分配器信息
    Alloc server_alloc;
    server_alloc.Set((char *)seg_rmr.raddr, seg_rmr.rlen);
    server_alloc.alloc(sizeof(Directory));

    // 重置远端 Lock
    alloc.ReSet(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    co_await conn->write(lock_rmr.raddr, lock_rmr.rkey, dir, dev_mem_size, lmr->lkey);

    // 重置远端 Directory
    dir->cur_level = 0;
    co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, dir, sizeof(Directory), lmr->lkey);
}

task<> Client::start(uint64_t total)
{
    uint64_t *start_cnt = (uint64_t *)alloc.alloc(sizeof(uint64_t), true);
    *start_cnt = 0;
    co_await conn->fetch_add(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, *start_cnt, 1);
    // log_info("Start_cnt:%lu", *start_cnt);
    while ((*start_cnt) < total)
    {
        co_await conn->read(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, start_cnt,
                            sizeof(uint64_t), lmr->lkey);
    }
}

task<> Client::stop()
{
    uint64_t *start_cnt = (uint64_t *)alloc.alloc(sizeof(uint64_t));
    co_await conn->fetch_add(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, *start_cnt, -1);
    // log_err("Start_cnt:%lu", *start_cnt);
    while ((*start_cnt) != 0)
    {
        co_await conn->read(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, start_cnt,
                            sizeof(uint64_t), lmr->lkey);
    }
}

task<> Client::insert(Slice *key, Slice *value)
{
    op_cnt++;
    uint64_t op_size = (1 << 20) * 1;
    // 因为存在pure_write,为上一个操作保留的空间，1MB够用了
    alloc.ReSet(sizeof(Directory));
    uint64_t pattern = hash(key->data, key->len);
    uint64_t tmp_fp = fp(pattern);
    KVBlock *kv_block = InitKVBlock(key, value, &alloc);
    uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 3;
    uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
    // writekv
    wo_wait_conn->pure_write(kvblock_ptr, seg_rmr.rkey, kv_block, kvblock_len, lmr->lkey);
    retry_cnt = 0;
    this->key_num = *(uint64_t *)key->data;
Retry:
    retry_cnt++;
    // if(retry_cnt>10000){
    //     log_err("[%lu:%lu:%lu]too much retry",this->cli_id,this->coro_id,this->key_num);
    //     exit(-1);
    // }
    alloc.ReSet(sizeof(Directory) + kvblock_len);
    // 1. Cal GroupIdx && BucIdx
    uint64_t group_id = pattern % init_group_num;
    uint64_t buc_id = (pattern / init_group_num) % bucket_per_group;

    // 2. Lock mutex of target group
    uintptr_t group_ptr = seg_rmr.raddr + sizeof(uint64_t) + sizeof(TopPointer) * group_id;
    if (!co_await conn->cas_n(group_ptr, seg_rmr.rkey, 0, 1))
    {
        // log_err("[%lu:%lu:%lu]fail to lock group:%lu at first level", this->cli_id, this->coro_id, this->key_num,group_id);
        goto Retry;
    }

    // 3. Read TopPointer
    co_await conn->read(group_ptr, seg_rmr.rkey, &(dir->first_level[group_id]), sizeof(TopPointer), lmr->lkey);
    // log_err("[%lu:%lu:%lu] group:%lu buc:%lu", this->cli_id, this->coro_id, this->key_num,group_id,buc_id);


    if (dir->first_level[group_id].size[buc_id] >= entry_per_bucket)
    {
        // log_err("[%lu:%lu:%lu]migrate_top group:%lu at first level", this->cli_id, this->coro_id, this->key_num,group_id);
        co_await migrate_top(group_id);
        co_await conn->cas_n(group_ptr, seg_rmr.rkey, 1, 0);
        goto Retry;
    }

    // 4. insert to bucket in first level
    uintptr_t entry_ptr = seg_rmr.raddr + sizeof(Directory) + group_id * bucket_per_group * sizeof(Bucket) + buc_id * sizeof(Bucket) + dir->first_level[group_id].size[buc_id] * sizeof(Entry);
    Entry *tmp = (Entry *)alloc.alloc(sizeof(Entry));
    tmp->fp = tmp_fp;
    tmp->len = (kvblock_len + ALIGNED_SIZE - 1) / ALIGNED_SIZE;
    tmp->offset = ralloc.offset(kvblock_ptr);
    co_await conn->write(entry_ptr, seg_rmr.rkey, tmp, sizeof(Entry), lmr->lkey);

    // 5. fdd size of target bucket
    // 这里直接write也可以？不清楚源代码为啥要用fdd
    co_await conn->fetch_add(group_ptr + (2 + buc_id) * sizeof(uint64_t), seg_rmr.rkey, dir->first_level[group_id].size[buc_id], 1);

    // 6. unlock target bucket
    co_await conn->cas_n(group_ptr, seg_rmr.rkey, 1, 0);
}

task<> Client::migrate_top(uint64_t group_id)
{
    Entry new_entrys[fanout * entry_per_group];
    uint64_t keys[fanout * entry_per_group];
    uint64_t sizes[fanout];
    memset(sizes, 0, sizeof(uint64_t) * fanout);
    TopPointer *top_group = &dir->first_level[group_id];
    uint64_t epoch = top_group->epoch;

    Bucket *buc = (Bucket *)alloc.alloc(sizeof(Bucket) * bucket_per_group);
    // 1. read bucket group
    uintptr_t buc_ptr = seg_rmr.raddr + sizeof(Directory) + group_id * bucket_per_group * sizeof(Bucket);
    co_await conn->read(buc_ptr, seg_rmr.rkey, buc, sizeof(Bucket) * bucket_per_group, lmr->lkey);

    // 2. rehash
    for (uint64_t buc_id = 0; buc_id < bucket_per_group; buc_id++)
    {
        co_await rehash(buc + buc_id, dir->first_level[group_id].size[buc_id], 0, keys, new_entrys, sizes);
    }

    // 3. insert bucket to next level
    co_await bulk_level_insert(1, epoch, keys, new_entrys, sizes);
     
    // 4. fetch_add epoch
    uintptr_t group_ptr = seg_rmr.raddr + sizeof(uint64_t) + group_id * sizeof(TopPointer);
    co_await conn->fetch_add(group_ptr+sizeof(uint64_t),seg_rmr.rkey,dir->first_level[group_id].epoch,1);

    // 5. clear bucket size of top level at group_id
    for (uint64_t idx = 0; idx < bucket_per_group; ++idx) {
        dir->first_level[group_id].size[idx] = 0 ;
    }
    co_await conn->write(group_ptr + 2*sizeof(uint64_t),seg_rmr.rkey,dir->first_level[group_id].size,sizeof(uint64_t)*bucket_per_group , lmr->lkey);
    alloc.free(sizeof(Bucket) * bucket_per_group);
}

/// @brief 将buc中size大小的entry，根据old_level+1处的1 bit追加分配到new_entrys[0]或new_entrys[1]中，并同步填入keys。 sizes记录new_entrys[0]和new_entrys[1]的数据量。
/// @param buc
/// @param size
/// @param old_level the level to which the keys belongs,从0(top level)开始
/// @param keys
/// @param new_entrys
/// @param sizes
/// @return
task<> Client::rehash(Bucket *buc, uint64_t size, uint64_t old_level, uint64_t *keys, Entry *new_entrys, uint64_t *sizes)
{
    KVBlock *tmp_block = (KVBlock *)alloc.alloc(8 * ALIGNED_SIZE);
    uint64_t new_group_id; // 或者叫fanout_id
    uint64_t pattern;
    uint64_t group_size = init_group_num * (1 << (old_level + 1));
    uint64_t tmp_key;
    uint64_t *pos;

    for (uint64_t key_id = 0; key_id < size; key_id++)
    {
        if (buc->entrys[key_id].offset == 0)
        {
            log_err("[%lu:%lu:%lu] empty entry during rehash", this->cli_id, this->coro_id, this->key_num);
        }
        // a. read key
        co_await conn->read(ralloc.ptr(buc->entrys[key_id].offset), seg_rmr.rkey, tmp_block, (buc->entrys[key_id].len) * ALIGNED_SIZE, lmr->lkey);
        tmp_key = *(uint64_t *)tmp_block->data;

        // b. cal group id
        pattern = hash(tmp_block->data, tmp_block->k_len);
        new_group_id = pattern % group_size;
        new_group_id = new_group_id >> (init_group_bits + old_level);

        // c. remove duplicate key
        uint64_t *first = keys + (new_group_id * entry_per_group);
        uint64_t *last = keys + (new_group_id * entry_per_group + sizes[new_group_id]);
        pos = std::find(first, last, tmp_key);

        if (pos != last)
        {
            uint64_t offset = pos - first;
            new_entrys[new_group_id * entry_per_group + offset] = buc->entrys[key_id];
            continue;
        }

        // d. insert key
        keys[new_group_id * entry_per_group + sizes[new_group_id]] = tmp_key;
        new_entrys[new_group_id * entry_per_group + sizes[new_group_id]] = buc->entrys[key_id];
        ++sizes[new_group_id];
    }
    alloc.free(8 * ALIGNED_SIZE);
}

/// @brief 将new_entrys写入到next_level中的fanou个group中
/// @param next_level keys将要被写入的层次，从1开始
/// @param epoch
/// @param keys 通过rehash设置的entrys数组，大小为fanout * entry_per_group
/// @param new_entrys 通过rehash设置的entrys数组，大小为fanout * entry_per_group
/// @param sizes uint64 [fanout]数组，记录fanout对应的group中的key数量
/// @return
task<> Client::bulk_level_insert(uint64_t next_level, uint64_t epoch, const uint64_t *keys, const Entry *new_entrys, const uint64_t *sizes)
{
    // 1. check cur_level
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, &dir->cur_level, sizeof(uint64_t), lmr->lkey);
    if (next_level > dir->cur_level)
    {
        log_err("[%lu:%lu:%lu]fetch_add cur_level to :%lu",this->cli_id,this->coro_id,this->key_num,next_level);
        co_await conn->cas_n(seg_rmr.raddr, seg_rmr.rkey, dir->cur_level, next_level);
    }

    // 2. Cal Next Level header ptr and buc ptr
    uint64_t group_size = init_group_num * (1 << next_level);
    uintptr_t group_start_ptr = seg_rmr.raddr + sizeof(uint64_t) + sizeof(TopPointer) * init_group_num;
    uintptr_t buc_start_ptr = seg_rmr.raddr + sizeof(Directory) + sizeof(Bucket) * bucket_per_group * init_group_num;
    uint64_t group_cnt = 0;
    for (uint64_t level_id = 1; level_id < next_level; level_id++)
    {
        group_start_ptr += sizeof(InnerGroupPointer) * init_group_num * (1 << level_id);
        buc_start_ptr += sizeof(Bucket) * bucket_per_group * init_group_num * (1 << level_id);
        group_cnt += init_group_num * (1 << level_id);
    }

    // 3. write key to bucket in new level
    Bucket *buc = (Bucket *)alloc.alloc(sizeof(Bucket));
    for (uint64_t fanout_id = 0; fanout_id < fanout; fanout_id++)
    {
        // log_err("migrate fanout:%lu with size:%lu",fanout_id,sizes[fanout_id]);
        if (sizes[fanout_id] == 0)
        {
            continue;
        }
        assert(sizes[fanout_id] <= entry_per_group);

        // 3.1 Read Inner Group Pointer
        uint64_t pattern = hash(&keys[fanout_id*entry_per_group], sizeof(uint64_t));
        uint64_t group_id = pattern % group_size;
        uintptr_t group_ptr = group_start_ptr + group_id * sizeof(InnerGroupPointer);
        InnerGroupPointer *inner_group = &dir->bottom_levels[group_cnt + group_id];
        co_await conn->read(group_ptr, seg_rmr.rkey, inner_group, sizeof(InnerGroupPointer), lmr->lkey);

        if (inner_group->size + sizes[fanout_id] > entry_per_group)
        {
            // 3.2 Migrate group at next level to make room for data from top level
            // log_err("[%lu:%lu:%lu] Migrate group at level:%lu group:%lu to make room ",this->cli_id,this->coro_id,this->key_num,next_level,group_id);
            co_await migrate_bot(next_level,group_cnt,group_id,group_ptr,buc_start_ptr);
        }

        // 3.3 insert entry into free bucket sequentially
        uint64_t elems_inserted = 0;
        {
            // try bulk insert
            uint64_t free_buc_idx = get_free_bucket_idx(inner_group->size);
            if (free_buc_idx == -1)
            {
                log_err("No more free buc during migrate");
                exit(-1);
            }

            while (free_buc_idx < bucket_per_group && elems_inserted < sizes[fanout_id])
            {
                // a. check bucket ptr
                uintptr_t buc_ptr = buc_start_ptr + (group_id * bucket_per_group + free_buc_idx) * sizeof(Bucket);
                if (inner_group->bucket_pointers[free_buc_idx].buc_ptr == 0)
                {
                    // 提前预留了空间，直接写入就行，省去分配空间的过程
                    inner_group->bucket_pointers[free_buc_idx].buc_ptr = buc_ptr;
                }

                // b. calculate free space in bucket
                uint64_t bucket_size = get_size_of_bucket(inner_group->size, free_buc_idx);
                uint64_t elems_to_insert = std::min(entry_per_bucket - bucket_size, sizes[fanout_id] - elems_inserted);

                // c. insert entrys
                if (elems_to_insert > 0)
                {
                    for (uint64_t entry_id = 0; entry_id < elems_to_insert; entry_id++)
                    {
                        // log_err("migrate key:%lu to level:%lu group:%lu buc:%lu entry_id:%lu",keys[fanout_id * entry_per_group + elems_inserted + entry_id],next_level,group_id,free_buc_idx,entry_id);
                        uint64_t tmp_hash = hash(keys + fanout_id * entry_per_group + elems_inserted + entry_id, sizeof(uint64_t));
                        inner_group->bucket_pointers[free_buc_idx].filter |= cal_filter(tmp_hash);
                        buc->entrys[bucket_size + entry_id] = new_entrys[fanout_id * entry_per_group + elems_inserted + entry_id];
                        co_await conn->write(buc_ptr + (bucket_size + entry_id)*sizeof(Entry),seg_rmr.rkey,&buc->entrys[bucket_size + entry_id],sizeof(Entry),lmr->lkey);
                    }
                    elems_inserted += elems_to_insert;
                }
                // d. update filter and size in bucket pointer
                co_await conn->write(group_ptr + 2 * sizeof(uint64_t) + free_buc_idx * sizeof(BucketPointer) , seg_rmr.rkey , &(inner_group->bucket_pointers[free_buc_idx]) , sizeof(BucketPointer) , lmr->lkey );
                free_buc_idx++;
            }

            // 3.4 update inner group pointer
            inner_group->size += elems_inserted;
            inner_group->epoch = epoch;
            co_await conn->write(group_ptr, seg_rmr.rkey, inner_group, 2 * sizeof(uint64_t), lmr->lkey);
        }
        if(elems_inserted != sizes[fanout_id]){
            log_err("[%lu:%lu:%lu]left unrehashed data with elems_inserted:%lu sizes[:%lu]=%lu ",this->cli_id,this->coro_id,this->key_num,elems_inserted,fanout_id,sizes[fanout_id]);
            exit(-1);
        }
        assert(elems_inserted == sizes[fanout_id]);
    }
    alloc.free(sizeof(Bucket));
}



/// @brief 将source_level中group_id上的数据写入到下一层中对应group上
/// @param source_level 数据所在层次
/// @param group_cnt 从bulk_level_insert中继承的到目标层次之前的group数目
/// @param group_id 在目标层次中要迁移的group
/// @param group_ptr 要迁移group的远端指针
/// @param buc_start_ptr 要迁移group所在level的bucket的起始地址，从bulk_level_insert继承
/// @return 
task<> Client::migrate_bot(uint64_t source_level,uint64_t group_cnt,uint64_t group_id,uintptr_t group_ptr,uintptr_t buc_start_ptr){
    InnerGroupPointer *inner_group = &dir->bottom_levels[group_cnt + group_id];
    uint64_t group_size = init_group_num * (1 << source_level);
    uint64_t keys[bucket_per_group * entry_per_group]; // 每个bucket最多
    Entry entrys[bucket_per_group * entry_per_group];
    uint64_t sizes[bucket_per_group] = {0};

    uint64_t rehashed = 0 ;
    uint64_t buc_idx = 0;
    int epoch = inner_group->epoch;

    // 1. rehash && move entrys
    Bucket* buc = (Bucket*)alloc.alloc(sizeof(Bucket)*bucket_per_group);
    uintptr_t buc_ptr = buc_start_ptr + group_id * bucket_per_group * sizeof(Bucket);
    co_await conn->read(buc_ptr,seg_rmr.rkey,buc,sizeof(Bucket)*bucket_per_group,lmr->lkey);
    while(rehashed < inner_group->size){
        uint64_t to_rehash = std::min(entry_per_bucket , inner_group->size - rehashed);
        co_await rehash(buc+buc_idx,to_rehash,source_level,keys,entrys,sizes);

        rehashed += to_rehash;
        buc_idx++;
    }
    co_await bulk_level_insert(source_level+1,epoch,keys,entrys,sizes);

    // 2. zero bloom filter for all buckets at srouce_level - group_id
    for(uint64_t i = 0 ; i < bucket_per_group ; i++){
        inner_group->bucket_pointers[i].filter = 0;
    }
    co_await conn->write(group_ptr + 2*sizeof(uint64_t),seg_rmr.rkey,inner_group->bucket_pointers,sizeof(BucketPointer)*bucket_per_group,lmr->lkey);
    
    inner_group->size = 0 ;
    co_await conn->write(group_ptr ,seg_rmr.rkey,&inner_group->size,sizeof(uint64_t),lmr->lkey);

    alloc.free(sizeof(Bucket)*bucket_per_group);
}


task<bool> Client::search(Slice *key, Slice *value)
{
    uint64_t pattern = hash(key->data, key->len);
    uint64_t tmp_fp = fp(pattern);
    this->key_num = *(uint64_t *)key->data;
Retry:
    alloc.ReSet(sizeof(Directory));
    KVBlock *res = nullptr;
    KVBlock *kv_block = (KVBlock *)alloc.alloc(7 * ALIGNED_SIZE);
    Bucket* buc = (Bucket*)alloc.alloc(sizeof(Bucket));

    // 1. Cal GroupIdx && BucIdx
    uint64_t group_id = pattern % init_group_num;
    uint64_t buc_id = (pattern / init_group_num) % bucket_per_group;

    // 2. Read TopPointer
    uintptr_t group_ptr = seg_rmr.raddr + sizeof(uint64_t) + sizeof(TopPointer) * group_id;
    co_await conn->read(group_ptr, seg_rmr.rkey, &(dir->first_level[group_id]), sizeof(TopPointer), lmr->lkey);
    uint64_t epoch = dir->first_level[group_id].epoch;
    // log_err("[%lu:%lu:%lu]find at level:0 group:%lu",this->cli_id,this->coro_id,this->key_num,group_id);

    // 3. search in top
    uintptr_t buc_ptr = seg_rmr.raddr + sizeof(Directory) + (group_id*bucket_per_group +buc_id)*sizeof(Bucket);
    co_await conn->read(buc_ptr,seg_rmr.rkey,buc,sizeof(Bucket),lmr->lkey);
    for(int i = dir->first_level[group_id].size[buc_id]-1 ; i >= 0 ;i--){
        if(buc->entrys[i].fp == tmp_fp){
            co_await conn->read(ralloc.ptr(buc->entrys[i].offset), seg_rmr.rkey, kv_block, (buc->entrys[i].len) * ALIGNED_SIZE, lmr->lkey);
            if (memcmp(key->data, kv_block->data, key->len) == 0){
                co_await conn->read(group_ptr, seg_rmr.rkey, &(dir->first_level[group_id]), sizeof(TopPointer), lmr->lkey);
                if(dir->first_level[group_id].epoch == epoch){
                    res = kv_block;
                    break;
                }
                goto Retry;
            }
        }
    }

    // 4. search in bot
    if(res == nullptr){
        uint64_t level = 1 ;
        __uint128_t filter = cal_filter(pattern);
        uintptr_t group_start_ptr = seg_rmr.raddr + sizeof(uint64_t) + sizeof(TopPointer) * init_group_num ;
        uintptr_t buc_start_ptr = seg_rmr.raddr + sizeof(Directory) + sizeof(Bucket)*init_group_num*bucket_per_group;
        uint64_t group_cnt = 0 ;
        co_await conn->read(seg_rmr.raddr,seg_rmr.rkey,&dir->cur_level,sizeof(uint64_t),lmr->lkey);
        while(level <= dir->cur_level){
            uint64_t group_size = init_group_num * (1<<level);
            group_id = pattern % group_size;
            uintptr_t group_ptr = group_start_ptr + group_id*sizeof(InnerGroupPointer);
            InnerGroupPointer* inner_group = &(dir->bottom_levels[group_cnt+group_id]);
BotRetry:
            co_await conn->read(group_ptr,seg_rmr.rkey,inner_group,sizeof(InnerGroupPointer),lmr->lkey); 
            // log_err("[%lu:%lu:%lu]find at level:%lu group:%lu",this->cli_id,this->coro_id,this->key_num,level,group_id);
            for(int i= bucket_per_group - 1 ; i >= 0 ; --i){
                buc_ptr = buc_start_ptr + (group_id * bucket_per_group + i ) * sizeof(Bucket);
                if(((!inner_group->bucket_pointers[i].filter) & filter) == 0){
                    co_await conn->read(buc_ptr,seg_rmr.rkey,buc,sizeof(Bucket),lmr->lkey);
                    uint64_t epoch = inner_group->epoch;
                    uint64_t size = inner_group->size;

                    uint64_t buc_size = get_size_of_bucket(size,i);
                    for(int entry_id = buc_size ; entry_id>=0 ; entry_id--){
                        if(buc->entrys[entry_id].fp == tmp_fp){
                            co_await conn->read(ralloc.ptr(buc->entrys[entry_id].offset), seg_rmr.rkey, kv_block, (buc->entrys[entry_id].len) * ALIGNED_SIZE, lmr->lkey);
                            if (memcmp(key->data, kv_block->data, key->len) == 0)
                            {
                                co_await conn->read(group_ptr,seg_rmr.rkey,inner_group,sizeof(InnerGroupPointer),lmr->lkey);
                                if(inner_group->epoch == epoch){
                                    res = kv_block;
                                    break;
                                }else{
                                    goto BotRetry;
                                }
                            }
                        }
                    }
                }
            }

            co_await conn->read(seg_rmr.raddr,seg_rmr.rkey,&dir->cur_level,sizeof(uint64_t),lmr->lkey);
            ++level;
            group_cnt += group_size;
            group_start_ptr += sizeof(InnerGroupPointer) * group_size;
            buc_start_ptr += sizeof(Bucket) * bucket_per_group * group_size;
        }
    }
    

    if (res != nullptr && res->v_len != 0)
    {
        value->len = res->v_len;
        memcpy(value->data, res->data + res->k_len, value->len);
        co_return true;
    }

    log_err("[%lu:%lu:%lu]No mathc key", this->cli_id, this->coro_id, this->key_num);
    // exit(-1);
    co_return false;
}

task<> Client::update(Slice *key, Slice *value)
{
    co_return;
}
task<> Client::remove(Slice *key)
{
    co_return;
}

} // namespace SPLIT_OP
