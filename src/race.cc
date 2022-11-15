// 因为使用协程的原因，不能嵌套太多层子函数调用
#include "race.h"
namespace RACE{

inline __attribute__((always_inline)) uint64_t get_seg_loc(uint64_t pattern, uint64_t global_depth)
{
    return ((pattern) & ((1 << global_depth) - 1));
}
inline __attribute__((always_inline)) uint64_t get_buc_loc(uint64_t pattern)
{
    return (pattern >> (8 * sizeof(uint64_t) - BUCKET_BITS - 1));
}
inline __attribute__((always_inline)) uint64_t get_buc_off(uint64_t buc_idx)
{
    return (((buc_idx / 2) * 3 + (buc_idx % 2)) * sizeof(struct Bucket));
}
inline __attribute__((always_inline)) Bucket *get_main_buc(uint64_t buc_idx, Bucket *buc)
{
    return ((buc_idx % 2 == 0) ? (buc) : (buc + 1));
}
inline __attribute__((always_inline)) Bucket *get_over_buc(uint64_t buc_idx, Bucket *buc)
{
    return ((buc_idx % 2 == 0) ? (buc + 1) : (buc));
}
inline __attribute__((always_inline)) uint64_t get_main_ptr(uint64_t buc_idx, uint64_t buc_ptr)
{
    return ((buc_idx % 2 == 0) ? (buc_ptr) : (buc_ptr + sizeof(struct Bucket)));
}
inline __attribute__((always_inline)) uint64_t get_over_ptr(uint64_t buc_idx, uint64_t buc_ptr)
{
    return ((buc_idx % 2 == 0) ? (buc_ptr + sizeof(struct Bucket)) : (buc_ptr));
}
inline __attribute__((always_inline)) bool check_suffix(uint64_t suffix, uint64_t seg_loc, uint64_t local_depth)
{
    return ((suffix & ((1 << local_depth) - 1)) ^ (seg_loc & ((1 << local_depth) - 1)));
}

void PrintDir(Directory *dir)
{
    printf("---------PrintRACE-----\n");
    printf("Global Depth:%lu\n", dir->global_depth);
    uint64_t dir_size = pow(2, dir->global_depth);
    printf("dir_size :%lu\n", dir_size);
    for (uint64_t i = 0; i < dir_size; i++)
    {
        printf("Segment:seg_loc:%lx lock:%lu local_depth:%lu seg_ptr:%lx\n", i,
                 dir->segs[i].split_lock, dir->segs[i].local_depth, dir->segs[i].seg_ptr);
    }
}

RACEServer::RACEServer(Config &config) : dev(nullptr, 1, config.roce_flag),
                                         ser(dev)
{
    lmr = dev.reg_mr(233, config.mem_size);
    alloc.Set((char *)lmr->addr, lmr->length);
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    Init(dir);
    ser.start_serve();
}

void RACEServer::Init(Directory *dir)
{
    dir->global_depth = INIT_DEPTH;
    dir->resize_lock = 0;
    uint64_t dir_size = pow(2, INIT_DEPTH);
    Segment *tmp;
    for (uint64_t i = 0; i < dir_size; i++)
    {
        tmp = (Segment *)alloc.alloc(sizeof(Segment));
        memset(tmp, 0, sizeof(Segment));
        dir->segs[i].seg_ptr = (uintptr_t)tmp;
        dir->segs[i].local_depth = INIT_DEPTH;
        for (uint64_t j = 0; j < BUCKET_PER_SEGMENT * 3; j++)
        {
            tmp->buckets[j].local_depth = INIT_DEPTH;
            tmp->buckets[j].suffix = i;
        }
    }
}

RACEServer::~RACEServer()
{
    rdma_free_mr(lmr);
}

RACEClient::RACEClient(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id)
{
    // id info
    machine_id = _machine_id;
    cli_id = _cli_id;
    coro_id = _coro_id;
    // rdma utils
    cli = _cli;
    conn = _conn;
    lmr = _lmr;

    // alloc info
    alloc.Set((char *)lmr->addr, lmr->length);
    log_info("laddr:%lx llen:%lx", (uint64_t)lmr->addr, lmr->length);
    rmr = cli->run(conn->query_remote_mr(233));
    log_info("raddr:%lx rlen:%lx rend:%lx", (uint64_t)rmr.raddr, rmr.rlen, rmr.raddr + rmr.rlen);
    uint64_t rbuf_size = (rmr.rlen - (1ul << 30) * 5) / (config.num_machine * config.num_cli * config.num_coro); // 头部保留5GB，其他的留给client
    ralloc.SetRemote(rmr.raddr + rmr.rlen - rbuf_size * (config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id), rbuf_size, rmr.raddr, rmr.rlen);

    // sync dir
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    cli->run(sync_dir());
}

RACEClient::~RACEClient(){
    perf.Print();
}

task<> RACEClient::reset_remote(){
    //模拟远端分配器信息
    Alloc server_alloc;
    server_alloc.Set((char *)rmr.raddr, rmr.rlen);
    server_alloc.alloc(sizeof(Directory));

    //重置远端segment
    memset(dir,0,sizeof(Directory));
    dir->global_depth = INIT_DEPTH;
    dir->resize_lock = 0;
    dir->start_cnt = 0;
    uint64_t dir_size = pow(2, INIT_DEPTH);
    alloc.ReSet(sizeof(Directory)); // Make room for local_segment
    Segment *local_seg = (Segment *)alloc.alloc(sizeof(Segment));
    uint64_t remote_seg;
    for (uint64_t i = 0; i < dir_size; i++)
    {
        remote_seg = (uintptr_t)server_alloc.alloc(sizeof(Segment));
        memset(local_seg, 0, sizeof(Segment));
        dir->segs[i].seg_ptr = remote_seg;
        dir->segs[i].local_depth = INIT_DEPTH;
        for (uint64_t j = 0; j < BUCKET_PER_SEGMENT * 3; j++)
        {
            local_seg->buckets[j].local_depth = INIT_DEPTH;
            local_seg->buckets[j].suffix = i;
        }
        co_await conn->write(remote_seg, rmr.rkey, local_seg, size_t(sizeof(Segment)), lmr->lkey);
    }
    //重置远端 Directory
    co_await conn->write(rmr.raddr, rmr.rkey, dir, size_t(sizeof(Directory)), lmr->lkey);
}

task<> RACEClient::start(uint64_t total)
{
    uint64_t *start_cnt = (uint64_t *)alloc.alloc(sizeof(uint64_t), true);
    *start_cnt = 0;
    co_await conn->fetch_add(rmr.raddr + sizeof(Directory) - sizeof(uint64_t), rmr.rkey, *start_cnt, 1);
    // log_info("Start_cnt:%lu", *start_cnt);
    while ((*start_cnt) < total)
    {
        co_await conn->read(rmr.raddr + sizeof(Directory) - sizeof(uint64_t), rmr.rkey, start_cnt, sizeof(uint64_t), lmr->lkey);
    }
}

task<> RACEClient::stop()
{
    uint64_t *start_cnt = (uint64_t *)alloc.alloc(sizeof(uint64_t));
    co_await conn->fetch_add(rmr.raddr + sizeof(Directory) - sizeof(uint64_t), rmr.rkey, *start_cnt, -1);
    // log_info("Start_cnt:%lu", *start_cnt);
    while ((*start_cnt) != 0)
    {
        co_await conn->read(rmr.raddr + sizeof(Directory) - sizeof(uint64_t), rmr.rkey, start_cnt, sizeof(uint64_t), lmr->lkey);
    }
}

task<> RACEClient::insert(Slice *key, Slice *value)
{
    alloc.ReSet(sizeof(Directory));
    uint64_t pattern_1, pattern_2;
    auto pattern = hash(key->data, key->len);
    pattern_1 = (uint64_t)pattern;
    pattern_2 = (uint64_t)(pattern >> 64);
    KVBlock *kv_block = InitKVBlock(key, value, &alloc);
    uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 2;
    uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
    auto wkv = conn->write(kvblock_ptr, rmr.rkey, kv_block, kvblock_len, lmr->lkey);
    uint64_t retry_cnt = 0;
Retry:
    alloc.ReSet(sizeof(Directory)+kvblock_len);
    perf.StartPerf();
    retry_cnt++;
    // Read Segment Ptr From CCEH_Cache
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_depth);
    uintptr_t segptr = dir->segs[segloc].seg_ptr;
    
    // Compute two bucket location
    uint64_t bucidx_1, bucidx_2; // calculate bucket idx for each key
    uintptr_t bucptr_1, bucptr_2;
    bucidx_1 = get_buc_loc(pattern_1);
    bucidx_2 = get_buc_loc(pattern_2);
    bucptr_1 = segptr + get_buc_off(bucidx_1);
    bucptr_2 = segptr + get_buc_off(bucidx_2);

    // 1RTT:Doorbell Read && Write KV-Data
    Bucket *buc_data = (Bucket *)alloc.alloc(4ul * sizeof(Bucket));
    auto rbuc1 = conn->read(bucptr_1, rmr.rkey, buc_data, 2 * sizeof(Bucket), lmr->lkey);
    auto rbuc2 = conn->read(bucptr_2, rmr.rkey, buc_data + 2, 2 * sizeof(Bucket), lmr->lkey);
    co_await std::move(rbuc2);
    co_await std::move(rbuc1);
    if (retry_cnt == 1)
    {
        co_await std::move(wkv);
    }

    if (dir->segs[segloc].local_depth != buc_data->local_depth ||
        dir->segs[segloc].local_depth != (buc_data + 2)->local_depth)
    {
        co_await sync_dir();
        goto Retry;
    }

    perf.AddPerf("ReadBuc");

    perf.StartPerf();
    bool buc_flag = FindLessBucket(buc_data, buc_data + 2);
    uint64_t buc_idx = buc_flag ? bucidx_1 : bucidx_2;
    Bucket *buc = buc_flag ? buc_data : buc_data + 2;
    uintptr_t buc_ptr = buc_flag ? bucptr_1 : bucptr_2;
    uintptr_t slot_ptr = FindEmptySlot(buc, buc_idx, buc_ptr);
    perf.AddPerf("CalPos");

    if (slot_ptr == 0ul)
    {
        // log_info("[%lu:%lu]Insert:%lu %s Split", cli_id, coro_id, *(uint64_t *)key->data, buc->local_depth == dir->global_depth ? "global" : "local");
        // if(retry_cnt%1000==0){
        //     log_err("[%lu:%lu]Split for :%lo with local_depth:%d global_depth:%ld segloc:%lu",cli_id,coro_id,*(uint64_t*)key->data,buc->local_depth , dir->global_depth,segloc);
        // }
        if (retry_cnt > 20000)
        {
            log_err("[%lu:%lu]Too much Split for Insert:%lo with local_depth:%d global_depth:%ld",cli_id,coro_id,*(uint64_t*)key->data,buc->local_depth , dir->global_depth);
            co_await sync_dir();
            PrintDir(dir);
            exit(-1);
            co_return;
        }
        co_await Split(segloc, segptr, buc->local_depth, buc->local_depth == dir->global_depth);
        goto Retry;
    }

    // 2nd RTT: Using RDMA CAS to write the pointer of the key-value block
    perf.StartPerf();
    Slot *tmp = (Slot *)alloc.alloc(sizeof(Slot));
    tmp->fp = FP(pattern_1);
    tmp->len = kvblock_len;
    tmp->offset = ralloc.offset(kvblock_ptr);
    perf.AddCnt("SlotCnt");
    if (!co_await conn->cas_n(slot_ptr, rmr.rkey, 0, *(uint64_t *)tmp))
    {
        perf.AddPerf("CasSlot");
        goto Retry;
    }
    perf.AddPerf("CasSlot");

    // 3rd RTT: Re-reading two combined buckets to remove duplicate keys
    perf.StartPerf();
    auto rbuc3 = conn->read(bucptr_1, rmr.rkey, buc_data, 2 * sizeof(Bucket), lmr->lkey);
    auto rbuc4 = conn->read(bucptr_2, rmr.rkey, buc_data + 2, 2 * sizeof(Bucket), lmr->lkey);
    co_await std::move(rbuc4);
    co_await std::move(rbuc3);
    perf.AddPerf("RRead");

    // Check Dupulicate-key
    perf.StartPerf();
    for (uint64_t round = 0; round < 4; round++)
    {
        buc = buc_data + round;
        buc_ptr = (round / 2 ? bucptr_2 : bucptr_1) + (round % 2 ? sizeof(Bucket) : 0);
        for (uint64_t i = 0; i < SLOT_PER_BUCKET; i++)
        {
            if (buc->slots[i].fp == tmp->fp && buc->slots[i].len == tmp->len && buc->slots[i].offset != tmp->offset)
            {
                char *tmp_key = (char *)alloc.alloc(buc->slots[i].len);
                co_await conn->read(ralloc.ptr(buc->slots[i].offset), rmr.rkey, tmp_key,
                                    buc->slots[i].len, lmr->lkey);
                if (memcmp(key->data, tmp_key + sizeof(uint64_t) * 2, key->len) == 0)
                {
                    log_err("[%lu:%lu]Duplicate-key :%lu",cli_id, coro_id, *(uint64_t *)key->data);
                    co_await conn->cas_n(buc_ptr + sizeof(uint64_t) * (i + 1), rmr.rkey, *(uint64_t *)tmp, 0);
                }
            }
        }
    }
    perf.AddPerf("CheckDuplicate");

    perf.StartPerf();
    buc = buc_flag ? buc_data : buc_data + 2;
    if (IsCorrectBucket(segloc, buc, pattern_1) == false)
    {
        co_await conn->cas_n(slot_ptr, rmr.rkey, *(uint64_t *)tmp, 0);
        co_await sync_dir();
        perf.AddPerf("IsCorrectBucket");
        goto Retry;
    }
    perf.AddPerf("IsCorrectBucket");
}

task<> RACEClient::sync_dir()
{
    co_await conn->read(rmr.raddr + sizeof(uint64_t), rmr.rkey, &dir->global_depth, sizeof(uint64_t), lmr->lkey);
    co_await conn->read(rmr.raddr + sizeof(uint64_t) * 2, rmr.rkey, dir->segs, (1 << dir->global_depth) * sizeof(DirEntry), lmr->lkey);
}

bool RACEClient::FindLessBucket(Bucket *buc1, Bucket *buc2)
{
    int buc1_tot = 0;
    int buc2_tot = 0;
    Bucket *tmp_1 = buc1, *tmp_2 = buc2;
    for (int round = 0; round < 2; round++)
    {
        for (uint64_t i = 0; i < SLOT_PER_BUCKET; i++)
        {
            if (*(uint64_t *)&tmp_1->slots[i])
                buc1_tot++;
            if (*(uint64_t *)&tmp_2->slots[i])
                buc2_tot++;
        }
        tmp_1++;
        tmp_2++;
    }
    return buc1_tot < buc2_tot;
}

uintptr_t RACEClient::FindEmptySlot(Bucket *buc, uint64_t buc_idx, uintptr_t buc_ptr)
{
    Bucket *main_buc = get_main_buc(buc_idx, buc);
    Bucket *over_buc = get_over_buc(buc_idx, buc);
    uint64_t main_buc_ptr = get_main_ptr(buc_idx, buc_ptr);
    uint64_t over_buc_ptr = get_over_ptr(buc_idx, buc_ptr);
    for (uint64_t i = 0; i < SLOT_PER_BUCKET; i++)
    {
        if (*(uint64_t *)(&main_buc->slots[i]) == 0)
        {
            return main_buc_ptr + sizeof(uint64_t) * (i + 1);
        }
    }
    for (uint64_t i = 0; i < SLOT_PER_BUCKET; i++)
    {
        if (*(uint64_t *)(&over_buc->slots[i]) == 0)
        {
            return over_buc_ptr + sizeof(uint64_t) * (i + 1);
        }
    }
    return 0ul;
}

bool RACEClient::IsCorrectBucket(uint64_t segloc, Bucket *buc, uint64_t pattern)
{
    if (buc->local_depth != dir->segs[segloc].local_depth)
    {
        uint64_t suffix = get_seg_loc(pattern, buc->local_depth);
        if (buc->suffix != suffix)
            return false;
    }
    return true;
}

task<int> RACEClient::Split(uint64_t seg_loc, uintptr_t seg_ptr, uint64_t local_depth, bool global_flag)
{
    perf.StartPerf();
    if (local_depth == MAX_DEPTH)
    {
        log_err("Exceed MAX_DEPTH");
        exit(-1);
    }
    if (co_await LockDir())
    {
        co_await sync_dir();
        perf.AddPerf("GetLock");
        co_return 1;
    }

    // Check global depth && global_flag;
    // 检查是否因为本地的过时local_depth导致的错误split类型
    uint64_t *remote_depth = (uint64_t *)alloc.alloc(sizeof(uint64_t));
    co_await conn->read(rmr.raddr + sizeof(uint64_t), rmr.rkey, remote_depth, sizeof(uint64_t), lmr->lkey);
    if (*remote_depth != dir->global_depth)
    {
        co_await UnlockDir();
        co_await sync_dir();
        perf.AddPerf("GetLock");
        co_return 1;
    }

    DirEntry *remote_entry = (DirEntry *)alloc.alloc(sizeof(DirEntry));
    co_await conn->read(rmr.raddr + 2 * sizeof(uint64_t) + seg_loc * sizeof(DirEntry),
                        rmr.rkey, remote_entry, sizeof(DirEntry), lmr->lkey);
    if (remote_entry->local_depth != dir->segs[seg_loc].local_depth)
    {
        co_await UnlockDir();
        co_await sync_dir();
        perf.AddPerf("GetLock");
        co_return 1;
    }
    if (remote_entry->split_lock == 1)
    {
        co_await UnlockDir();
        co_await sync_dir();
        perf.AddPerf("GetLock");
        co_return 1;
    }
    perf.AddPerf("GetLock");
    
    perf.AddCnt("SplitCnt");
    // Allocate New Seg and Init header && write to server
    perf.StartPerf();
    Segment *new_seg = (Segment *)alloc.alloc(sizeof(Segment));
    memset(new_seg, 0, sizeof(Segment));
    uint64_t new_seg_ptr = ralloc.alloc(sizeof(Segment), true); //按八字节对齐
    uint64_t first_seg_loc = seg_loc & ((1ull << local_depth) - 1);
    uint64_t new_seg_loc = first_seg_loc | (1ull << local_depth);
    for (uint64_t i = 0; i < BUCKET_PER_SEGMENT * 3; i++)
    {
        new_seg->buckets[i].local_depth = local_depth + 1;
        new_seg->buckets[i].suffix = new_seg_loc;
    }
    co_await conn->write(new_seg_ptr, rmr.rkey, new_seg, sizeof(Segment), lmr->lkey);
    perf.AddPerf("InitBuc");

    // Edit Directory pointer
    /* 因为使用了MSB和提前分配充足空间的Directory，所以可以直接往后增加Directory Entry*/
    perf.StartPerf();
    co_await sync_dir(); // Global Split必须同步一次Dir，来保证之前没有被同步的DirEntry不会被写到远端。
    if (global_flag)
    {
        // Update Old_seg depth
        dir->segs[seg_loc].split_lock = 1;
        dir->segs[seg_loc].local_depth = local_depth + 1;
        co_await conn->write(rmr.raddr + 2 * sizeof(uint64_t) + seg_loc * sizeof(DirEntry),
                             rmr.rkey, &dir->segs[seg_loc], sizeof(DirEntry), lmr->lkey);

        // Extend Dir
        uint64_t dir_size = 1 << dir->global_depth;
        memcpy(dir->segs + dir_size, dir->segs, dir_size * sizeof(DirEntry));
        dir->segs[new_seg_loc].local_depth = local_depth + 1;
        dir->segs[new_seg_loc].split_lock = 1;
        dir->segs[new_seg_loc].seg_ptr = new_seg_ptr;
        co_await conn->write(rmr.raddr + 2 * sizeof(uint64_t) + dir_size * sizeof(DirEntry),
                             rmr.rkey, dir->segs + dir_size, dir_size * sizeof(DirEntry), lmr->lkey);
        // Update Global Depthx
        dir->global_depth++;
        co_await conn->write(rmr.raddr + sizeof(uint64_t),
                             rmr.rkey, &dir->global_depth, sizeof(uint64_t), lmr->lkey);
    }
    else
    {
        // Local split: Edit all directory share this seg_ptr
        //笔记见备忘录
        uint64_t stride = (1llu) << (dir->global_depth - local_depth);
        uint64_t cur_seg_loc;
        for (uint64_t i = 0; i < stride; i++)
        {
            cur_seg_loc = (i << local_depth) | first_seg_loc;
            if (i & 1)
                dir->segs[cur_seg_loc].seg_ptr = new_seg_ptr;
            else
                dir->segs[cur_seg_loc].seg_ptr = seg_ptr;
            dir->segs[cur_seg_loc].local_depth = local_depth + 1;
            dir->segs[cur_seg_loc].split_lock = 1;

            co_await conn->write(rmr.raddr + 2 * sizeof(uint64_t) + cur_seg_loc * sizeof(DirEntry), rmr.rkey,
                                 dir->segs + cur_seg_loc, sizeof(DirEntry), lmr->lkey);
        }
    }
    co_await UnlockDir();
    perf.AddPerf("EditDir");

    // Move Data
    Segment *old_seg = (Segment *)alloc.alloc(sizeof(Segment));
    co_await MoveData(seg_ptr, new_seg_ptr, old_seg, new_seg);

    // Free Move_Data Lock
    perf.StartPerf();
    while (co_await LockDir())
    {
    }

    if (global_flag)
    {
        dir->segs[seg_loc].split_lock = 0;
        co_await conn->write(rmr.raddr + 2 * sizeof(uint64_t) + seg_loc * sizeof(DirEntry),
                             rmr.rkey, &(dir->segs[seg_loc].split_lock), sizeof(uint64_t), lmr->lkey);
        dir->segs[new_seg_loc].split_lock = 0;
        co_await conn->write(rmr.raddr + 2 * sizeof(uint64_t) + new_seg_loc * sizeof(DirEntry),
                             rmr.rkey, &(dir->segs[new_seg_loc].split_lock), sizeof(uint64_t), lmr->lkey);
    }
    else
    {
        uint64_t stride = (1llu) << (dir->global_depth - local_depth);
        uint64_t cur_seg_loc;
        // log_info("global_dep:%lu local_depth:%lu stride:%lu",dir->global_depth,local_depth,stride);
        for (uint64_t i = 0; i < stride; i++)
        {
            cur_seg_loc = (i << local_depth) | first_seg_loc;
            dir->segs[cur_seg_loc].split_lock = 0;
            co_await conn->write(rmr.raddr + 2 * sizeof(uint64_t) + cur_seg_loc * sizeof(DirEntry), rmr.rkey,
                                 &(dir->segs[cur_seg_loc].split_lock), sizeof(uint64_t), lmr->lkey);
        }
    }
    
    co_await UnlockDir();
    perf.AddPerf("FreeLock");

    co_return 0;
}

task<> RACEClient::MoveData(uint64_t old_seg_ptr, uint64_t new_seg_ptr, Segment *seg, Segment *new_seg)
{
    struct Bucket *cur_buc;
    uint64_t pattern_1, pattern_2, suffix;
    uint64_t buc_ptr;

    for (uint64_t i = 0; i < BUCKET_PER_SEGMENT * 3; i++)
    {
        perf.StartPerf();
        buc_ptr = old_seg_ptr + i * sizeof(Bucket);
        cur_buc = &seg->buckets[i];
        co_await conn->read(buc_ptr, rmr.rkey, cur_buc, sizeof(Bucket), lmr->lkey);
        

        // Update local_depth&suffix
        cur_buc->local_depth = new_seg->buckets[0].local_depth;
        co_await conn->write(buc_ptr, rmr.rkey, cur_buc, sizeof(uint64_t), lmr->lkey);
        perf.AddPerf("ReadOldBuc");

        for (uint64_t slot_idx = 0; slot_idx < SLOT_PER_BUCKET; slot_idx++)
        {
            if (*(uint64_t *)(&cur_buc->slots[slot_idx]) == 0)
                continue;
            perf.StartPerf();
            KVBlock *kv_block = (KVBlock *)alloc.alloc(cur_buc->slots[slot_idx].len);
            co_await conn->read(ralloc.ptr(cur_buc->slots[slot_idx].offset), rmr.rkey,
                                kv_block, cur_buc->slots[slot_idx].len, lmr->lkey);
            perf.AddPerf("ReadKv");

            perf.StartPerf();
            auto pattern = hash(kv_block->data, kv_block->k_len);
            pattern_1 = (uint64_t)pattern;
            pattern_2 = (uint64_t)(pattern >> 64);
            suffix = get_seg_loc(pattern_1, dir->global_depth);
            if (check_suffix(suffix, new_seg->buckets[0].suffix, new_seg->buckets[0].local_depth) == 0)
            {
                // Find free slot in two bucketgroup
                uint64_t bucidx_1 = get_buc_loc(pattern_1);
                uint64_t bucptr_1 = new_seg_ptr + get_buc_off(bucidx_1);
                uint64_t main_buc_ptr1 = get_main_ptr(bucidx_1, bucptr_1);
                uint64_t over_buc_ptr1 = get_over_ptr(bucidx_1, bucptr_1);
                uint64_t bucidx_2 = get_buc_loc(pattern_2);
                uint64_t bucptr_2 = new_seg_ptr + get_buc_off(bucidx_2);
                uint64_t main_buc_ptr2 = get_main_ptr(bucidx_2, bucptr_2);
                uint64_t over_buc_ptr2 = get_over_ptr(bucidx_2, bucptr_2);

                // 依次尝试Bucket 1，OverBuc 1，Bucket 2，OverBuc 2
                if (co_await SetSlot(main_buc_ptr1, *(uint64_t *)(&cur_buc->slots[slot_idx])) && co_await SetSlot(over_buc_ptr1, *(uint64_t *)(&cur_buc->slots[slot_idx])) && co_await SetSlot(main_buc_ptr2, *(uint64_t *)(&cur_buc->slots[slot_idx])) && co_await SetSlot(over_buc_ptr2, *(uint64_t *)(&cur_buc->slots[slot_idx])))
                {
                    // log_err("Fail to move data");
                    continue;
                }

                // CAS slot in old seg to zero
                //  assert((buc_ptr+sizeof(uint64_t)*(slot_idx+1))%8 == 0);
                uint64_t old_slot = *(uint64_t *)(&cur_buc->slots[slot_idx]);
                co_await conn->cas(buc_ptr + sizeof(uint64_t) * (slot_idx + 1),
                                   rmr.rkey, old_slot, 0);
                if (old_slot != *(uint64_t *)(&cur_buc->slots[slot_idx]))
                {
                    //也不影响，只要是这里被的旧slot被删除了就行
                    //只有可能是并发的update导致的
                }
            }
            perf.AddPerf("MoveSlot");
        }
    }
}

/// @brief Used in MoveData
/// @param buc_ptr
/// @param slot
/// @return 0-success to write slot into new_seg at bucidx
//          1-invalid bucidx
task<int> RACEClient::SetSlot(uint64_t buc_ptr, uint64_t slot)
{
    uint64_t slot_idx = 0;
    while (slot_idx < SLOT_PER_BUCKET)
    {
        // assert((buc_ptr+sizeof(uint64_t)*(slot_idx+1))%8 == 0);
        if (co_await conn->cas_n(buc_ptr + sizeof(uint64_t) * (slot_idx + 1), rmr.rkey, 0, slot))
            co_return 0;
        slot_idx++;
    }
    co_return 1;
}

/// @brief 设置Lock为1
/// @return return: 0-success, 1-split conflict
task<int> RACEClient::LockDir()
{
    uint64_t lock;
    // assert((connector.get_remote_addr())%8 == 0);
    if (co_await conn->cas_n(rmr.raddr, rmr.rkey, 0, 1))
    {
        co_return 0;
    }
    co_return 1;
}

task<> RACEClient::UnlockDir()
{
    // Set global split bit
    // assert((connector.get_remote_addr())%8 == 0);
    while (true)
    {
        if (co_await conn->cas_n(rmr.raddr, rmr.rkey, 1, 0))
        {
            break;
        }
    }
}

}//NAMESPACE RACE