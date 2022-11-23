#include "race_op.h"

namespace RACEOP
{
inline __attribute__((always_inline)) uint64_t fp(uint64_t pattern)
{
    return ((uint64_t)((pattern) >> 32) & ((1 << 8) - 1));
}

inline __attribute__((always_inline)) uint64_t get_seg_loc(uint64_t pattern, uint64_t global_depth)
{
    return ((pattern) & ((1 << global_depth) - 1));
}

inline __attribute__((always_inline)) bool check_suffix(uint64_t suffix, uint64_t seg_loc, uint64_t local_depth)
{
    return ((suffix & ((1 << local_depth) - 1)) ^ (seg_loc & ((1 << local_depth) - 1)));
}

RACEServer::RACEServer(Config &config) : dev(nullptr, 1, config.roce_flag), ser(dev)
{
    seg_mr = dev.reg_mr(233, config.mem_size);
    auto [dm, mr] = dev.reg_dmmr(234, dev_mem_size);
    lock_dm = dm;
    lock_mr = mr;

    alloc.Set((char *)seg_mr->addr, seg_mr->length);
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    Init();
    log_info("init");

    // Init locks
    // memset(lock_mr->addr, 0, lock_mr->length);
    char tmp[dev_mem_size] = {};
    lock_dm->memcpy_to_dm(lock_dm, 0, tmp, dev_mem_size);
    log_info("memset");

    ser.start_serve();
}

void RACEServer::Init()
{
    dir->global_depth = INIT_DEPTH;
    uint64_t dir_size = pow(2, INIT_DEPTH);
    Segment *tmp;
    for (uint64_t i = 0; i < dir_size; i++)
    {
        tmp = (Segment *)alloc.alloc(sizeof(Segment));
        // printf("Segment %lu : ptr:%lx\n", i, (uint64_t)tmp);
        memset(tmp, 0, sizeof(Segment));
        dir->segs[i].seg_ptr = (uintptr_t)tmp;
        dir->segs[i].local_depth = INIT_DEPTH;
        tmp->local_depth = INIT_DEPTH;
        tmp->suffix = i;
    }
}

RACEServer::~RACEServer()
{
    rdma_free_mr(seg_mr);
    rdma_free_dmmr({lock_dm, lock_mr});
}

RACEClient::RACEClient(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, rdma_conn *_wowait_conn,
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
    log_info("laddr:%lx llen:%lx", (uint64_t)lmr->addr, lmr->length);
    seg_rmr = cli->run(conn->query_remote_mr(233));
    lock_rmr = cli->run(conn->query_remote_mr(234));
    log_info("raddr:%lx rlen:%lx rend:%lx", (uint64_t)seg_rmr.raddr, seg_rmr.rlen, seg_rmr.raddr + seg_rmr.rlen);
    uint64_t rbuf_size = (seg_rmr.rlen - (1ul << 30) * 5) /
                         (config.num_machine * config.num_cli * config.num_coro); // 头部保留5GB，其他的留给client
    ralloc.SetRemote(
        seg_rmr.raddr + seg_rmr.rlen -
            rbuf_size * (config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id),
        rbuf_size, seg_rmr.raddr, seg_rmr.rlen);

    // init tmp variable
    op_cnt = 0;

    // sync dir
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    cli->run(sync_dir());
}

RACEClient::~RACEClient()
{
    perf.Print();
}

task<> RACEClient::reset_remote()
{
    //模拟远端分配器信息
    Alloc server_alloc;
    server_alloc.Set((char *)seg_rmr.raddr, seg_rmr.rlen);
    server_alloc.alloc(sizeof(Directory));

    //重置远端 Lock
    alloc.ReSet(sizeof(Directory)); // Make room for local_segment
    memset(dir, 0, sizeof(Directory));
    co_await conn->write(lock_rmr.raddr, lock_rmr.rkey, dir, dev_mem_size, lmr->lkey);

    //重置远端segment
    dir->global_depth = INIT_DEPTH;
    dir->start_cnt = 0;
    uint64_t dir_size = pow(2, INIT_DEPTH);
    Segment *local_seg = (Segment *)alloc.alloc(sizeof(Segment));
    uint64_t remote_seg;
    for (uint64_t i = 0; i < dir_size; i++)
    {
        remote_seg = (uintptr_t)server_alloc.alloc(sizeof(Segment));
        memset(local_seg, 0, sizeof(Segment));
        dir->segs[i].seg_ptr = remote_seg;
        dir->segs[i].local_depth = INIT_DEPTH;
        local_seg->local_depth = INIT_DEPTH;
        local_seg->suffix = i;
        co_await conn->write(remote_seg, seg_rmr.rkey, local_seg, size_t(sizeof(Segment)), lmr->lkey);
    }
    //重置远端 Directory
    co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, dir, size_t(sizeof(Directory)), lmr->lkey);
}

task<> RACEClient::start(uint64_t total)
{
    // co_await sync_dir();
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

task<> RACEClient::stop()
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

task<> RACEClient::sync_dir()
{
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, &dir->global_depth, sizeof(uint64_t), lmr->lkey);
    co_await conn->read(seg_rmr.raddr + sizeof(uint64_t), seg_rmr.rkey, dir->segs,
                        (1 << dir->global_depth) * sizeof(DirEntry), lmr->lkey);
}

task<> RACEClient::insert(Slice *key, Slice *value)
{
    op_cnt++;
    perf.StartPerf();
    alloc.ReSet(sizeof(Directory));
    uint64_t pattern_1 = (uint64_t)hash(key->data, key->len);
    KVBlock *kv_block = InitKVBlock(key, value, &alloc);
    uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 2;
    uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
    auto wkv = conn->write(kvblock_ptr, seg_rmr.rkey, kv_block, kvblock_len, lmr->lkey);
#ifdef WO_WAIT_WRITE
    // poll wo_wait conn every 63 write
    auto poll_wowait = (op_cnt == 31) ? wo_wait_conn->read(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t),
                                                           seg_rmr.rkey, &dir->start_cnt, sizeof(uint64_t), lmr->lkey)
                                      : rdma_future{};
#endif
    uint64_t retry_cnt = 0;
    perf.AddPerf("InitKv");
Retry:
    alloc.ReSet(sizeof(Directory) + kvblock_len);
    Slot *tmp = (Slot *)alloc.alloc(sizeof(Slot));
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_depth);
    uintptr_t segptr = dir->segs[segloc].seg_ptr;
    uintptr_t lock_ptr = lock_rmr.raddr + sizeof(uint64_t) + (sizeof(uint64_t)) * (segloc % num_lock);

    // lock segment
    perf.StartPerf();
    while (!co_await conn->cas_n(lock_ptr, lock_rmr.rkey, 0, 1))
    {
        // log_err("[%lu:%lu]Fail to lock seg:%lx for key:%lu", cli_id, coro_id, segloc, *(uint64_t *)key->data);
    }
    perf.AddPerf("LockSeg");

    // read segment
    perf.StartPerf();
    Segment *tmp_segs = (Segment *)alloc.alloc(sizeof(Segment));
    co_await conn->read(segptr, seg_rmr.rkey, tmp_segs, sizeof(Segment), lmr->lkey);
    perf.AddPerf("ReadSeg");

    if (dir->segs[segloc].local_depth != tmp_segs->local_depth)
    {
        co_await sync_dir();
#ifdef WO_WAIT_WRITE
        *tmp = 0;
        wo_wait_conn->pure_write(lock_ptr, lock_rmr.rkey, tmp, sizeof(uint64_t), lmr->lkey);
        if (op_cnt == 31)
            co_await poll_wowait;
#else
        *tmp = 0;
        co_await conn->write(lock_ptr, lock_rmr.rkey, tmp, sizeof(uint64_t), lmr->lkey);
#endif
        goto Retry;
    }

    // find free slot
    perf.StartPerf();
    uint64_t slot_id = linear_search((uint64_t *)tmp_segs->slots, SLOT_PER_SEGMENT - 1, 0);
    // uint64_t slot_id = linear_search_avx_ur((uint64_t *)tmp_segs->slots, SLOT_PER_SEGMENT - 1, 0);
    perf.AddPerf("FindSlot");
    if (slot_id == -1)
    {
        // log_err("[%lu:%lu]No Free Slot for key:%lx with pattern_1:%lx at segloc:%lx with local_dep:%x global_depth:%lx",
        //         cli_id, coro_id, *(uint64_t *)key->data, pattern_1, segloc, tmp_segs->local_depth, dir->global_depth);
        // Split
        co_await Split(segloc, segptr, tmp_segs);
        goto Retry;
    }

    // write slot
    perf.StartPerf();
    tmp->fp = fp(pattern_1);
    tmp->len = kvblock_len;
    tmp->offset = ralloc.offset(kvblock_ptr);
    if (retry_cnt == 0)
    {
        co_await std::move(wkv);
        retry_cnt++;
    }
#ifdef WO_WAIT_WRITE
    wo_wait_conn->pure_write(segptr + sizeof(uint64_t) + slot_id * sizeof(Slot), seg_rmr.rkey, tmp, sizeof(Slot),
                             lmr->lkey);
#else
    co_await conn->write(segptr + sizeof(uint64_t) + slot_id * sizeof(Slot), seg_rmr.rkey, tmp, sizeof(Slot),
                         lmr->lkey);
#endif
    perf.AddPerf("WriteSlot");

    // free segment
    *tmp = 0;
    perf.StartPerf();
#ifdef WO_WAIT_WRITE
    wo_wait_conn->pure_write(lock_ptr, lock_rmr.rkey, tmp, sizeof(uint64_t), lmr->lkey);
    if (op_cnt == 31)
        co_await poll_wowait;
#else
    co_await conn->write(lock_ptr, lock_rmr.rkey, tmp, sizeof(uint64_t), lmr->lkey);
#endif
    perf.AddPerf("FreeSeg");
}

task<> RACEClient::Split(uint64_t seg_loc, uintptr_t seg_ptr, Segment *old_seg)
{
    // Alloc new_segment at local && remote
    uint64_t local_depth = old_seg->local_depth;
    Segment *local_seg = (Segment *)alloc.alloc(sizeof(Segment));
    uintptr_t new_seg_ptr = ralloc.alloc(sizeof(Segment), true);
    uint64_t first_seg_loc = seg_loc & ((1ull << old_seg->local_depth) - 1);
    uint64_t new_seg_loc = first_seg_loc | (1ull << old_seg->local_depth);
    // MoveData
    uint64_t pattern_1, suffix;
    uint64_t slot_off = 0; // offset of free slot in new_seg
    for (uint64_t i = 0; i < SLOT_PER_SEGMENT; i++)
    {
        perf.StartPerf();
        KVBlock *kv_block = (KVBlock *)alloc.alloc(old_seg->slots[i].len);
        co_await conn->read(ralloc.ptr(old_seg->slots[i].offset), seg_rmr.rkey, kv_block, old_seg->slots[i].len,
                            lmr->lkey);
        perf.AddPerf("ReadKv");
        auto pattern = hash(kv_block->data, kv_block->k_len);
        pattern_1 = (uint64_t)pattern;
        suffix = (local_depth == dir->global_depth) ? get_seg_loc(pattern_1, dir->global_depth + 1)
                                                    : get_seg_loc(pattern_1, dir->global_depth );
        if (check_suffix(suffix, new_seg_loc,local_depth + 1) == 0)
        {
            local_seg->slots[slot_off++] = old_seg->slots[i];
            old_seg->slots[i] = 0;
        }
    }
    local_seg->local_depth = local_depth + 1;
    old_seg->local_depth = local_depth + 1;
    auto w_oldseg = conn->write(seg_ptr, seg_rmr.rkey, old_seg, sizeof(Segment), lmr->lkey);
    auto w_newseg = conn->write(new_seg_ptr, seg_rmr.rkey, local_seg, sizeof(Segment), lmr->lkey);
    // Lock Directory && Edit dir-entry
    while (co_await LockDir())
    {
    }

    co_await sync_dir(); // 同步一次Dir，来保证之前没有被同步的DirEntry不会被写到远端。
    if (local_depth == dir->global_depth)
    {
        // Global Split
        dir->segs[seg_loc].local_depth = local_depth + 1;
        co_await conn->write(seg_rmr.raddr + sizeof(uint64_t) + seg_loc * sizeof(DirEntry), seg_rmr.rkey,
                             &dir->segs[seg_loc], sizeof(DirEntry), lmr->lkey);
        // Extend Dir
        uint64_t dir_size = 1 << dir->global_depth;
        memcpy(dir->segs + dir_size, dir->segs, dir_size * sizeof(DirEntry));
        dir->segs[new_seg_loc].local_depth = local_depth + 1;
        dir->segs[new_seg_loc].seg_ptr = new_seg_ptr;
        co_await conn->write(seg_rmr.raddr + sizeof(uint64_t) + dir_size * sizeof(DirEntry), seg_rmr.rkey,
                             dir->segs + dir_size, dir_size * sizeof(DirEntry), lmr->lkey);
        // Update Global Depthx
        dir->global_depth++;
        co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, &dir->global_depth, sizeof(uint64_t), lmr->lkey);
    }
    else
    {
        // Local Split
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

            // co_await conn->write(seg_rmr.raddr + sizeof(uint64_t) + cur_seg_loc * sizeof(DirEntry), seg_rmr.rkey,
            //                      dir->segs + cur_seg_loc, sizeof(DirEntry), lmr->lkey);
        }
        co_await conn->write(seg_rmr.raddr + sizeof(uint64_t), seg_rmr.rkey, dir->segs,
                             sizeof(DirEntry) * (1ull << dir->global_depth), lmr->lkey);
    }

    // Free Directory Lock && Segment Lock
#ifdef WO_WAIT_WRITE
    // Free Directory
    uint64_t *tmp = (uint64_t *)alloc.alloc(sizeof(uint64_t));
    *tmp = 0;
    wo_wait_conn->pure_write(lock_rmr.raddr, lock_rmr.rkey, tmp, sizeof(uint64_t), lmr->lkey);

    // Free Segment
    uintptr_t lock_ptr = lock_rmr.raddr + sizeof(uint64_t) + (sizeof(uint64_t)) * (seg_loc % num_lock);
    wo_wait_conn->pure_write(lock_ptr, lock_rmr.rkey, tmp, sizeof(uint64_t), lmr->lkey);
    op_cnt += 2;
#else
    co_await UnlockDir();
    uintptr_t lock_ptr = lock_rmr.raddr + sizeof(uint64_t) + (sizeof(uint64_t)) * (seg_loc % num_lock);
    uint64_t *tmp = (uint64_t *)alloc.alloc(sizeof(uint64_t));
    *tmp = 0;
    co_await conn->write(lock_ptr, lock_rmr.rkey, tmp, sizeof(uint64_t), lmr->lkey);
#endif
    co_await std::move(w_oldseg);
    co_await std::move(w_newseg);
    // log_info("co_await move");
}

/// @brief 设置Lock为1
/// @return return: 0-success, 1-split conflict
task<int> RACEClient::LockDir()
{
    // assert((connector.get_remote_addr())%8 == 0);
    if (co_await conn->cas_n(lock_rmr.raddr, lock_rmr.rkey, 0, 1))
    {
        co_return 0;
    }
    co_return 1;
}

task<> RACEClient::UnlockDir()
{
    // Set global split bit
    // assert((connector.get_remote_addr())%8 == 0);
    co_await conn->cas_n(lock_rmr.raddr, lock_rmr.rkey, 1, 0);
}

task<bool> RACEClient::search(Slice *key, Slice *value)
{
    uintptr_t slot_ptr;
    uint64_t slot;
    uint64_t cnt = 0;
Retry:
    alloc.ReSet(sizeof(Directory));
    // Calculate Segment
    uint64_t pattern_1 = (uint64_t)hash(key->data, key->len);
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_depth);
    uintptr_t segptr = dir->segs[segloc].seg_ptr;

    // Read Segment
    Segment* local_seg = (Segment*)alloc.alloc(sizeof(Segment));
    co_await conn->read(segptr, seg_rmr.rkey, local_seg, sizeof(Segment), lmr->lkey);

    // Check Depth
    if (dir->segs[segloc].local_depth != local_seg->local_depth){
        co_await sync_dir();
        goto Retry;
    }

    // Find Slot && Read KV
    for(uint64_t i = 0 ; i < SLOT_PER_SEGMENT ; i++){
        if (local_seg->slots[i]!=0 && local_seg->slots[i].fp == fp(pattern_1))
        {
            KVBlock *kv_block = (KVBlock *)alloc.alloc(local_seg->slots[i].len);
            co_await conn->read(ralloc.ptr(local_seg->slots[i].offset), seg_rmr.rkey, kv_block, local_seg->slots[i].len, lmr->lkey);
            if (memcmp(key->data, kv_block->data, key->len) == 0)
            {
                slot = *(uint64_t *)&(local_seg->slots[i]);
                value->len = kv_block->v_len;
                memcpy(value->data, kv_block->data + kv_block->k_len, value->len);
                co_return true;
            }
        }
    }
    log_err("[%lu:%lu]No match key for %lu",cli_id,coro_id,*(uint64_t*)key->data);
    co_return false;
}

task<> RACEClient::update(Slice *key, Slice *value)
{
    char data[1024];
    Slice ret_value;
    ret_value.data = data;
    KVBlock *kv_block = InitKVBlock(key, value, &alloc);
    uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 2;
    uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
    auto wkv = conn->write(kvblock_ptr, seg_rmr.rkey, kv_block, kvblock_len, lmr->lkey);

    uint64_t pattern_1, pattern_2;
    auto pattern = hash(key->data, key->len);
    pattern_1 = (uint64_t)pattern;
    pattern_2 = (uint64_t)(pattern >> 64);
    uint64_t cnt = 0;
Retry:
    alloc.ReSet(sizeof(Directory) + kvblock_len);
    // 1st RTT: Using RDMA doorbell batching to fetch two combined buckets
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_depth);
    uintptr_t segptr = dir->segs[segloc].seg_ptr;
    co_return;
}

task<> RACEClient::remove(Slice *key)
{
    char data[1024];
    Slice ret_value;
    ret_value.data = data;
    uint64_t cnt = 0;
Retry:
    alloc.ReSet(sizeof(Directory));
    // 1st RTT: Using RDMA doorbell batching to fetch two combined buckets
    uint64_t pattern_1, pattern_2;
    auto pattern = hash(key->data, key->len);
    pattern_1 = (uint64_t)pattern;
    pattern_2 = (uint64_t)(pattern >> 64);
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_depth);
    uintptr_t segptr = dir->segs[segloc].seg_ptr;
    co_return;
}

} // namespace RACEOP