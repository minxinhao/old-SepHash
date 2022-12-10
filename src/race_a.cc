#include "race_a.h"

namespace RACEA
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
    log_err("init");

    // Init locks
    // memset(lock_mr->addr, 0, lock_mr->length);
    char tmp[dev_mem_size] = {};
    lock_dm->memcpy_to_dm(lock_dm, 0, tmp, dev_mem_size);
    log_err("memset");

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
    ralloc.alloc(ALIGNED_SIZE); // 提前分配ALIGNED_SIZE，免得读取的时候越界

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
    // 模拟远端分配器信息
    Alloc server_alloc;
    server_alloc.Set((char *)seg_rmr.raddr, seg_rmr.rlen);
    server_alloc.alloc(sizeof(Directory));

    // 重置远端 Lock
    alloc.ReSet(sizeof(Directory)); // Make room for local_segment
    memset(dir, 0, sizeof(Directory));
    co_await conn->write(lock_rmr.raddr, lock_rmr.rkey, dir, dev_mem_size, lmr->lkey);

    // 重置远端segment
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
    // 重置远端 Directory
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
    uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 3;
    uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
    uint64_t retry_cnt = 0;
    perf.AddPerf("InitKv");
Retry:
    alloc.ReSet(sizeof(Directory) + kvblock_len);
    Slot *tmp = (Slot *)alloc.alloc(sizeof(Slot));
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_depth);
    uintptr_t segptr = dir->segs[segloc].seg_ptr;
    uintptr_t version_ptr = lock_rmr.raddr + sizeof(uint64_t) + (sizeof(uint64_t)) * (segloc % num_lock);

    // faa version for seg
    perf.StartPerf();
    uint64_t *version = (uint64_t *)alloc.alloc(sizeof(uint64_t));
    co_await conn->fetch_add(version_ptr, lock_rmr.rkey, *version, 1);
    kv_block->version = *version;
    perf.AddPerf("LockSeg");

    // read segment
    perf.StartPerf();
    Segment *tmp_segs = (Segment *)alloc.alloc(sizeof(Segment));
    co_await conn->read(segptr, seg_rmr.rkey, tmp_segs, sizeof(Segment), lmr->lkey);
    perf.AddPerf("ReadSeg");

    // log_err(
    //     "[%lu:%lu]insert key:%lu with local_depth:%u remote_depth:%lu global_depth:%lu at segloc:%lx with seg_ptr:%lx",
    //     cli_id, coro_id, *(uint64_t *)key->data, tmp_segs->local_depth, dir->segs[segloc].local_depth,
    //     dir->global_depth, segloc, segptr);
    if (dir->segs[segloc].local_depth != tmp_segs->local_depth)
    {
        perf.StartPerf();
        co_await sync_dir();
        perf.AddPerf("SyncDir");
        exit(-1);
        goto Retry;
    }

    // find free slot
    perf.StartPerf();
    uint64_t slot_id = linear_search((uint64_t *)tmp_segs->slots, SLOT_PER_SEGMENT - 1, 0);
    // uint64_t slot_id = linear_search_avx_ur((uint64_t *)tmp_segs->slots, SLOT_PER_SEGMENT - 1, 0);
    perf.AddPerf("FindSlot");
    // log_err("[%lu:%lu]insert key:%lu at slot:%lx of segloc:%lx", cli_id, coro_id, *(uint64_t *)key->data, slot_id,
    //         segloc);
    if (slot_id == -1)
    {
        // Split
        perf.AddCnt("SplitCnt");
        // log_err("[%lu:%lu]%s split for key:%lu with local_depth:%u global_depth:%lu at segloc:%lx", cli_id, coro_id,
        // (tmp_segs->local_depth == dir->global_depth) ? "gloabl" : "local", *(uint64_t *)key->data,
        // tmp_segs->local_depth, dir->global_depth, segloc);
        co_await Split(segloc, segptr, tmp_segs);
        goto Retry;
    }

    // write slot
    perf.AddCnt("SlotCnt");
    perf.StartPerf();
    uint64_t dep = tmp_segs->local_depth - (tmp_segs->local_depth % 4); // 按4对齐
    tmp->dep = pattern_1 >> dep;
    tmp->fp = fp(pattern_1);
    tmp->len = (kvblock_len + ALIGNED_SIZE - 1) / ALIGNED_SIZE;
    tmp->offset = ralloc.offset(kvblock_ptr);
    wo_wait_conn->pure_write(kvblock_ptr, seg_rmr.rkey, kv_block, kvblock_len, lmr->lkey); // write kv
#ifndef WO_WAIT_WRITE
    co_await conn->write(segptr + 2 * sizeof(uint64_t) + slot_id * sizeof(Slot), seg_rmr.rkey, tmp, sizeof(Slot),
                         lmr->lkey);
#else
    wo_wait_conn->pure_write(segptr + 2 * sizeof(uint64_t) + slot_id * sizeof(Slot), seg_rmr.rkey, tmp, sizeof(Slot),
                             lmr->lkey);
#endif
    perf.AddPerf("WriteSlot");
}

task<> RACEClient::Split(uint64_t seg_loc, uintptr_t seg_ptr, Segment *old_seg)
{
    perf.StartPerf();
    uint64_t local_depth = old_seg->local_depth;
    if (old_seg->local_depth == MAX_DEPTH)
    {
        log_err("Exceed MAX_DEPTH");
        exit(-1);
    }

    // Lock Segment && Move Data
    if (!co_await conn->cas_n(seg_ptr, seg_rmr.rkey, 0, 1))
    {
        // log_err("%d", __LINE__);
        co_return;
    }
    // Allocate New Seg and Init header && write to server
    perf.StartPerf();
    Segment *new_seg_1 = (Segment *)alloc.alloc(sizeof(Segment));
    Segment *new_seg_2 = (Segment *)alloc.alloc(sizeof(Segment));
    memset(new_seg_1, 0, sizeof(Segment));
    memset(new_seg_2, 0, sizeof(Segment));
    uint64_t new_seg_ptr_1 = ralloc.alloc(sizeof(Segment), true); // 按八字节对齐
    uint64_t new_seg_ptr_2 = ralloc.alloc(sizeof(Segment), true); // 按八字节对齐
    uint64_t first_seg_loc = seg_loc & ((1ull << local_depth) - 1);
    uint64_t new_seg_loc = (1ull << local_depth) | first_seg_loc;
    uint64_t pattern_1;
    uint64_t dep_off = (local_depth) % 4;
    bool dep_bit;
    new_seg_1->local_depth = local_depth + 1;
    new_seg_2->local_depth = local_depth + 1;
    for (uint64_t i = 0; i < SLOT_PER_SEGMENT; i++)
    {
        dep_bit = (old_seg->slots[i].dep >> dep_off) & 1;
        if (dep_off == 3)
        {
            // if dep_off == 3 (Have consumed all info in dep bits), read && construct new dep
            KVBlock *kv_block = (KVBlock *)alloc.alloc(old_seg->slots[i].len);
            co_await conn->read(ralloc.ptr(old_seg->slots[i].offset), seg_rmr.rkey, kv_block,
                                old_seg->slots[i].len * ALIGNED_SIZE, lmr->lkey);
            pattern_1 = (uint64_t)hash(kv_block->data, kv_block->k_len);
            uint64_t old_slot = *(uint64_t *)(&old_seg->slots[i]);
            old_seg->slots[i].dep = pattern_1 >> (local_depth + 1);
        }
        if (dep_bit)
        {
            // move data to new_seg
            new_seg_1->slots[i] = old_seg->slots[i];
        }
        else
        {
            new_seg_2->slots[i] = old_seg->slots[i];
        }
    }
    auto w_newseg_1 = conn->write(new_seg_ptr_1, seg_rmr.rkey, new_seg_1, sizeof(Segment), lmr->lkey);
    auto w_newseg_2 = conn->write(new_seg_ptr_2, seg_rmr.rkey, new_seg_2, sizeof(Segment), lmr->lkey);
    co_await std::move(w_newseg_2);
    co_await std::move(w_newseg_1);
    perf.AddPerf("InitBuc");

    // Edit Directory pointer
    while (co_await LockDir())
        ;
    perf.AddPerf("GetLock");
    /* 因为使用了MSB和提前分配充足空间的Directory，所以可以直接往后增加Directory Entry*/
    perf.StartPerf();
    co_await sync_dir(); // Global Split必须同步一次Dir，来保证之前没有被同步的DirEntry不会被写到远端。
    if (dir->segs[seg_loc].local_depth != local_depth)
    { // 已经被split
        co_await UnlockDir();
        co_await conn->cas_n(seg_ptr, seg_rmr.rkey, 1, 0);
        // log_err("%d",__LINE__);
        co_return;
    }
    if (local_depth == dir->global_depth)
    {
        // Update Old_seg depth
        // log_err("seg_loc:%lx new_seg_ptr_2:%lx", seg_loc,new_seg_ptr_2);
        dir->segs[seg_loc].seg_ptr = new_seg_ptr_2;
        dir->segs[seg_loc].local_depth = local_depth + 1;
        co_await conn->write(seg_rmr.raddr + sizeof(uint64_t) + seg_loc * sizeof(DirEntry), seg_rmr.rkey,
                             &dir->segs[seg_loc], sizeof(DirEntry), lmr->lkey);
        // Extend Dir
        // 这里可能会把前部分正在执行local_split的dir entry，移动到后半部分，使得其split_lock在不知情的情况下被设置为1
        // 仔细思考的话这样是必须得，因为后续新生成的segment会认为自己是一组独立的segment(根据设置的local__depth)
        // (好像也不会再出现额外的split了，指针指向的内容是一样)
        // 所以记得再把这部分隐藏的数据修改为0就行
        // 这部分大小应该不超过2-3吧，只能根据经验来设置了
        uint64_t dir_size = 1 << dir->global_depth;
        memcpy(dir->segs + dir_size, dir->segs, dir_size * sizeof(DirEntry));
        dir->segs[new_seg_loc].local_depth = local_depth + 1;
        dir->segs[new_seg_loc].seg_ptr = new_seg_ptr_1;
        // log_err("new_seg_loc:%lx new_seg_ptr_1:%lx", new_seg_loc,new_seg_ptr_1);
        co_await conn->write(seg_rmr.raddr + sizeof(uint64_t) + dir_size * sizeof(DirEntry), seg_rmr.rkey,
                             dir->segs + dir_size, dir_size * sizeof(DirEntry), lmr->lkey);
        // Update Global Depth
        dir->global_depth++;
        co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, &dir->global_depth, sizeof(uint64_t), lmr->lkey);
    }
    else
    {
        // Local split: Edit all directory share this seg_ptr
        // 笔记见备忘录
        uint64_t stride = (1llu) << (dir->global_depth - local_depth);
        uint64_t cur_seg_loc;
        for (uint64_t i = 0; i < stride; i++)
        {
            cur_seg_loc = (i << local_depth) | first_seg_loc;
            // log_err("cur_seg_loc:%lx", cur_seg_loc);
            if (i & 1)
                dir->segs[cur_seg_loc].seg_ptr = new_seg_ptr_1;
            else
                dir->segs[cur_seg_loc].seg_ptr = new_seg_ptr_2;
            dir->segs[cur_seg_loc].local_depth = local_depth + 1;
            co_await conn->write(seg_rmr.raddr + sizeof(uint64_t) + cur_seg_loc * sizeof(DirEntry), seg_rmr.rkey,
                                 dir->segs + cur_seg_loc, sizeof(DirEntry), lmr->lkey);
        }
    }
    co_await UnlockDir();
    co_await conn->cas_n(seg_ptr, seg_rmr.rkey, 1, 0);
    perf.AddPerf("EditDir");
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
    uint64_t pattern_1 = (uint64_t)hash(key->data, key->len);
Retry:
    alloc.ReSet(sizeof(Directory));
    // Calculate Segment
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_depth);
    uintptr_t segptr = dir->segs[segloc].seg_ptr;

    // log_err("[%lu:%lu]search key:%lu at segloc:%lx with seg_ptr:%lx", cli_id, coro_id, *(uint64_t *)key->data, segloc,segptr);

    // Read Segment
    Segment *local_seg = (Segment *)alloc.alloc(sizeof(Segment));
    co_await conn->read(segptr, seg_rmr.rkey, local_seg, sizeof(Segment), lmr->lkey);

    // Check Depth
    if (dir->segs[segloc].local_depth != local_seg->local_depth)
    {
        co_await sync_dir();
        goto Retry;
    }

    // Find Slot && Read KV
    uint64_t version = UINT64_MAX;
    uint64_t res_slot = UINT64_MAX;
    KVBlock *res = nullptr;
    for (uint64_t i = 0; i < SLOT_PER_SEGMENT; i++)
    {
        if (local_seg->slots[i] != 0 && local_seg->slots[i].fp == fp(pattern_1))
        {
            KVBlock *kv_block = (KVBlock *)alloc.alloc((local_seg->slots[i].len) * ALIGNED_SIZE);
            co_await conn->read(ralloc.ptr(local_seg->slots[i].offset), seg_rmr.rkey, kv_block,
                                (local_seg->slots[i].len) * ALIGNED_SIZE, lmr->lkey);
            // log_err("read key:%lu key-len:%lu version:%lu value-len:%lu value:%s", *(uint64_t*)kv_block->data, kv_block->k_len,kv_block->version,kv_block->v_len, kv_block->data + kv_block->k_len);
            if (memcmp(key->data, kv_block->data, key->len) == 0)
            {
                if (kv_block->version > version || version == UINT64_MAX)
                {
                    res_slot = i;
                    version = kv_block->version;
                    res = kv_block;
                }
            }
        }
    }
    if (res != nullptr && res->v_len != 0)
    {
        value->len = res->v_len;
        memcpy(value->data, res->data + res->k_len, value->len);
        co_return true;
    }
    log_err("[%lu:%lu]No match key for %lu", cli_id, coro_id, *(uint64_t *)key->data);
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

} // namespace RACEA