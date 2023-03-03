#include "split_op2b.h"
namespace SPLIT_OP2B
{

inline __attribute__((always_inline)) uint64_t fp(uint64_t pattern)
{
    return ((uint64_t)((pattern) >> 32) & ((1 << 8) - 1));
}

inline __attribute__((always_inline)) uint64_t fp2(uint64_t pattern)
{
    return ((uint64_t)((pattern) >> 24) & ((1 << 8) - 1));
}

inline __attribute__((always_inline)) uint64_t get_seg_loc(uint64_t pattern, uint64_t global_depth)
{
    return ((pattern) & ((1 << global_depth) - 1));
}

void print_mainseg(Slot *main_seg, uint64_t main_seg_len);

inline __attribute__((always_inline)) std::tuple<uint64_t, uint64_t> get_fp_bit(uint8_t fp1, uint8_t fp2)
{
    uint64_t fp = fp1;
    fp = fp << 8;
    fp = fp | fp2;
    fp = fp & ((1 << 10) - 1);
    uint64_t bit_loc = fp / 64;
    uint64_t bit_info = (fp % 64);
    bit_info = 1ll << bit_info;
    return std::make_tuple(bit_loc, bit_info);
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
    // memset(lock_mr->addr, 0, lock_mr->length);
    char tmp[dev_mem_size] = {};
    lock_dm->memcpy_to_dm(lock_dm, 0, tmp, dev_mem_size);
    log_err("memset");

    ser.start_serve();
}

void Server::Init()
{
    // Set MainTable to zero
    dir->global_depth = INIT_DEPTH;

    // Init CurTable
    CurSeg *cur_seg;
    for (uint64_t i = 0; i < (1 << dir->global_depth); i++)
    {
        dir->segs[i].cur_seg_ptr = (uintptr_t)alloc.alloc(sizeof(CurSeg));
        dir->segs[i].local_depth = INIT_DEPTH;
        cur_seg = (CurSeg *)dir->segs[i].cur_seg_ptr;
        memset(cur_seg, 0, sizeof(CurSeg));
        cur_seg->local_depth = INIT_DEPTH;
        cur_seg->sign = 1;
    }
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
    uint64_t rbuf_size = (seg_rmr.rlen - (1ul << 20) * 20) /
                            (config.num_machine * config.num_cli * config.num_coro); // 头部保留5GB，其他的留给client
    ralloc.SetRemote(
        seg_rmr.raddr + seg_rmr.rlen -
            rbuf_size * (config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id),
        rbuf_size, seg_rmr.raddr, seg_rmr.rlen);
    ralloc.alloc(ALIGNED_SIZE); // 提前分配ALIGNED_SIZE，免得读取的时候越界
    // log_err("ralloc start_addr:%lx offset_max:%lx", ralloc.raddr, ralloc.rsize);
    op_cnt = 0;

    // sync dir
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    memset(offset, 0, sizeof(uint8_t) * DIR_SIZE);
    cli->run(sync_dir());
    // dir->print();
}

Client::~Client()
{
}

task<> Client::reset_remote()
{
    // dir->print();
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

    CurSeg *cur_seg = (CurSeg *)alloc.alloc(sizeof(CurSeg));
    memset(cur_seg, 0, sizeof(CurSeg));
    cur_seg->local_depth = INIT_DEPTH;
    cur_seg->sign = 1;
    for (uint64_t i = 0; i < (1 << dir->global_depth); i++)
    {
        dir->segs[i].cur_seg_ptr = (uintptr_t)server_alloc.alloc(sizeof(CurSeg));
        dir->segs[i].local_depth = INIT_DEPTH;
        co_await conn->write(dir->segs[i].cur_seg_ptr, seg_rmr.rkey, cur_seg, size_t(sizeof(CurSeg)), lmr->lkey);
    }

    // 重置远端 Directory
    // dir->print();
    co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, dir, sizeof(Directory), lmr->lkey);
}

task<> Client::start(uint64_t total)
{
    co_await sync_dir();
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

task<> Client::sync_dir()
{
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, &dir->global_depth, 2 * sizeof(uint64_t), lmr->lkey);
    co_await conn->read(seg_rmr.raddr + sizeof(uint64_t), seg_rmr.rkey, dir->segs,
                        (1 << dir->global_depth) * sizeof(DirEntry), lmr->lkey);
}

task<> Client::insert(Slice *key, Slice *value)
{
    op_cnt++;
    uint64_t op_size = (1 << 20) * 1; // 为每个操作保留的空间，1MB够用了
    if (op_cnt % 2)
    {
        alloc.ReSet(sizeof(Directory) + op_size);
    }
    else
    {
        alloc.ReSet(sizeof(Directory));
    }
    uint64_t pattern_1 = (uint64_t)hash(key->data, key->len);
    KVBlock *kv_block = InitKVBlock(key, value, &alloc);
    uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 3;
    uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
    uint64_t retry_cnt1 = 0;
    uint64_t retry_cnt2 = 0;
    this->key_num = *(uint64_t *)key->data;
    uint64_t num = 10000000 / (1*4);
Retry:
    retry_cnt1++;
    retry_cnt2 = 0;
    if (op_cnt % 2)
    {
        alloc.ReSet(sizeof(Directory) + op_size + kvblock_len);
    }
    else
    {
        alloc.ReSet(sizeof(Directory) + kvblock_len);
    }
    // if(retry_cnt1>1000 && retry_cnt1%10000==0) log_err("[%lu:%lu:%lu]", cli_id, coro_id, this->key_num);
    Slot *tmp = (Slot *)alloc.alloc(sizeof(Slot));
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_depth);
    uintptr_t segptr = dir->segs[segloc].cur_seg_ptr;
    // read segment_meta
    CurSegMeta *curseg_meta = (CurSegMeta *)alloc.alloc(sizeof(CurSegMeta));
    auto read_meta = conn->read(segptr + sizeof(uint64_t), seg_rmr.rkey, curseg_meta, sizeof(CurSegMeta), lmr->lkey);
    Slot *curseg_slots = (Slot *)alloc.alloc(sizeof(Slot) * 2 * SLOT_BATCH_SIZE);
Retry2:
    retry_cnt2++;
    uintptr_t seg_offset = this->offset[segloc].offset;
    seg_offset = seg_offset % SLOT_PER_SEG;
    uintptr_t curseg_slots_ptr = segptr + sizeof(uint64_t) + sizeof(CurSegMeta) + seg_offset * sizeof(Slot);
    uint64_t slots_len = SLOT_PER_SEG - seg_offset;
    slots_len = (slots_len < (2 * SLOT_BATCH_SIZE)) ? slots_len : SLOT_BATCH_SIZE;
    auto read_slots = wo_wait_conn->read(curseg_slots_ptr, seg_rmr.rkey, curseg_slots, sizeof(Slot) * slots_len, lmr->lkey);
    
    // Check whether split happened on cur_table
    if (retry_cnt2 == 1)
    {
        co_await std::move(read_meta);
        if(curseg_meta->main_seg_ptr != this->offset[segloc].main_seg_ptr){
            this->offset[segloc].main_seg_ptr = curseg_meta->main_seg_ptr;
            this->offset[segloc].offset = 0;
            // if(retry_cnt1>1000 && retry_cnt1%10000==0) log_err("[%lu:%lu:%lu]re-read segloc:%lx ",cli_id, coro_id, this->key_num, segloc);
            co_await std::move(read_slots);
            if (curseg_meta->local_depth != dir->segs[segloc].local_depth || dir->global_depth < curseg_meta->local_depth)
            {
                // if(retry_cnt1>1000 && retry_cnt1%10000==0) log_err("[%lu:%lu:%lu]segloc:%lx local_depth:%lu remote local_depth:%lu", cli_id, coro_id, this->key_num, segloc, dir->segs[segloc].local_depth, curseg_meta->local_depth);
                co_await sync_dir();
            }
            goto Retry;
        }

        if (curseg_meta->local_depth != dir->segs[segloc].local_depth || dir->global_depth < curseg_meta->local_depth)
        {
            // if(retry_cnt1>1000 && retry_cnt1%10000==0) log_err("[%lu:%lu:%lu]segloc:%lx local_depth:%lu remote local_depth:%lu", cli_id, coro_id, this->key_num, segloc, dir->segs[segloc].local_depth, curseg_meta->local_depth);
            co_await sync_dir();
            co_await std::move(read_slots);
            goto Retry;
        }
    }

    // find free slot
    uint64_t sign = !curseg_meta->sign;
    uint64_t slot_id = -1;
    co_await std::move(read_slots);

    for (uint64_t i = 0; i < slots_len; i++)
    {
        if (curseg_slots[i].sign == sign)
        {
            slot_id = i;
            break;
        }
    }
    if (slot_id == -1)
    {
        uint8_t tmp = this->offset[segloc].offset;
        uint64_t old = this->offset[segloc].offset;
        old += slots_len;
        old = old % SLOT_PER_SEG;
        this->offset[segloc].offset = old;
        // if(retry_cnt1>1000 && retry_cnt1%10000==0){
        //     log_err("[%lu:%lu:%10lu]segloc:%lx edit segoffset:%u to %u with slot_len:%lu slot_id:%lu and seg_meta: depth:%lu main_ptr:%lx", cli_id, coro_id, this->key_num,segloc,tmp,this->offset[segloc].offset,slots_len,slot_id,curseg_meta->local_depth,curseg_meta->main_seg_ptr);
        // }
        if(retry_cnt2 >= RETRY_LIMIT || old == 0){
            co_await Split(segloc, segptr, curseg_meta);
            goto Retry;
        }   
        // goto Retry2;
        goto Retry;
    }
    else if(slot_id == slots_len-1){
        uint8_t tmp = this->offset[segloc].offset;
        uint64_t old = this->offset[segloc].offset;
        old += slots_len;
        old = old % SLOT_PER_SEG;
        this->offset[segloc].offset = old;
        // if(retry_cnt1>1000 && retry_cnt1%10000==0)    log_err("[%lu:%lu:%10lu]segloc:%lx edit segoffset:%u to %u with slot_len:%lu slot_id:%lu and seg_meta: depth:%lu main_ptr:%lx", cli_id, coro_id, this->key_num,segloc,tmp,this->offset[segloc].offset,slots_len,slot_id,curseg_meta->local_depth,curseg_meta->main_seg_ptr);
    }
    // write slot
    uint64_t dep = curseg_meta->local_depth - (curseg_meta->local_depth % 4); // 按4对齐
    tmp->dep = pattern_1 >> dep;
    tmp->fp = fp(pattern_1);
    tmp->len = (kvblock_len + ALIGNED_SIZE - 1) / ALIGNED_SIZE;
    tmp->sign = curseg_meta->sign;
    tmp->offset = ralloc.offset(kvblock_ptr);

    // faa version for seg
    uintptr_t version_ptr = lock_rmr.raddr + sizeof(uint64_t) + (sizeof(uint64_t)) * (segloc % num_lock);
    uint64_t *version = (uint64_t *)alloc.alloc(sizeof(uint64_t));
    auto fetch_ver = wo_wait_conn->fetch_add(version_ptr, lock_rmr.rkey, *version, 1);

    uintptr_t slot_ptr = curseg_slots_ptr + slot_id * sizeof(Slot);
    if (!co_await conn->cas_n(slot_ptr, seg_rmr.rkey,
                                (uint64_t)(curseg_slots[slot_id]), *tmp))
    {
        co_await std::move(fetch_ver);
        goto Retry;
    }
    
    // write kv
    co_await std::move(fetch_ver);
    kv_block->version = *version;
    wo_wait_conn->pure_write(kvblock_ptr, seg_rmr.rkey, kv_block, kvblock_len, lmr->lkey);
    
    // write fp2
    tmp->fp_2 = fp2(pattern_1);
    wo_wait_conn->pure_write(slot_ptr + sizeof(uint64_t), seg_rmr.rkey,
                                &tmp->fp_2, sizeof(uint8_t), lmr->lkey);
    

    if(seg_offset+slot_id == SLOT_PER_SEG-1){
        // Split
        co_await Split(segloc, segptr, curseg_meta);
        co_return;
    }
    
    // TODO:可以搬到前面隐藏起来
    // TODO:思考了一下，这里换成pure_write影响很小
    // write fp bitmap
    auto [bit_loc, bit_info] = get_fp_bit(tmp->fp, tmp->fp_2);
    uintptr_t fp_ptr = segptr + (4 + bit_loc) * sizeof(uint64_t);
    while ((curseg_meta->fp_bitmap[bit_loc]&bit_info)==0 )
    {
        if(co_await conn->cas(fp_ptr, seg_rmr.rkey,
                            curseg_meta->fp_bitmap[bit_loc], curseg_meta->fp_bitmap[bit_loc]| bit_info)){
            break;
        }
    }

    // if(retry_cnt1>1000 && retry_cnt1%10000==0)
    //     log_err("[%lu:%lu:%10lu]segloc:%lx\tseg_offset:%lu\tslot_id:%lu\tsign:%d  seg_meta: depth:%lu main_ptr:%lx sign:%d", cli_id, coro_id, this->key_num, segloc,seg_offset,seg_offset+slot_id,curseg_slots[slot_id].sign,curseg_meta->local_depth,curseg_meta->main_seg_ptr,curseg_meta->sign);
}

void merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg)
{
    std::sort(data, data + len);
    uint8_t sign = data[0].sign;
    int off_1 = 0, off_2 = 0;
    for (uint64_t i = 0; i < len + old_seg_len; i++)
    {
        if(data[off_1].sign != sign){
            log_err("wrong sign");
            print_mainseg(data,len);
            exit(-1);
        }
        if (data[off_1].fp <= old_seg[off_2].fp)
        {
            new_seg[i] = data[off_1];
            off_1++;
        }
        else
        {
            new_seg[i] = old_seg[off_2];
            off_2++;
        }
        if (off_1 >= len || off_2 >= old_seg_len)
            break;
    }
    if (off_1 < len)
    {
        memcpy(new_seg + old_seg_len + off_1, data + off_1, (len - off_1) * sizeof(Slot));
    }
    else if (off_2 < old_seg_len)
    {
        memcpy(new_seg + len + off_2, old_seg + off_2, (old_seg_len - off_2) * sizeof(Slot));
    }
}

void print_mainseg(Slot *main_seg, uint64_t main_seg_len)
{
    log_err("main_seg_len:%lu", main_seg_len);
    for (uint64_t i = 0; i < main_seg_len; i++)
    {
        main_seg[i].print();
    }
}

void print_fpinfo(FpInfo *fp_info)
{
    for (uint64_t i = 0; i <= UINT8_MAX; i++)
    {
        log_err("FP:%lu NUM:%d", i, fp_info[i].num);
    }
}

void cal_fpinfo(Slot *main_seg, uint64_t main_seg_len, FpInfo *fp_info)
{
    double avg = (1.0 * main_seg_len) / UINT8_MAX;
    uint64_t base_off = 0;
    uint64_t base_index = 0;
    uint64_t predict;
    uint8_t prev_fp = 0;
    uint64_t max_error = 32;
    uint64_t correct_cnt = 0;
    uint64_t err;
    for (uint64_t i = 0; i < main_seg_len; i++)
    {
        fp_info[main_seg[i].fp].num++;
    }
}

task<> Client::Split(uint64_t seg_loc, uintptr_t seg_ptr, CurSegMeta *old_seg_meta)
{
    uint64_t local_depth = old_seg_meta->local_depth;
    uint64_t global_depth = dir->global_depth;
    uint64_t main_seg_ptr = old_seg_meta->main_seg_ptr;
    uint64_t num = 10000000 / (1*4);

    // 1. Lock Segment && Move Data
    // log_err("[%lu:%lu:%lu] segloc:%lx", cli_id, coro_id, this->key_num,seg_loc);
    if (!co_await conn->cas_n(seg_ptr, seg_rmr.rkey, 0, 1))
    {
        // co_await sync_dir();
        // log_err("[%lu:%lu:%lu] lock fail at segloc:%lx ", cli_id, coro_id, this->key_num,seg_loc);
        co_return;
    }
    // Read CurSegment
    CurSeg *cur_seg = (CurSeg *)alloc.alloc(sizeof(CurSeg));
    // log_err("[%lu:%lu:%lu] lock succeed at segloc:%lx", cli_id, coro_id, this->key_num,seg_loc);
    co_await conn->read(seg_ptr, seg_rmr.rkey, cur_seg, sizeof(CurSeg), lmr->lkey);
    dir->segs[seg_loc].main_seg_ptr = cur_seg->main_seg_ptr;
    dir->segs[seg_loc].main_seg_len = cur_seg->main_seg_len;
    dir->segs[seg_loc].local_depth = cur_seg->local_depth;
    // 1.1 判断main_seg_ptr是否变化;所有的split操作都会修改main_seg_ptr
    if (dir->segs[seg_loc].main_seg_ptr != old_seg_meta->main_seg_ptr || dir->segs[seg_loc].local_depth != local_depth)
    {
        // log_err("[%lu:%lu:%lu] inconsistent ptr at segloc:%lx local-lp:%lu remote-lp:%lu local-main_ptr:%lx remote-main_ptr:%lx",cli_id,coro_id,this->key_num,seg_loc,local_depth,dir->segs[seg_loc].local_depth,old_seg_meta->main_seg_ptr,dir->segs[seg_loc].main_seg_ptr);
        // exit(-1);
        co_await conn->cas_n(seg_ptr, seg_rmr.rkey, 1, 0);
        co_await sync_dir(); // 注释掉这个发生search miss，性能也不提升,甚至下降
        co_return;
    }
    // log_err("[%lu:%lu:%lu] split at segloc:%lx",cli_id,coro_id,this->key_num,seg_loc);

    // 1.2 Read Main-Segment
    uint64_t main_seg_size = sizeof(Slot) * dir->segs[seg_loc].main_seg_len;
    MainSeg *main_seg = (MainSeg *)alloc.alloc(main_seg_size);
    co_await conn->read(dir->segs[seg_loc].main_seg_ptr, seg_rmr.rkey, main_seg, main_seg_size, lmr->lkey);

    // TODO : 在Read数据之后，就可以修改CurSeg的sign，来允许后续写入
    // 1.3 sort segment && write
    MainSeg *new_main_seg = (MainSeg *)alloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG);
    merge_insert(cur_seg->slots, SLOT_PER_SEG, main_seg->slots, dir->segs[seg_loc].main_seg_len, new_main_seg->slots);
    FpInfo fp_info[MAX_FP_INFO] = {};
    cal_fpinfo(new_main_seg->slots, SLOT_PER_SEG + dir->segs[seg_loc].main_seg_len, fp_info);
    if (dir->segs[seg_loc].main_seg_len >= MAX_MAIN_SIZE)
    {
        // Split
        // Split Main Segment
        // log_err("[%lu:%lu:%lu]", cli_id, coro_id, this->key_num);
        MainSeg *new_seg_1 = (MainSeg *)alloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG);
        MainSeg *new_seg_2 = (MainSeg *)alloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG);
        bool dep_bit = false;
        uint64_t dep_off = (local_depth) % 4;
        uint64_t pattern_1;
        uint64_t off1 = 0, off2 = 0;
        KVBlock *kv_block = (KVBlock *)alloc.alloc(7 * ALIGNED_SIZE);
        for (uint64_t i = 0; i < (SLOT_PER_SEG + dir->segs[seg_loc].main_seg_len); i++)
        {
            // dep_bit = !dep_bit; // 经测试，split_op的insert性能是由并发策略影响的，而不是read kv
            dep_bit = (new_main_seg->slots[i].dep >> dep_off) & 1;
            // log_err("[%lu:%lu:%lu] segloc:%lx total:%lu i:%lu", cli_id, coro_id, this->key_num,seg_loc,SLOT_PER_SEG + dir->segs[seg_loc].main_seg_len,i);
            if (dep_off == 3)
            {
                // if dep_off == 3 (Have consumed all info in dep bits), read && construct new dep
                co_await conn->read(ralloc.ptr(new_main_seg->slots[i].offset), seg_rmr.rkey, kv_block,
                                    new_main_seg->slots[i].len * ALIGNED_SIZE, lmr->lkey);
                pattern_1 = (uint64_t)hash(kv_block->data, kv_block->k_len);
                // pattern_1 = (uint64_t)hash(kv_block->data, 8);
                new_main_seg->slots[i].dep = pattern_1 >> (local_depth + 1);
                if (kv_block->k_len != 8)
                {
                    new_main_seg->slots[i].print();
                    uint64_t cros_seg_loc = get_seg_loc(pattern_1, local_depth+1);
                    log_err("[%lu:%lu:%lu]kv_block k_len:%lu v_len:%lu key:%lu value:%s cros_seg_loc:%lx",cli_id,coro_id,this->key_num, kv_block->k_len, kv_block->v_len, *(uint64_t *)kv_block->data, kv_block->data + 8, cros_seg_loc);
                    exit(-1);
                }
            }
            if (dep_bit)
            {
                // move data to new_seg
                new_seg_2->slots[off2++] = new_main_seg->slots[i];
            }
            else
            {
                new_seg_1->slots[off1++] = new_main_seg->slots[i];
            }
        }
        FpInfo fp_info1[MAX_FP_INFO] = {};
        FpInfo fp_info2[MAX_FP_INFO] = {};
        cal_fpinfo(new_seg_1->slots, off1, fp_info1);
        cal_fpinfo(new_seg_2->slots, off2, fp_info2);
        // log_err("[%lu:%lu:%lu] seg_loc:%lx", cli_id, coro_id, this->key_num,seg_loc);

        // Alloc new cur table
        uintptr_t new_cur_ptr = ralloc.alloc(sizeof(CurSeg), true);
        CurSeg *new_cur_seg = (CurSeg *)alloc.alloc(sizeof(CurSeg));
        memset(new_cur_seg, 0, sizeof(CurSeg));
        new_cur_seg->local_depth = local_depth + 1;
        new_cur_seg->sign = 1;
        new_cur_seg->main_seg_ptr = ralloc.alloc(sizeof(Slot) * off2);
        new_cur_seg->main_seg_len = off2;
        co_await conn->write(new_cur_ptr, seg_rmr.rkey, new_cur_seg, sizeof(CurSeg), lmr->lkey);
        co_await conn->write(new_cur_seg->main_seg_ptr, seg_rmr.rkey, new_seg_2, sizeof(Slot) * off2, lmr->lkey);

        // Edit Dir
        // log_err("[%lu:%lu:%lu] seg_loc:%lx", cli_id, coro_id, this->key_num,seg_loc);
        while (co_await LockDir()){
            // log_err("[%lu:%lu:%lu] seg_loc:%lx fail to lock_dir", cli_id, coro_id, this->key_num,seg_loc);
        }
            // log_err("[%lu:%lu:%lu] seg_loc:%lx succeed to lock_dir", cli_id, coro_id, this->key_num,seg_loc);
        co_await sync_dir(); // Global Split必须同步一次Dir，来保证之前没有被同步的DirEntry不会被写到远端。
        if (dir->segs[seg_loc].local_depth != local_depth || dir->global_depth != global_depth)
        { 
            // 应该只需要更新Global Depth就好了
            // log_err("[%lu:%lu:%lu] inconsistent ptr at segloc:%lx local-lp:%lu remote-lp:%lu local-gp:%lu remote-gp:%lu ",cli_id,coro_id,this->key_num,seg_loc,local_depth,dir->segs[seg_loc].local_depth,global_depth,dir->global_depth);
            
            // co_await UnlockDir();
            // co_await conn->cas_n(seg_ptr, seg_rmr.rkey, 1, 0);
            // co_return;

            // 
        }
        // log_err("[%lu:%lu:%lu] seg_loc:%lx", cli_id, coro_id, this->key_num,seg_loc);

        // 将Old_Seg放置到Lock之后，避免重复修改？
        cur_seg->main_seg_ptr = ralloc.alloc(sizeof(Slot) * off1);
        cur_seg->main_seg_len = off1;
        cur_seg->local_depth = local_depth + 1;
        cur_seg->sign = !cur_seg->sign; // 对old cur_seg的清空放到最后?保证同步。
        memset(cur_seg->fp_bitmap, 0, sizeof(uint64_t) * 16);
        co_await conn->write(cur_seg->main_seg_ptr, seg_rmr.rkey, new_seg_1, sizeof(Slot) * off1, lmr->lkey);
        co_await conn->write(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, (3 + 16) * sizeof(uint64_t),
                                lmr->lkey);
        
        // log_err("[%lu:%lu:%lu] seg_loc:%lx", cli_id, coro_id, this->key_num,seg_loc);
        uint64_t first_seg_loc = seg_loc & ((1ull << local_depth) - 1);
        uint64_t new_seg_loc = (1ull << local_depth) | first_seg_loc;
        if (local_depth == dir->global_depth)
        {
            if (local_depth == MAX_DEPTH)
            {
                log_err("Exceed MAX_DEPTH");
                exit(-1);
            }
            // log_err("[%lu:%lu:%lu] segloc:%lx", cli_id, coro_id, this->key_num,seg_loc);
            dir->segs[seg_loc].main_seg_ptr = cur_seg->main_seg_ptr;
            dir->segs[seg_loc].main_seg_len = cur_seg->main_seg_len;
            dir->segs[seg_loc].local_depth = local_depth + 1;
            this->offset[seg_loc].offset = 0;
            memcpy(dir->segs[seg_loc].fp, fp_info1, sizeof(FpInfo) * MAX_FP_INFO);
            co_await conn->write(seg_rmr.raddr + sizeof(uint64_t) + seg_loc * sizeof(DirEntry), seg_rmr.rkey,
                                    &dir->segs[seg_loc], sizeof(DirEntry), lmr->lkey);
            // Extend Dir
            uint64_t dir_size = 1 << dir->global_depth;
            memcpy(dir->segs + dir_size, dir->segs, dir_size * sizeof(DirEntry));
            memcpy(offset + dir_size, offset, dir_size * sizeof(uint8_t));
            // log_err("[%lu:%lu:%lu] seg_loc:%lx", cli_id, coro_id, this->key_num,seg_loc);
            dir->segs[new_seg_loc].local_depth = local_depth + 1;
            dir->segs[new_seg_loc].cur_seg_ptr = new_cur_ptr;
            dir->segs[new_seg_loc].main_seg_ptr = new_cur_seg->main_seg_ptr;
            dir->segs[new_seg_loc].main_seg_len = new_cur_seg->main_seg_len;
            this->offset[new_seg_loc].offset = 0;
            memcpy(dir->segs[new_seg_loc].fp, fp_info2, sizeof(FpInfo) * MAX_FP_INFO);
            co_await conn->write(seg_rmr.raddr + sizeof(uint64_t) + dir_size * sizeof(DirEntry), seg_rmr.rkey,
                                    dir->segs + dir_size, dir_size * sizeof(DirEntry), lmr->lkey);
            // Update Global Depth
            dir->global_depth++;
            co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, &dir->global_depth, sizeof(uint64_t), lmr->lkey);
            log_err("[%lu:%lu:%lu]Global SPlit At segloc:%lx depth:%lu to :%lu with new_main_seg_ptr:%lx", cli_id, coro_id, this->key_num, seg_loc, local_depth, local_depth + 1, cur_seg->main_seg_ptr);
            log_err("[%lu:%lu:%lu]Global SPlit At new_seg_loc:%lx depth:%lu to :%lu with main_seg_ptr:%lx", cli_id, coro_id, this->key_num, new_seg_loc, local_depth, local_depth + 1, new_cur_seg->main_seg_ptr);
        }
        else
        {
            // Local split: Edit all directory share this seg_ptr
            // 笔记见备忘录
            // log_err("[%lu:%lu:%lu] segloc:%lx", cli_id, coro_id, this->key_num,seg_loc);
            uint64_t stride = (1llu) << (dir->global_depth - local_depth);
            uint64_t cur_seg_loc;
            for (uint64_t i = 0; i < stride; i++)
            {
                cur_seg_loc = (i << local_depth) | first_seg_loc;
                if (i & 1)
                {
                    dir->segs[cur_seg_loc].cur_seg_ptr = new_cur_ptr;
                    dir->segs[cur_seg_loc].main_seg_ptr = new_cur_seg->main_seg_ptr;
                    dir->segs[cur_seg_loc].main_seg_len = new_cur_seg->main_seg_len;
                    memcpy(dir->segs[cur_seg_loc].fp, fp_info2, sizeof(FpInfo) * MAX_FP_INFO);
                    // if((this->key_num - (coro_id*num)) > 80000 && (this->key_num - (coro_id*num)) <= 100000)
                        // log_err("[%lu:%lu:%lu]Local SPlit At segloc:%lx depth:%lu to :%lu with new main_seg_ptr:%lx", cli_id, coro_id, this->key_num, cur_seg_loc, local_depth, local_depth + 1, new_cur_seg->main_seg_ptr);
                }
                else
                {
                    dir->segs[cur_seg_loc].cur_seg_ptr = seg_ptr;
                    dir->segs[cur_seg_loc].main_seg_ptr = cur_seg->main_seg_ptr;
                    dir->segs[cur_seg_loc].main_seg_len = cur_seg->main_seg_len;
                    memcpy(dir->segs[cur_seg_loc].fp, fp_info1, sizeof(FpInfo) * MAX_FP_INFO);
                    // if((this->key_num - (coro_id*num)) > 80000 && (this->key_num - (coro_id*num)) <= 100000)
                        // log_err("[%lu:%lu:%lu]Local SPlit At segloc:%lx depth:%lu to :%lu with new main_seg_ptr:%lx", cli_id, coro_id, this->key_num, cur_seg_loc, local_depth, local_depth + 1, cur_seg->main_seg_ptr);
                }
                this->offset[cur_seg_loc].offset = 0;
                dir->segs[cur_seg_loc].local_depth = local_depth + 1;
                co_await conn->write(seg_rmr.raddr + sizeof(uint64_t) + cur_seg_loc * sizeof(DirEntry), seg_rmr.rkey,
                                        dir->segs + cur_seg_loc, sizeof(DirEntry), lmr->lkey);
            }
        }
        co_await UnlockDir();
        cur_seg->split_lock = 0;
        co_await conn->write(seg_ptr, seg_rmr.rkey, &cur_seg->split_lock, sizeof(uint64_t), lmr->lkey);
        // log_err("[%lu:%lu:%lu]", cli_id, coro_id, this->key_num);
    }
    else
    {
        // Merge
        // log_err("[%lu:%lu:%lu] segloc:%lx", cli_id, coro_id, this->key_num,seg_loc);
        uintptr_t new_main_ptr = ralloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG, true);
        co_await conn->write(new_main_ptr, seg_rmr.rkey, new_main_seg->slots,
                                main_seg_size + sizeof(Slot) * SLOT_PER_SEG, lmr->lkey);

        cur_seg->main_seg_ptr = new_main_ptr;
        cur_seg->main_seg_len = main_seg_size / sizeof(Slot) + SLOT_PER_SEG;
        cur_seg->sign = !cur_seg->sign;
        this->offset[seg_loc].offset = 0;
        memset(cur_seg->fp_bitmap, 0, sizeof(uint64_t) * 16);
        co_await conn->write(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, (3 + 16) * sizeof(uint64_t),
                                lmr->lkey);
        while (co_await LockDir())
            ;
        uint64_t stride = (1llu) << (dir->global_depth - local_depth);
        uint64_t cur_seg_loc;
        uint64_t first_seg_loc = seg_loc & ((1ull << local_depth) - 1);
        for (uint64_t i = 0; i < stride; i++)
        {
            cur_seg_loc = (i << local_depth) | first_seg_loc;
            dir->segs[cur_seg_loc].main_seg_ptr = new_main_ptr;
            dir->segs[cur_seg_loc].main_seg_len = cur_seg->main_seg_len;
            this->offset[cur_seg_loc].offset = 0;
            memcpy(dir->segs[cur_seg_loc].fp, fp_info, sizeof(FpInfo) * MAX_FP_INFO);
            co_await conn->write(
                seg_rmr.raddr + sizeof(uint64_t) + cur_seg_loc * sizeof(DirEntry) + 2 * sizeof(uint64_t), seg_rmr.rkey,
                &dir->segs[cur_seg_loc].main_seg_ptr, 2 * sizeof(uint64_t) + sizeof(FpInfo) * MAX_FP_INFO, lmr->lkey);
        }
        uint64_t dir_size = (1 << dir->global_depth);
        for (uint64_t i = 0; i < stride; i++)
        {
            cur_seg_loc = (i << local_depth) | first_seg_loc;
            dir->segs[cur_seg_loc + dir_size].main_seg_ptr = new_main_ptr;
            dir->segs[cur_seg_loc + dir_size].main_seg_len = cur_seg->main_seg_len;
            this->offset[cur_seg_loc + dir_size].offset = 0;
            memcpy(dir->segs[cur_seg_loc + dir_size].fp, fp_info, sizeof(FpInfo) * MAX_FP_INFO);
            co_await conn->write(seg_rmr.raddr + sizeof(uint64_t) + (cur_seg_loc + dir_size) * sizeof(DirEntry) +
                                        2 * sizeof(uint64_t),
                                    seg_rmr.rkey, &dir->segs[cur_seg_loc + dir_size].main_seg_ptr,
                                    2 * sizeof(uint64_t) + sizeof(FpInfo) * MAX_FP_INFO, lmr->lkey);
        }
        co_await UnlockDir();
        // 1.4 FreeLock && Change Sign
        cur_seg->split_lock = 0;
        co_await conn->write(seg_ptr, seg_rmr.rkey, &cur_seg->split_lock, sizeof(uint64_t), lmr->lkey);
        // if((this->key_num - (coro_id*num)) > 80000 && (this->key_num - (coro_id*num)) <= 100000)
            // log_err("[%lu:%lu:%lu]Merge At segloc:%lx depth:%lu with new_main_ptr:%lx",cli_id,coro_id,this->key_num,cur_seg_loc,local_depth,new_main_ptr);
    }
}

task<int> Client::LockDir()
{
    // assert((connector.get_remote_addr())%8 == 0);
    if (co_await conn->cas_n(lock_rmr.raddr, lock_rmr.rkey, 0, 1))
    {
        co_return 0;
    }
    int a = 1; // 不知道为啥在33上不加点东西这个函数会卡住
    co_return 1;
}
task<> Client::UnlockDir()
{
    co_await conn->cas_n(lock_rmr.raddr, lock_rmr.rkey, 1, 0);
}

task<bool> Client::search(Slice *key, Slice *value)
{
    uintptr_t slot_ptr;
    uint64_t slot;
    uint64_t cnt = 0;
    uint64_t pattern_1 = (uint64_t)hash(key->data, key->len);
    uint64_t tmp_fp = fp(pattern_1);
    this->key_num = *(uint64_t *)key->data;
Retry:
    alloc.ReSet(sizeof(Directory));
    // Calculate Segment
    uint64_t segloc = get_seg_loc(pattern_1, dir->global_depth);
    uintptr_t cur_seg_ptr = dir->segs[segloc].cur_seg_ptr;
    uintptr_t main_seg_ptr = dir->segs[segloc].main_seg_ptr;
    uint64_t main_seg_len = dir->segs[segloc].main_seg_len;
    uint64_t base_index = 0;
    uint64_t base_off = 0;

    CurSegMeta *cur_segmeta = (CurSegMeta *)alloc.alloc(sizeof(CurSegMeta));
    auto read_segmeta = conn->read(cur_seg_ptr + sizeof(uint64_t), seg_rmr.rkey, cur_segmeta, sizeof(CurSegMeta), lmr->lkey);
    uint64_t start_pos = 0;
    uint64_t end_pos = main_seg_len;
    for (uint64_t i = 0; i <= UINT8_MAX; i++)
    {
        if (i == UINT8_MAX || i >= tmp_fp)
        {
            break;
        }
        start_pos += dir->segs[segloc].fp[i].num;
    }
    end_pos = start_pos + dir->segs[segloc].fp[tmp_fp].num;
    uint64_t main_size = (end_pos - start_pos) * sizeof(Slot);
    Slot *main_seg = (Slot *)alloc.alloc(main_size);
    auto read_main_seg =
        wo_wait_conn->read(main_seg_ptr + start_pos * sizeof(Slot), seg_rmr.rkey, main_seg, main_size, lmr->lkey);

    co_await std::move(read_segmeta);
    // Check Depth && MainSeg
    if (dir->segs[segloc].local_depth != cur_segmeta->local_depth || cur_segmeta->main_seg_ptr != main_seg_ptr)
    {
        log_err("[%lu:%lu:%lu]Inconsistent Depth at segloc:%lx with local local_depth:%lu remote local_depth:%lu and "
                "global_depth:%lu seg_ptr:%lx local main_seg_ptr:%lx remote main_seg_ptr:%lx",
                cli_id, coro_id, key_num, segloc, dir->segs[segloc].local_depth, cur_segmeta->local_depth,
                dir->global_depth, cur_seg_ptr, dir->segs[segloc].main_seg_ptr, cur_segmeta->main_seg_ptr);
        co_await sync_dir();
        co_await std::move(read_main_seg);
        goto Retry;
    }

    // Find Slot && Read KV
    uint64_t version = UINT64_MAX;
    uint64_t res_slot = UINT64_MAX;
    KVBlock *res = nullptr;
    KVBlock *kv_block = (KVBlock *)alloc.alloc(7 * ALIGNED_SIZE);
    uint64_t dep = dir->segs[segloc].local_depth - (dir->segs[segloc].local_depth % 4); // 按4对齐
    uint8_t dep_info = (pattern_1 >> dep) & 0xf;

    // 判断Key是否出现在CurSeg中
    auto [bit_loc, bit_info] = get_fp_bit(fp(pattern_1), fp2(pattern_1));
    rdma_future read_seg_slots;
    Slot *curseg_slots;
    if (cur_segmeta->fp_bitmap[bit_loc] & bit_info == 1)
    {
        curseg_slots = (Slot *)alloc.alloc(sizeof(Slot) * SLOT_PER_SEG);
        auto read_curseg = conn->read(cur_seg_ptr + sizeof(uint64_t) + sizeof(CurSegMeta), seg_rmr.rkey, curseg_slots, sizeof(Slot) * SLOT_PER_SEG, lmr->lkey);
        read_seg_slots.conn = read_curseg.conn;
        read_seg_slots.cor = read_curseg.cor;
    }

    // log_err("Main");
    co_await std::move(read_main_seg);
    if (res == nullptr)
    {
        for (uint64_t i = 0; i < end_pos - start_pos; i++)
        {
            // main_seg[i].print();
            if (main_seg[i] != 0 && main_seg[i].fp == tmp_fp && main_seg[i].dep == dep_info &&
                main_seg[i].fp_2 == fp2(pattern_1))
            {
                co_await conn->read(ralloc.ptr(main_seg[i].offset), seg_rmr.rkey, kv_block,
                                    (main_seg[i].len) * ALIGNED_SIZE, lmr->lkey);
                // log_err("[%lu:%lu:%lu] read %lu at
                // main_seg:%lu",cli_id,coro_id,key_num,*(uint64_t*)kv_block->data,i+end_pos);
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
    }

    // Search In CurSeg
    if (cur_segmeta->fp_bitmap[bit_loc] & bit_info == 1)
    {
        co_await std::move(read_seg_slots);
        // log_err("[%lu:%lu:%lu] search in seg:%lu", cli_id, coro_id, this->key_num, segloc);
        for (uint64_t i = 0; i < SLOT_PER_SEG; i++)
        {
            // cur_seg->slots[i].print();
            if (curseg_slots[i] != 0 && curseg_slots[i].fp == tmp_fp && curseg_slots[i].dep == dep_info &&
                curseg_slots[i].fp_2 == fp2(pattern_1))
            {
                co_await conn->read(ralloc.ptr(curseg_slots[i].offset), seg_rmr.rkey, kv_block, (curseg_slots[i].len) * ALIGNED_SIZE, lmr->lkey);
                // log_err("[%lu:%lu:%lu] read %lu at cur_seg",cli_id,coro_id,key_num,*(uint64_t*)kv_block->data);
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
    }
    if (res == nullptr)
    {
        // 不带fp2再查一遍，有可能fp2没写入成功就被merge/split了
        //  log_err("Main");
        for (uint64_t i = 0; i < end_pos - start_pos; i++)
        {
            // main_seg[i].print();
            if (main_seg[i] != 0 && main_seg[i].fp == tmp_fp && main_seg[i].dep == dep_info)
            {
                co_await conn->read(ralloc.ptr(main_seg[i].offset), seg_rmr.rkey, kv_block,
                                    (main_seg[i].len) * ALIGNED_SIZE, lmr->lkey);
                // log_err("[%lu:%lu:%lu] read %lu at
                // main_seg:%lu",cli_id,coro_id,key_num,*(uint64_t*)kv_block->data,i+end_pos);
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
    }

    if (res != nullptr && res->v_len != 0)
    {
        value->len = res->v_len;
        memcpy(value->data, res->data + res->k_len, value->len);
        co_return true;
    }

    log_err("[%lu:%lu:%lu] No match key at segloc:%lx with local_depth:%lu and global_depth:%lu seg_ptr:%lx "
            "main_seg_ptr:%lx",
            cli_id, coro_id, key_num, segloc, dir->segs[segloc].local_depth, dir->global_depth, cur_seg_ptr,
            dir->segs[segloc].main_seg_ptr);

    // std::string tmp_value = std::string(32, '1');
    // value->len = tmp_value.length();
    // memcpy(value->data,(char *)tmp_value.data(),value->len);

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

