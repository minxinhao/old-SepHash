#include "cluster_hash.h"
namespace CLUSTER
{

inline __attribute__((always_inline)) uint64_t fp(uint64_t pattern)
{
    return ((uint64_t)((pattern) >> 32) & ((1 << 8) - 1));
}

inline __attribute__((always_inline)) uint64_t get_seg_loc(uint64_t pattern, uint64_t global_depth)
{
    return ((pattern) & ((1 << global_depth) - 1));
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
    // Init CurTable
    Bucket *buc;
    dir->tables[0].logical_num = INIT_TABLE_SIZE;
    uint64_t indirect_num = INIT_TABLE_SIZE / BUCKET_SIZE; // Drtm这样的算法就是预留的indirect num数目为当插入的key翻倍后进行resize
    // dir->tables[0].indirect_num = INIT_TABLE_SIZE / BUCKET_SIZE; // Drtm这样的算法就是预留的indirect num数目为当插入的key翻倍后进行resize
    dir->tables[0].free_indirect_num = 1; // Drtm omit the first indirect node, which i didn't understand
    uint64_t buc_num = dir->tables[0].logical_num + indirect_num;
    // uint64_t buc_num = dir->tables[0].logical_num + dir->tables[0].indirect_num;
    dir->tables[0].buc_start = (uintptr_t)alloc.alloc(buc_num * sizeof(Bucket));
    memset((Bucket *)dir->tables[0].buc_start, 0, sizeof(Bucket) * buc_num);
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
    // uint64_t rbuf_size = (seg_rmr.rlen - (1ul << 20) * 20) /
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

    // 重置远端segment
    dir->tables[0].logical_num = INIT_TABLE_SIZE;
    dir->tables[0].free_indirect_num = 1; // Drtm omit the first indirect node, which i didn't understand
    uint64_t buc_num = INIT_TABLE_SIZE + INIT_TABLE_SIZE/BUCKET_SIZE;
    dir->tables[0].buc_start = (uintptr_t)server_alloc.alloc(bucket_batch_size * sizeof(Bucket));
    Bucket *local_buc = (Bucket *)alloc.alloc(bucket_batch_size * sizeof(Bucket));
    memset(local_buc, 0, sizeof(Bucket) * bucket_batch_size);
    uint64_t upper = buc_num / bucket_batch_size; // 保证buc_num能够整除bucket_batch_size
    uint64_t batch_size = bucket_batch_size * sizeof(Bucket);
    for(uint64_t i = 0 ; i < upper ; i++){
        co_await conn->write(dir->tables[0].buc_start + i * batch_size, seg_rmr.rkey, local_buc, batch_size, lmr->lkey);
    }

    // 重置远端 Directory
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
    if (op_cnt % 2)
        alloc.ReSet(sizeof(Directory) + op_size);
    else
        alloc.ReSet(sizeof(Directory));
    uint64_t pattern = (uint64_t)hash(key->data, key->len);
    KVBlock *kv_block = InitKVBlock(key, value, &alloc);
    uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 3;
    uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
    // writekv
    wo_wait_conn->pure_write(kvblock_ptr, seg_rmr.rkey, kv_block, kvblock_len, lmr->lkey);
    retry_cnt = 0;
    this->key_num = *(uint64_t *)key->data;

Retry:
    // log_err("[%lu:%lu:%lu]",this->cli_id,this->coro_id,this->key_num);
    // 1. Read TableHeader && Bucket
    // 1.1 Read Directory (less than 64 bytes)
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, dir, sizeof(Directory), lmr->lkey);
    // dir->print();
    // TODO : Split可以实现单条slot CAS和批量Move两种方式
    if (dir->dir_lock)
    {
        // Resizing
        goto Retry;
    }
    // 1.2 Cal BucIdx
    TableHeader *cur_table = &dir->tables[dir->offset];
    uint64_t buc_idx = pattern % cur_table->logical_num;
    uintptr_t buc_ptr = cur_table->buc_start + sizeof(Bucket) * buc_idx;
    uintptr_t fisrt_buc_ptr = buc_ptr;
    // 1.3 read bucket
    Bucket *buc = (Bucket *)alloc.alloc(sizeof(Bucket));
    co_await conn->read(buc_ptr, seg_rmr.rkey, buc, sizeof(Bucket), lmr->lkey);
    uintptr_t next_buc = buc->next;

    // 2. Find Free Entry
    // 2.1 search in bucket list
    uint64_t entry_id = -1;
    while (1)
    {
        for (uint64_t i = 0; i < BUCKET_SIZE; i++)
        {
            if (buc->entrys[i].offset == 0)
            {
                entry_id = i;
                break;
            }
        }
        if (entry_id != -1 || next_buc == 0)
        {
            break;
        }
        buc_ptr = next_buc;
        co_await conn->read(buc_ptr, seg_rmr.rkey, buc, sizeof(Bucket), lmr->lkey);
        next_buc = buc->next;
    }

    // 2.2 lock bucket
    if (!co_await conn->cas_n(fisrt_buc_ptr, seg_rmr.rkey, 0, 1))
    {
        // log_err("[%lu:%lu:%lu] fail to lock bucket list:%lu",this->cli_id,this->coro_id,this->key_num,buc_idx);
        goto Retry;
    }

    // 2.3 write entry
    Entry *tmp = (Entry *)alloc.alloc(sizeof(Entry));
    tmp->fp = fp(pattern);
    tmp->len = (kvblock_len + ALIGNED_SIZE - 1) / ALIGNED_SIZE;
    tmp->offset = ralloc.offset(kvblock_ptr);
    if (entry_id != -1)
    {
        uintptr_t slot_ptr = buc_ptr + sizeof(uint64_t) * 2 + sizeof(Entry) * entry_id;
        co_await conn->write(slot_ptr, seg_rmr.rkey, tmp, sizeof(Entry), lmr->lkey);
    }

    // 2.4 unlock bucket list
    buc->lock = 0;
    co_await conn->write(fisrt_buc_ptr, seg_rmr.rkey, &buc->lock, sizeof(uint64_t), lmr->lkey);
    if(entry_id != -1){
        co_return;
    }

    // 4. Add overflow bucket Or Resize
    // 4.1 lock dir
    if (!co_await conn->cas_n(seg_rmr.raddr, seg_rmr.rkey, 0, 1))
    {
        // log_err("[%lu:%lu:%lu] fail to lock dir",this->cli_id,this->coro_id,this->key_num);
        goto Retry;
    }

    // 4.1 Add Overflow
    if(cur_table->free_indirect_num < cur_table->logical_num/BUCKET_SIZE){
        // log_err("[%lu:%lu:%lu]Add Overflow with free_indirect_num:%lu",this->cli_id,this->coro_id,this->key_num,cur_table->free_indirect_num);
        uint64_t new_buc_idx = cur_table->free_indirect_num + cur_table->logical_num;
        uintptr_t new_buc_ptr = cur_table->buc_start + new_buc_idx * sizeof(Bucket);
        // a. Edit Bucket list
        buc->next = new_buc_ptr;
        co_await conn->write(buc_ptr + sizeof(uint64_t),seg_rmr.rkey,&(buc->next),sizeof(uint64_t),lmr->lkey);
        // b. write entry
        memset(buc,0,sizeof(Bucket));
        buc->entrys[0]=*tmp;
        co_await conn->write(new_buc_ptr, seg_rmr.rkey, buc, sizeof(Bucket), lmr->lkey);
        // c. write free_indirect_num
        uintptr_t free_indirect_ptr = seg_rmr.raddr + sizeof(uint64_t) + sizeof(uint64_t) + dir->offset * sizeof(TableHeader) + sizeof(uint64_t);
        dir->tables[dir->offset].free_indirect_num++;
        co_await conn->write(free_indirect_ptr, seg_rmr.rkey, &(dir->tables[dir->offset].free_indirect_num),sizeof(uint64_t),lmr->lkey);
        // log_err("[%lu:%lu:%lu]Add buc_idx:%lu buc_ptr:%lx with new overflow:%lx",this->cli_id,this->coro_id,this->key_num,buc_idx,buc_ptr,new_buc_ptr);

    }else{
        // 4.2 Resize
        log_err("[%lu:%lu:%lu]resize",this->cli_id,this->coro_id,this->key_num);
        // a. alloc new table
        // log_err("Alloc new table");
        dir->dir_lock = 1;
        uint64_t next_table = (dir->offset + 1) % table_num;
        dir->tables[next_table].logical_num = dir->tables[dir->offset].logical_num * 2;
        dir->tables[next_table].free_indirect_num = 1;
        uint64_t buc_num = dir->tables[next_table].logical_num + dir->tables[next_table].logical_num/BUCKET_SIZE;
        // 一次分配这么大的空间 单个cli的ralloc肯定是不够的
        // 因为Cluseter Hash的空间十分对齐，直接从前往后分配好了
        // dir->tables[next_table].buc_start = ralloc.alloc(sizeof(Bucket)*buc_num);
        uint64_t old_buc_num = dir->tables[dir->offset].logical_num + dir->tables[dir->offset].logical_num /BUCKET_SIZE; 
        dir->tables[next_table].buc_start = dir->tables[dir->offset].buc_start;
        dir->tables[next_table].buc_start += old_buc_num * sizeof(Bucket);
        // 由于空间限制，只能一部分一部分的清零
        uint64_t batch_size = bucket_batch_size*sizeof(Bucket);
        Bucket* local_buc = (Bucket*)alloc.alloc(batch_size);
        memset(local_buc,0,batch_size);
        uint64_t upper = buc_num / bucket_batch_size; // 保证buc_num能够整除bucket_batch_size
        for(uint64_t i = 0 ; i < upper ; i++){
            co_await conn->write(dir->tables[next_table].buc_start + i * batch_size, seg_rmr.rkey, local_buc, batch_size, lmr->lkey);
        }

        // a. insert given key
        co_await move_entry(kv_block,tmp);
        
        // b. Move Data
        // log_err("Move Data");
        upper = old_buc_num / bucket_batch_size;
        for(uint64_t i = 0 ; i < upper ; i++){
            co_await conn->read(dir->tables[dir->offset].buc_start + i * batch_size , seg_rmr.rkey,local_buc,batch_size,lmr->lkey);
            for(uint64_t buc_idx = 0 ; buc_idx < bucket_batch_size ; buc_idx++){
                for(uint64_t entry_id = 0 ; entry_id < BUCKET_SIZE ; entry_id++){
                    // log_err("Move i:%lu buc_idx:%lu entry_id:%lu",i,buc_idx,entry_id);
                    // local_buc[buc_idx].entrys[entry_id].print();
                    co_await conn->read(ralloc.ptr(local_buc[buc_idx].entrys[entry_id].offset), seg_rmr.rkey, kv_block,(local_buc[buc_idx].entrys[entry_id].len)*ALIGNED_SIZE, lmr->lkey);
                    co_await move_entry(kv_block,&local_buc[buc_idx].entrys[entry_id]);
                }
            }
        }
        alloc.free(batch_size);

        // c. Edit Global offset
        dir->offset = next_table;
        co_await conn->write(seg_rmr.raddr+sizeof(uint64_t), seg_rmr.rkey, &dir->offset, sizeof(uint64_t)+table_num*sizeof(TableHeader), lmr->lkey);
    }
    
    // 4.4 Unlock Dir  
    dir->dir_lock = 0;  
    co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, &dir->dir_lock, sizeof(uint64_t), lmr->lkey);

}

task<> Client::move_entry(KVBlock* kv_block,Entry* entry){
    uint64_t old_offset = alloc.offset;

    uint64_t next_table = (dir->offset + 1) % table_num;
    TableHeader *new_table = &dir->tables[next_table];

    // 1. Cal BucIdx
    auto pattern = hash(kv_block->data, kv_block->k_len);
    uint64_t buc_idx = pattern % dir->tables[next_table].logical_num;
    uintptr_t buc_ptr = new_table->buc_start + sizeof(Bucket) * buc_idx;

    // 2. Read Bucket
    Bucket *buc = (Bucket *)alloc.alloc(sizeof(Bucket));
    co_await conn->read(buc_ptr, seg_rmr.rkey, buc, sizeof(Bucket), lmr->lkey);
    uintptr_t next_buc = buc->next;

    // 3. Find Free Entry
    uint64_t entry_id = -1;
    while (1)
    {
        for (uint64_t i = 0; i < BUCKET_SIZE; i++)
        {
            if (buc->entrys[i].offset == 0)
            {
                entry_id = i;
                break;
            }
        }
        if (entry_id != -1 || next_buc == 0)
        {
            break;
        }
        buc_ptr = next_buc;
        co_await conn->read(buc_ptr, seg_rmr.rkey, buc, sizeof(Bucket), lmr->lkey);
        next_buc = buc->next;
    }

    // 4 write entry
    if (entry_id != -1)
    {
        // log_err("Move %lu to new buc:%lu",*(uint64_t*)kv_block->data,buc_idx);
        uintptr_t slot_ptr = buc_ptr + sizeof(uint64_t) * 2 + sizeof(Entry) * entry_id;
        co_await conn->write(slot_ptr, seg_rmr.rkey, entry, sizeof(Entry), lmr->lkey);
        alloc.offset = old_offset;
        co_return;
    }

    // 5. Add overflow bucket
    // 直接读取本地的Table Header即可
    uint64_t new_buc_idx = new_table->free_indirect_num + new_table->logical_num;
    uintptr_t new_buc_ptr = new_table->buc_start + new_buc_idx * sizeof(Bucket);
    new_table->free_indirect_num++;
    // log_err("Add overflow for bucidx:%lx with free_indirect_num:%lu logical_num:%lu",buc_idx,new_table->free_indirect_num,new_table->logical_num);
    // a. Edit Bucket list
    buc->next = new_buc_ptr;
    co_await conn->write(buc_ptr + sizeof(uint64_t),seg_rmr.rkey,&(buc->next),sizeof(uint64_t),lmr->lkey);
    // b. write entry
    uintptr_t slot_ptr = new_buc_ptr + sizeof(uint64_t) * 2;
    co_await conn->write(slot_ptr, seg_rmr.rkey, entry, sizeof(Entry), lmr->lkey);
    if(new_table->free_indirect_num >= new_table->logical_num/BUCKET_SIZE){
        log_err("Resize during resize isn't allowed");
        exit(-1);
    }

    alloc.offset = old_offset;
}


task<bool> Client::search(Slice *key, Slice *value)
{
    uint64_t pattern = (uint64_t)hash(key->data, key->len);
    uint64_t pattern_fp1 = fp(pattern);
    this->key_num = *(uint64_t *)key->data;
Retry:
    alloc.ReSet(sizeof(Directory));

    // 1. Read TableHeader && Bucket
    // 1.1 Read Directory (less than 64 bytes)
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, dir, sizeof(Directory), lmr->lkey);
    // TODO : Split可以实现单条slot CAS和批量Move两种方式
    if (dir->dir_lock)
    {
        // Resizing
        goto Retry;
    }
    // 1.2 Cal BucIdx
    TableHeader *cur_table = &dir->tables[dir->offset];
    uint64_t buc_idx = pattern % cur_table->logical_num;
    uintptr_t buc_ptr = cur_table->buc_start + sizeof(Bucket) * buc_idx;
    uintptr_t fisrt_buc_ptr = buc_ptr;
    // 1.3 read bucket
    Bucket *buc = (Bucket *)alloc.alloc(sizeof(Bucket));
    co_await conn->read(buc_ptr, seg_rmr.rkey, buc, sizeof(Bucket), lmr->lkey);
    uintptr_t next_buc = buc->next;
    // log_err("[%lu:%lu:%lu]buc_idx:%lu buc_ptr:%lx next_buc:%lx",this->cli_id,this->coro_id,this->key_num,buc_idx,buc_ptr,next_buc);
    // 2. Find Free Entry
    // 2.1 search in bucket list
    uint64_t entry_id = -1;
    KVBlock *res = nullptr;
    KVBlock *kv_block = (KVBlock *)alloc.alloc(7 * ALIGNED_SIZE);
    while (1)
    {
        for (uint64_t i = 0; i < BUCKET_SIZE; i++)
        {
            if (buc->entrys[i].offset != 0 && buc->entrys[i].fp == pattern_fp1)
            {
                co_await conn->read(ralloc.ptr(buc->entrys[i].offset), seg_rmr.rkey, kv_block,(buc->entrys[i].len) * ALIGNED_SIZE, lmr->lkey);
                if (memcmp(key->data, kv_block->data, key->len) == 0)
                {
                    // log_err("[%lu:%lu:%lu]find key:%lu with value:%s",this->cli_id,this->coro_id,this->key_num,*(uint64_t*)kv_block->data,kv_block->data+sizeof(uint64_t));
                    res = kv_block;
                    break;
                }
                
            }
        }
        if (res != nullptr || next_buc == 0)
        {
            break;
        }
        buc_ptr = next_buc;
        co_await conn->read(buc_ptr, seg_rmr.rkey, buc, sizeof(Bucket), lmr->lkey);
        next_buc = buc->next;
    }

    if (res != nullptr && res->v_len != 0)
    {
        value->len = res->v_len;
        memcpy(value->data, res->data + res->k_len, value->len);
        co_return true;
    }

    log_err("[%lu:%lu:%lu]No mathc key",this->cli_id,this->coro_id,this->key_num);
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
