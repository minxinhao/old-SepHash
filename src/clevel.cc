#include "clevel.h"
namespace CLEVEL
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
    dir->is_resizing = 0;

    dir->last_level = (uintptr_t)alloc.alloc(sizeof(LevelTable)+sizeof(Bucket)*INIT_TABLE_SIZE);
    dir->first_level = (uintptr_t)alloc.alloc(sizeof(LevelTable)+sizeof(Bucket)*INIT_TABLE_SIZE*2);

    LevelTable* cur_table = (LevelTable*)dir->last_level;
    cur_table->up = dir->first_level;
    cur_table->capacity = INIT_TABLE_SIZE;
    memset(cur_table->buckets,0,sizeof(Bucket)*cur_table->capacity);

    cur_table = (LevelTable*)dir->first_level;
    cur_table->up = 0;
    cur_table->capacity = INIT_TABLE_SIZE*2;
    memset(cur_table->buckets,0,sizeof(Bucket)*cur_table->capacity);
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
    dir->is_resizing = 0;
    
    dir->last_level = (uintptr_t)server_alloc.alloc(sizeof(LevelTable)+sizeof(Bucket)*INIT_TABLE_SIZE);
    dir->first_level = (uintptr_t)server_alloc.alloc(sizeof(LevelTable)+sizeof(Bucket)*INIT_TABLE_SIZE*2);

    LevelTable* cur_table = (LevelTable*)alloc.alloc(sizeof(LevelTable)+sizeof(Bucket)*zero_size);
    memset(cur_table->buckets,0,sizeof(Bucket)*zero_size);
    cur_table->up = dir->first_level;
    cur_table->capacity = INIT_TABLE_SIZE;
    uint64_t upper = INIT_TABLE_SIZE / zero_size ;
    for(uint64_t i = 0 ; i < upper ; i++){
        co_await conn->write(dir->last_level+sizeof(LevelTable)+i*zero_size*sizeof(Bucket),seg_rmr.rkey,cur_table->buckets,sizeof(Bucket)*zero_size,lmr->lkey);
    }
    
    cur_table->up = 0;
    cur_table->capacity = INIT_TABLE_SIZE*2;
    upper = cur_table->capacity / zero_size ;
    for(uint64_t i = 0 ; i < upper ; i++){
        co_await conn->write(dir->first_level+sizeof(LevelTable)+i*zero_size*sizeof(Bucket),seg_rmr.rkey,cur_table->buckets,sizeof(Bucket)*zero_size,lmr->lkey);
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
    auto pattern = hash(key->data, key->len);
    uint64_t pattern_1 = (uint64_t)pattern;
    uint64_t pattern_2 = (uint64_t)(pattern >> 64);
    uint64_t tmp_fp = fp(pattern_1);
    KVBlock *kv_block = InitKVBlock(key, value, &alloc);
    uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 3;
    uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
    // writekv
    wo_wait_conn->pure_write(kvblock_ptr, seg_rmr.rkey, kv_block, kvblock_len, lmr->lkey);
    retry_cnt = 0;
    this->key_num = *(uint64_t *)key->data;
    KVBlock *tmp_block = (KVBlock *)alloc.alloc(8 * ALIGNED_SIZE,true);
Retry:
    retry_cnt++;
    if(retry_cnt >= 10) exit(-1);
    alloc.ReSet(sizeof(Directory));
    // 1. Read Header
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, dir, sizeof(Directory), lmr->lkey);    

    // 2. Find Duplicate or Free Slot
    // https://www.icyf.me/2020/09/04/paper-atc20-clevel/
    // 对于插入：由最底层开始逐层向上寻找空位插入； 对于更新：由最底层开始逐层向上查找，如果出现多次命中，只保留位于最高层的那一个。
    // 根据CLevel的源码，第一次需要从下往上查找所有匹配key并进行删除，然后插入到尽可能高位的Empty Entry
    // 对于可能出现的duplicate key，都允许存在;search时，认为最高Level的Insert为有效数据
    LevelTable* cur_table = (LevelTable*) alloc.alloc(sizeof(LevelTable));
    uintptr_t cur_table_ptr = dir->last_level;
    Bucket * buc1 = (Bucket*)alloc.alloc(sizeof(Bucket));
    Bucket * buc2 = (Bucket*)alloc.alloc(sizeof(Bucket));
    Bucket * buc3 = (Bucket*)alloc.alloc(sizeof(Bucket));
    uint64_t level_id = 0;
    uintptr_t free_slot_ptr = -1;
    uint64_t first_level_ptr = -1;

    while(cur_table_ptr != 0){
        // 2.1 Read LevelTable Header : capacity,up
        co_await conn->read(cur_table_ptr,seg_rmr.rkey,cur_table,sizeof(LevelTable),lmr->lkey);
        uint64_t buc_idx1,buc_idx2;
        buc_idx1 = pattern_1 % cur_table->capacity;
        buc_idx2 = pattern_2 % cur_table->capacity;
        
        // 2.2 Read 2 Bucket
        uintptr_t buc_ptr1 = cur_table_ptr + sizeof(LevelTable) + buc_idx1 * sizeof(Bucket);
        uintptr_t buc_ptr2 = cur_table_ptr + sizeof(LevelTable) + buc_idx2 * sizeof(Bucket);
        auto read_buc1 = conn->read(buc_ptr1,seg_rmr.rkey,buc1,sizeof(Bucket),lmr->lkey);
        auto read_buc2 = wo_wait_conn->read(buc_ptr2,seg_rmr.rkey,buc2,sizeof(Bucket),lmr->lkey);
        
        // 2.3 Find Duplicate Key
        Bucket* buc;
        uintptr_t buc_ptr;
        for(uint64_t i = 0 ; i < 2 ; i++){
            if(i==0){
                co_await std::move(read_buc1);
            }else{
                co_await std::move(read_buc2);
            }

            buc = (i==0)? buc1:buc2;
            buc_ptr = (i==0)? buc_ptr1:buc_ptr2;
            for(uint64_t entry_id = 0 ; entry_id < BUCKET_SIZE ; entry_id++){
                uint64_t buc_id = (i==0)? buc_idx1:buc_idx2;
                
                if(buc->entrys[entry_id] == 0){
                    free_slot_ptr = buc_ptr + sizeof(Entry) * entry_id;
                    // if(level_id == 1) break;
                    continue;
                }
                // if(buc->entrys[entry_id].fp == tmp_fp){
                //     co_await conn->read(ralloc.ptr(buc->entrys[entry_id].offset), seg_rmr.rkey, tmp_block,(buc->entrys[entry_id].len) * ALIGNED_SIZE, lmr->lkey);
                //     if (memcmp(key->data, tmp_block->data, key->len) == 0)
                //     {
                //         // duplicate key : delete
                //         log_err("[%lu:%lu:%lu]duplicate",this->cli_id,this->coro_id,this->key_num);
                //         uintptr_t slot_ptr = buc_ptr + sizeof(Entry) * entry_id;
                //         co_await conn->cas_n(slot_ptr, seg_rmr.rkey,buc->entrys[entry_id], 0);
                //     }
                // }
            }
        }
        // if(level_id != 0 && free_slot_ptr != -1) break;
        first_level_ptr = cur_table_ptr;
        cur_table_ptr = cur_table->up;
        level_id++;
    }

    // 3. Write KV 
    // log_err("[%lu:%lu:%lu]",this->cli_id,this->coro_id,this->key_num);
    if(free_slot_ptr != -1){
        // 3.1 check rehash
        if(dir->is_resizing == 1 && level_id == 0){
            goto Retry;
        }
        // 3.2 cas entry
        Entry *tmp = (Entry *)alloc.alloc(sizeof(Entry));
        tmp->fp = tmp_fp;
        tmp->len = (kvblock_len + ALIGNED_SIZE - 1) / ALIGNED_SIZE;
        tmp->offset = ralloc.offset(kvblock_ptr);
        if(!co_await conn->cas_n(free_slot_ptr, seg_rmr.rkey,0,*tmp)){
            log_err("[%lu:%lu:%lu] cas entry fail at slot_ptr:%lx",this->cli_id,this->coro_id,this->key_num,free_slot_ptr);
            goto Retry;
        }
        // 3.3 recheck global context
        co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, dir, sizeof(Directory), lmr->lkey);
        if(dir->is_resizing == 1 && level_id == 0){
            goto Retry;
        }
        co_return;
    }

    // 4. Recheck Global Context
    Directory* tmp_dir = (Directory*)alloc.alloc(sizeof(Directory));
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, tmp_dir, sizeof(Directory), lmr->lkey);
    tmp_dir->print();
    if(dir->first_level != tmp_dir->first_level){
        goto Retry;
    }

    // 5. Resize
    log_err("[%lu:%lu:%lu]expand",this->cli_id,this->coro_id,this->key_num);
    // 5.1 alloc new level
    uintptr_t new_level_ptr = dir->first_level + sizeof(LevelTable) + cur_table->capacity * sizeof(Bucket);
    cur_table->capacity = cur_table->capacity * 2;
    cur_table->up = 0 ;
    co_await conn->write(new_level_ptr,seg_rmr.rkey,cur_table,sizeof(LevelTable),lmr->lkey);
    
    // 5.2 clear new level table
    LevelTable* zero_table = (LevelTable*)alloc.alloc(sizeof(LevelTable)+sizeof(Bucket)*zero_size);
    memset(zero_table->buckets,0,sizeof(Bucket)*zero_size);
    uint64_t upper = cur_table->capacity / zero_size ;
    for(uint64_t i = 0 ; i < upper ; i++){
        log_err("[%lu:%lu:%lu]i:%lu upper:%lu cur_table->capacity:%lu zero_size:%lu",this->cli_id,this->coro_id,this->key_num,i,upper,cur_table->capacity,zero_size);
        co_await conn->write(new_level_ptr+sizeof(LevelTable)+i*zero_size*sizeof(Bucket),seg_rmr.rkey,cur_table->buckets,sizeof(Bucket)*zero_size,lmr->lkey);
    }

    log_err("[%lu:%lu:%lu]",this->cli_id,this->coro_id,this->key_num);
    // 5.2 list new level to first level
    cur_table->up = new_level_ptr;
    co_await conn->write(first_level_ptr,seg_rmr.rkey,&cur_table->up,sizeof(uint64_t),lmr->lkey);
    
    // 5.3 Expand Global Context
    // log_err("[%lu:%lu:%lu]dir->first_level:%lx new_level_ptr:%lx cur_table_ptr:%lx cas_ptr:%lx",this->cli_id,this->coro_id,this->key_num,dir->first_level,new_level_ptr,cur_table_ptr,seg_rmr.raddr + sizeof(uint64_t));
    if(!co_await conn->cas_n(seg_rmr.raddr + sizeof(uint64_t), seg_rmr.rkey,dir->first_level,new_level_ptr)){
        goto Retry;
    }
    dir->is_resizing = 1;
    co_await conn->write(seg_rmr.raddr,seg_rmr.rkey,dir,sizeof(uint64_t),lmr->lkey);
    // log_err("[%lu:%lu:%lu]",this->cli_id,this->coro_id,this->key_num);

    // 6. ReHash
    // Complete By Rehash Thread

    // 7. ReInsert
    goto Retry;
}

task<> Client::rehash(){
    Bucket* buc = (Bucket*)alloc.alloc(zero_size*sizeof(Bucket));
    LevelTable * last_table = (LevelTable*)alloc.alloc(sizeof(LevelTable));
    LevelTable * first_table = (LevelTable*)alloc.alloc(sizeof(LevelTable));
    Bucket* buc1 = (Bucket*)alloc.alloc(sizeof(Bucket));
    Bucket* buc2 = (Bucket*)alloc.alloc(sizeof(Bucket));
    KVBlock *kv_block = (KVBlock *)alloc.alloc(7 * ALIGNED_SIZE);
    while(true){
        co_await conn->read(seg_rmr.raddr,seg_rmr.rkey,dir,sizeof(uint64_t),lmr->lkey);
        if(dir->is_resizing){
            // 1. read last level header
            co_await conn->read(dir->last_level,seg_rmr.rkey,last_table,sizeof(LevelTable),lmr->lkey);
                        
            // 2. read last level buckets
            uint64_t upper = last_table->capacity / zero_size ;
            uint64_t buc_id = 0; //记录update的位置
            uintptr_t buc_ptr = dir->last_level + sizeof(LevelTable);
            for(uint64_t i = 0 ; i < upper ; i++){
                co_await conn->read(buc_ptr,seg_rmr.rkey,buc,sizeof(Bucket)*zero_size,lmr->lkey);
                for(uint64_t buc_id = 0 ; buc_id < zero_size ; buc_id++){
                    for(uint64_t entry_id = 0 ; entry_id < BUCKET_SIZE ; entry_id++){
                        // 2.1 Read KvBlock
                        co_await conn->read(ralloc.ptr(buc[buc_id].entrys[entry_id].offset), seg_rmr.rkey, kv_block,(buc[buc_id].entrys[entry_id].len) * ALIGNED_SIZE, lmr->lkey);
                        auto pattern = hash(kv_block->data, kv_block->k_len);
                        uint64_t pattern_1 = (uint64_t)pattern;
                        uint64_t pattern_2 = (uint64_t)(pattern >> 64);
Retry:
                        // 2.2 Read First level header
                        co_await conn->read(seg_rmr.raddr+sizeof(uint64_t),seg_rmr.rkey,&dir->first_level,sizeof(uint64_t),lmr->lkey);
                        co_await conn->read(dir->first_level,seg_rmr.rkey,first_table,sizeof(LevelTable),lmr->lkey);

                        // 2.3 read 2 bucket in first level && insert
                        uint64_t buc_idx1 = pattern_1 % first_table->capacity;
                        uint64_t buc_idx2 = pattern_2 % first_table->capacity;
                        uintptr_t buc_ptr1 = dir->first_level + sizeof(LevelTable) + buc_idx1 * sizeof(Bucket);
                        uintptr_t buc_ptr2 = dir->first_level + sizeof(LevelTable) + buc_idx2 * sizeof(Bucket);
                        auto read_buc1 = conn->read(buc_ptr1,seg_rmr.rkey,buc1,sizeof(Bucket),lmr->lkey);
                        auto read_buc2 = wo_wait_conn->read(buc_ptr2,seg_rmr.rkey,buc2,sizeof(Bucket),lmr->lkey);
                        uintptr_t free_slot_ptr = -1;
                        co_await std::move(read_buc1);
                        co_await std::move(read_buc2);
                        for(uint64_t i = 0 ; i < 2 ; i++){
                            buc = (i==0)? buc1:buc2;
                            buc_ptr = (i==0)? buc_ptr1:buc_ptr2;
                            for(uint64_t entry_id = 0 ; entry_id < BUCKET_SIZE ; entry_id++){
                                if(buc->entrys[entry_id].offset == 0){
                                    free_slot_ptr = buc_ptr1 + sizeof(Entry) * entry_id;
                                    break;
                                }
                            }
                        }

                        if(free_slot_ptr != -1){
                            if(!co_await conn->cas_n(free_slot_ptr, seg_rmr.rkey,0,buc[buc_id].entrys[entry_id])){
                                goto Retry;
                            }else{
                                continue;
                            }
                        }
                        
                        // 2.4 no free slot in first level
                        // a. expand 
                        uintptr_t new_level_ptr = dir->first_level + sizeof(LevelTable) + first_table->capacity * sizeof(Bucket);
                        co_await conn->cas_n(seg_rmr.raddr + sizeof(uint64_t), seg_rmr.rkey,dir->first_level,new_level_ptr);
                        // b. retry insert
                        goto Retry;

                    }
                }
                buc_ptr += zero_size * sizeof(Bucket);
            }

            // 3. write is_resizing to false
            dir->is_resizing = 0;
            co_await conn->write(seg_rmr.raddr,seg_rmr.rkey,dir,sizeof(uint64_t),lmr->lkey);
        }
    }
}

task<bool> Client::search(Slice *key, Slice *value)
{
    auto pattern = hash(key->data, key->len);
    uint64_t pattern_1 = (uint64_t)pattern;
    uint64_t pattern_2 = (uint64_t)(pattern >> 64);
    uint64_t tmp_fp = fp(pattern_1);
    this->key_num = *(uint64_t *)key->data;
Retry:
    alloc.ReSet(sizeof(Directory));
    KVBlock *res = nullptr;
    KVBlock *kv_block = (KVBlock *)alloc.alloc(7 * ALIGNED_SIZE);

    // 1. Read Header
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, dir, sizeof(Directory), lmr->lkey);

    // 2. Find Key
    // 由最底层开始逐层向上查找；为了解决假阴性问题，如果出现阴性结果，那么要检查全局 context 是否发生变化，如果变化了就再查一次。
    LevelTable* cur_table = (LevelTable*) alloc.alloc(sizeof(LevelTable));
    uintptr_t cur_table_ptr = dir->last_level;
    Bucket * buc1 = (Bucket*)alloc.alloc(sizeof(Bucket));
    Bucket * buc2 = (Bucket*)alloc.alloc(sizeof(Bucket));
    while(cur_table_ptr != 0){
        // 2.1 Read LevelTable Header : capacity,up
        co_await conn->read(cur_table_ptr,seg_rmr.rkey,cur_table,sizeof(LevelTable),lmr->lkey);
        uint64_t buc_idx1,buc_idx2;
        buc_idx1 = pattern_1 % cur_table->capacity;
        buc_idx2 = pattern_2 % cur_table->capacity;
        
        // 2.2 Read 2 Bucket
        uintptr_t buc_ptr1 = cur_table_ptr + sizeof(LevelTable) + buc_idx1 * sizeof(Bucket);
        uintptr_t buc_ptr2 = cur_table_ptr + sizeof(LevelTable) + buc_idx2 * sizeof(Bucket);
        auto read_buc1 = conn->read(buc_ptr1,seg_rmr.rkey,buc1,sizeof(Bucket),lmr->lkey);
        auto read_buc2 = wo_wait_conn->read(buc_ptr2,seg_rmr.rkey,buc2,sizeof(Bucket),lmr->lkey);
        
        // 2.3 Find Duplicate Key
        Bucket* buc;
        uintptr_t buc_ptr;
        for(uint64_t i = 0 ; i < 2 ; i++){
            if(i==0){
                co_await std::move(read_buc1);
            }else{
                co_await std::move(read_buc2);
            }

            buc = (i==0)? buc1:buc2;
            buc_ptr = (i==0)? buc_ptr1:buc_ptr2;
            for(uint64_t entry_id = 0 ; entry_id < BUCKET_SIZE ; entry_id++){
                if(buc->entrys[entry_id].fp == tmp_fp){
                    co_await conn->read(ralloc.ptr(buc->entrys[entry_id].offset), seg_rmr.rkey, kv_block,(buc->entrys[entry_id].len) * ALIGNED_SIZE, lmr->lkey);
                    if (memcmp(key->data, kv_block->data, key->len) == 0)
                    {
                        res = kv_block;
                    }
                }
            }
        }
        cur_table_ptr = cur_table->up;
    }

    // 3. recheck global context
    Directory * tmp_dir = (Directory*)alloc.alloc(sizeof(Directory));
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, tmp_dir, sizeof(Directory), lmr->lkey);
    if(tmp_dir->first_level != dir->first_level ){
        goto Retry;
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
