#include <stdint.h>
#include <set>
#include "race.h"
#include "kv_trait.h"
#include "generator.h"
// #define ORDERED_INSERT
Config config;
uint64_t load_num = 10000000;
using ClientType = RACE::RACEClient;
using ServerType = RACE::RACEServer;

inline uint64_t GenKey(uint64_t key){
#ifdef ORDERED_INSERT
    return key;
#else
    return FNVHash64(key);
#endif
}

template <class Client>
requires KVTrait<Client, Slice *, Slice *>
    task<> load(Client *cli, uint64_t cli_id, uint64_t coro_id)
{
    co_await cli->start(config.num_machine * config.num_cli * config.num_coro);
    uint64_t tmp_key;
    Slice key, value;
    std::string tmp_value = std::string(32, '1');
    value.len = tmp_value.length();
    value.data = (char *)tmp_value.data();
    key.len = sizeof(uint64_t);
    key.data = (char *)&tmp_key;
    uint64_t num_op = load_num / (config.num_machine * config.num_cli * config.num_coro);
    for (uint64_t i = 0; i < num_op; i++)
    {
        tmp_key = GenKey((config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id) * num_op + i);
        co_await cli->insert(&key, &value);
    }
    co_await cli->stop();
    co_return;
}

template <class Client>
requires KVTrait<Client, Slice *, Slice *>
    task<> run(Generator *gen, Client *cli, uint64_t cli_id, uint64_t coro_id)
{
    co_await cli->start(config.num_machine * config.num_cli * config.num_coro);
    uint64_t tmp_key;
    Slice key, value, ret_value;
    std::string tmp_value = std::string(32, '1');
    value.len = tmp_value.length();
    value.data = (char *)tmp_value.data();
    key.len = sizeof(uint64_t);
    key.data = (char *)&tmp_key;

    double op_frac;
    double read_frac = config.insert_frac + config.read_frac;
    xoshiro256pp op_chooser;
    xoshiro256pp key_chooser;
    uint64_t num_op = config.num_op / (config.num_machine * config.num_cli * config.num_coro);
    for (uint64_t i = 0; i < num_op; i++)
    {
        tmp_key = GenKey(load_num + (config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id) * num_op + gen->operator()(key_chooser()));
        op_frac = op_chooser();
        if (op_frac < config.insert_frac)
        {
            co_await cli->insert(&key, &value);
        }
        else if (op_frac < read_frac)
        {
            // co_await cli->search(&key, &ret_value);
        }
        else
        {
            // update
        }
    }
    co_await cli->stop();
    co_return;
}

int main(int argc, char *argv[])
{
    config.ParseArg(argc, argv);
    if (config.is_server)
    {
        ServerType ser(config);
        getchar();
    }
    else
    {
        uint64_t cbuf_size = (1ul << 20) * 250;
        char *mem_buf = (char *)malloc(cbuf_size * config.num_cli * config.num_coro);
        rdma_dev dev(nullptr, 1, config.roce_flag);
        std::vector<ibv_mr *> lmrs(config.num_cli * config.num_coro, nullptr);
        std::vector<rdma_client *> rdma_clis(config.num_cli, nullptr);
        std::vector<rdma_conn *> rdma_conns(config.num_cli, nullptr);
        std::vector<ClientType *> clis;
        std::thread ths[80];

        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            rdma_clis[i] = new rdma_client(dev, so_qp_cap, rdma_default_tempmp_size, config.max_coro, config.cq_size);
            rdma_conns[i] = rdma_clis[i]->connect(config.server_ip);
            assert(rdma_conns[i] != nullptr);
            for (uint64_t j = 0; j < config.num_coro; j++)
            {
                lmrs[i * config.num_coro + j] = dev.create_mr(cbuf_size, mem_buf + cbuf_size * (i * config.num_coro + j));
                auto cli = new ClientType(config, lmrs[i * config.num_coro + j], rdma_clis[i], rdma_conns[i], config.machine_id, i, j);
                clis.push_back(cli);
            }
        }

        printf("Load start\n");
        auto start = std::chrono::steady_clock::now();
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            auto th = [&](rdma_client *rdma_cli, uint64_t cli_id)
            {
                std::vector<task<>> tasks;
                for (uint64_t j = 0; j < config.num_coro; j++)
                {
                    tasks.emplace_back(load(clis[cli_id * config.num_coro + j], cli_id, j));
                }
                rdma_cli->run(gather(std::move(tasks)));
            };
            ths[i] = std::thread(th, rdma_clis[i], i);
        }
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            ths[i].join();
        }
        auto end = std::chrono::steady_clock::now();
        double op_cnt = 1.0 * load_num;
        double duration = std::chrono::duration<double, std::milli>(end - start).count();
        printf("Load duration:%.2lfms\n", duration);
        printf("Load IOPS:%.2lfKops\n", op_cnt / duration);

        printf("Run start\n");
        auto op_per_coro = config.num_op / (config.num_machine * config.num_cli * config.num_coro);
        std::vector<Generator *> gens;
        for (uint64_t i = 0; i < config.num_cli * config.num_coro; i++)
        {
            if (config.pattern_type == 0){
                gens.push_back(new seq_gen(op_per_coro));
            }else if (config.pattern_type == 1){
                gens.push_back(new uniform(op_per_coro));
            }else if (config.pattern_type == 2){
                gens.push_back(new zipf99(op_per_coro));
            }else{
                gens.push_back(new SkewedLatestGenerator(op_per_coro));
            }
        }
        start = std::chrono::steady_clock::now();
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            auto th = [&](rdma_client *rdma_cli, uint64_t cli_id)
            {
                std::vector<task<>> tasks;
                for (uint64_t j = 0; j < config.num_coro; j++)
                {
                    tasks.emplace_back(run(gens[cli_id * config.num_coro + j], clis[cli_id * config.num_coro + j], cli_id, j));
                }
                rdma_cli->run(gather(std::move(tasks)));
            };
            ths[i] = std::thread(th, rdma_clis[i], i);
        }
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            ths[i].join();
        }
        end = std::chrono::steady_clock::now();
        op_cnt = 1.0 * config.num_op;
        duration = std::chrono::duration<double, std::milli>(end - start).count();
        printf("Run duration:%.2lfms\n", duration);
        printf("Run IOPS:%.2lfKops\n", op_cnt / duration);

        for(auto gen:gens){
            delete gen;
        }

        // Reset Ser
        if (config.machine_id == 0)
        {
            rdma_clis[0]->run(clis[0]->reset_remote());
        }

        free(mem_buf);
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            for (uint64_t j = 0; j < config.num_coro; j++)
            {
                rdma_free_mr(lmrs[i * config.num_coro + j], false);
                delete clis[i * config.num_coro + j];
            }
            delete rdma_conns[i];
            delete rdma_clis[i];
        }
    }
}