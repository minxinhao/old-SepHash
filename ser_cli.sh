# /bin/bash
# usage: 
#       server: sudo ../ser_cli.sh server
#       client_0: sudo ../ser_cli.sh 0
#       client_1: sudo ../ser_cli.sh 1
if [ "$1" = "server" ]
then
    echo "server"
    ./ser_cli --server \
    --roce \
    --max_coro 256 --cq_size 64 \
    --mem_size 26843545600 
else
    echo "machine" $1

    for read_size in `seq 6 6`;do
        echo "read_size" $((1<<$read_size))
        for num_cli in `seq 0 0`;do
            for num_coro in `seq 1 1`;do
            # for num_coro in 1 2 4;do
                for ((load_num=235000;load_num<=266000;load_num+=1000)); do
=                # for load_num in 1000 10000 100000 1000000 10000000 ;do
                # for load_num in 10000000;do
                    echo "num_cli" $((1<<$num_cli)) "num_coro" $num_coro "load_num" $load_num
                    ./ser_cli \
                    --server_ip 192.168.1.52 --num_machine 1 --num_cli $((1<<$num_cli)) --num_coro $num_coro \
                    --roce \
                    --max_coro 256 --cq_size 64 \
                    --machine_id $1  \
                    --load_num $load_num \
                    --num_op 100 \
                    --pattern_type 3 \
                    --insert_frac 0.0 \
                    --read_frac   1.0 \
                    --update_frac  0.0 \
                    --delete_frac  0.0 \
                    --read_size     $((1<<$read_size))
                done 
            done
        done
    done
fi


# YCSB A : read:0.5,update:0.5 zipfian(2)
# YCSB B : read:0.95,update:0.05 zipfian(2)
# YCSB C : read:1.0,update:0.0 zipfian(2)
# YCSB D : read:0.95,insert:0.5 latest(3)
# YCSB E : scan--不考虑
# YCSB F : read:0.5,rmq:0.5 zipfian(2) -- RMW ，不考虑
