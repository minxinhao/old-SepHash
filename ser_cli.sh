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
    --mem_size 19327352832 
else
    for num_cli in `seq 0 0`;do
        for num_coro in `seq 2 2`;do
            echo "num_cli" $((1<<$num_cli)) "num_coro" $num_coro 
            ./ser_cli \
            --server_ip 192.168.1.44 --num_machine 1 --num_cli $((1<<$num_cli)) --num_coro $num_coro \
            --roce \
            --max_coro 256 --cq_size 64 \
            --machine_id $1  \
            --num_op 1000000
        done 
    done
fi


