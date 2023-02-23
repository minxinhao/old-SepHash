# /bin/bash
# usage: 
#       server: sudo ../ser_cli.sh server
#       client_0: sudo ../ser_cli.sh 0
#       client_1: sudo ../ser_cli.sh 1

# git checkout .
# git pull
# cd build && make

# clis=("192.168.1.33" "192.168.1.44" "192.168.1.51" "192.168.1.52" "192.168.1.53" "192.168.1.88" "192.168.1.69")

# cnt=1
# for cli in ${clis[@]}
# do 
#     echo $cnt
#     sshpass -p 'mxh' ssh mxh@$cli " echo "mxh" | sudo -S ~/ser_cli.sh $cnt 1>out_$cnt.txt " &
#     ((cnt += 1))
# done
# echo "mxh" | sudo -S ../ser_cli.sh 0 1>out.txt


./ser_cli.sh $1 1>out.txt
