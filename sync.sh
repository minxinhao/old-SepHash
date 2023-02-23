# usage: 
#       ./sync.sh out/in
clis=("192.168.1.88" "192.168.1.89" "192.168.1.33" "192.168.1.44" "192.168.1.51" "192.168.1.52" "192.168.1.53" "192.168.1.69")
if [ "$1" = "out" ]
then
    make
    for cli in ${clis[@]}
    do 
        echo "cli" $cli
        sshpass -p 'mxh' scp ./ser_cli mxh@$cli:/home/mxh/
        sshpass -p 'mxh' scp ../ser_cli.sh mxh@$cli:/home/mxh/
        sshpass -p 'mxh' scp ../run.sh mxh@$cli:/home/mxh/
    done
else
    cnt=1
    for cli in ${clis[@]}
    do 
        echo "cli" $cli
        rm -f ./out$cli.txt
        sshpass -p 'mxh' scp mxh@$cli:/home/mxh/out.txt ./out$cli.txt
        ((cnt += 1))
    done
fi