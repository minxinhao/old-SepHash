make
clis=("192.168.1.88" "192.168.1.89")
for cli in ${clis[@]}
do 
    echo "cli" $cli
    sshpass -p 'mxh' scp ./ser_cli mxh@$cli:/home/mxh/
    sshpass -p 'mxh' scp ../ser_cli.sh mxh@$cli:/home/mxh/
done
# echo "mxh" | sudo -S ../ser_cli.sh 0 1>out.txt & sshpass -p 'mxh' ssh mxh@192.168.1.88 " echo "mxh" | sudo -S ~/ser_cli.sh 1 1>out.txt "