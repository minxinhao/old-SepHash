make
clis=("192.168.1.88" "192.168.1.89")
for cli in ${clis[@]}
do 
    echo "cli" $cli
    sshpass -p 'mxh' scp ./ser_cli mxh@$cli:/home/mxh/
    sshpass -p 'mxh' scp ../ser_cli.sh mxh@$cli:/home/mxh/
done

cnt=1
for cli in ${clis[@]}
do 
    echo $cnt
    sshpass -p 'mxh' ssh mxh@$cli " echo "mxh" | sudo -S ~/ser_cli.sh $cnt 1>out_$cnt.txt " &
    ((cnt += 1))
done
echo "mxh" | sudo -S ../ser_cli.sh 0 1>out.txt

cnt=1
for cli in ${clis[@]}
do 
    echo "cli" $cli
    sshpass -p 'mxh' scp mxh@$cli:/home/mxh/out_$cnt.txt .
    ((cnt += 1))
done