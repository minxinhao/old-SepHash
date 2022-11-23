make
sshpass -p 'mxh' scp ./ser_cli mxh@192.168.1.88:/home/mxh/
sshpass -p 'mxh' scp ../ser_cli.sh mxh@192.168.1.88:/home/mxh/
echo "mxh" | sudo -S ../ser_cli.sh 0 1>out.txt & sshpass -p 'mxh' ssh mxh@192.168.1.88 " echo "mxh" | sudo -S ~/ser_cli.sh 1 1>out.txt "