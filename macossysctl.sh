sudo sysctl -w kern.ipc.somaxconn=65536
sudo sysctl -w  net.inet.tcp.win_scale_factor=8
sudo sysctl -w  net.inet.tcp.autorcvbufmax=33554432
sudo sysctl -w  net.inet.tcp.autosndbufmax=33554432
sudo sysctl -w  kern.ipc.maxsockbuf= 4194304
sudo sysctl -w  net.inet.tcp.sendspace=1042560
sudo sysctl -w  net.inet.tcp.recvspace=1042560
# http://coryklein.com/tcp/2015/11/25/custom-configuration-of-tcp-socket-keep-alive-timeouts.html
sudo sysctl -w  net.inet.tcp.keepidle=180000
sudo sysctl -w  net.inet.tcp.keepintvl=10000
sudo sysctl -w  net.inet.tcp.keepcnt=3
sudo sysctl -w  net.inet.tcp.sack=0