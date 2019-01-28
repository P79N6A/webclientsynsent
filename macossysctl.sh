sudo sysctl -w kern.ipc.somaxconn=65536
sudo sysctl -w  net.inet.tcp.win_scale_factor=8
sudo sysctl -w  net.inet.tcp.autorcvbufmax=33554432
sudo sysctl -w  net.inet.tcp.autosndbufmax=33554432
sudo sysctl -w  kern.ipc.maxsockbuf= 4194304
sudo sysctl -w  net.inet.tcp.sendspace=1042560
sudo sysctl -w  net.inet.tcp.recvspace=1042560
# http://coryklein.com/tcp/2015/11/25/custom-configuration-of-tcp-socket-keep-alive-timeouts.html
sudo sysctl -w  net.inet.tcp.keepidle=5000
sudo sysctl -w  net.inet.tcp.keepintvl=1000
sudo sysctl -w  net.inet.tcp.keepcnt=3
sudo sysctl -w  net.inet.tcp.sack=0
sudo sysctl -w net.inet.tcp.msl=1000
# https://rerepi.wordpress.com/2008/04/19/tuning-freebsd-sysoev-rit/
sudo sysctl -w net.inet.tcp.randomize_ports=0
sudo sysctl -w net.inet.ip.portrange.first=10240
sudo sysctl -w net.inet.ip.portrange.last=65535
# sudo sysctl -w net.inet.tcp.nolocaltimewait=1
# https://rolande.wordpress.com/2010/12/30/performance-tuning-the-network-stack-on-mac-osx-10-6/
sudo sysctl -w net.inet.tcp.delayed_ack=0
sudo sysctl -w net.inet.tcp.mssdflt=1448
sudo sysctl -w net.inet.tcp.v6mssdflt=1428

# Kernel sysctl configuration file for BSD
#
# Version 1.5 - 2010-08-09
# Michiel Klaver - IT Professional
# http://klaver.it/bsd/ for the latest version - http://klaver.it/linux/ for a linux variant
#
# This file should be saved as /etc/sysctl.conf and can be activated using the command:
# /etc/rc.d/sysctl start
#
# For binary values, 0 is disabled, 1 is enabled.  See sysctl(8) and sysctl.conf(5) for more details.
#
# Tested with: FreeBSD 7.2-RELEASE-p4
#              FreeBSD 6.2-RELEASE-p3
#              MacOS-X 10.6 Snow Leopard

# Intended use for dedicated server systems at high-speed networks with loads of RAM and bandwidth available
# Optimised and tuned for high-performance web/ftp/mail/dns servers with high connection-rates
# DO NOT USE at busy networks or xDSL/Cable connections where packetloss can be expected
# ----------

# Increase some buffers
# sudo sysctl -w kern.ipc.somaxconn=32768
# sudo sysctl -w kern.ipc.maxsockets=32768
# sudo sysctl -w kern.ipc.nmbclusters=32768
# sudo sysctl -w kern.ipc.maxsockbuf=16777216
# sudo sysctl -w net.inet.tcp.sendspace=1048576
# sudo sysctl -w net.inet.tcp.recvspace=1048576
# sudo sysctl -w net.inet.tcp.recvbuf_auto=1
# sudo sysctl -w net.inet.tcp.sendbuf_auto=1
# sudo sysctl -w net.inet.tcp.sendbuf_max=16777216
# sudo sysctl -w net.inet.tcp.recvbuf_max=16777216
# sudo sysctl -w net.inet.udp.maxdgram=57344
# sudo sysctl -w net.inet.udp.recvspace=256960
# sudo sysctl -w net.local.stream.recvspace=65535
# sudo sysctl -w net.local.stream.sendspace=65535
# sudo sysctl -w net.inet.tcp.win_scale_factor=8
# sudo sysctl -w net.inet.tcp.maxtcptw=65535
# sudo sysctl -w net.inet.ip.intr_queue_maxlen=4096
# sudo sysctl -w net.inet.ip.portrange.first=16384
# sudo sysctl -w net.inet.ip.portrange.last=65535
# sudo sysctl -w net.inet.ip.portrange.randomized=0

# Set extra network options
# sudo sysctl -w net.inet.tcp.msl=5000
# sudo sysctl -w net.inet.ip.process_options=0
# sudo sysctl -w net.inet.tcp.inflight.enable=0
# sudo sysctl -w net.inet.tcp.hostcache.expire=1
# sudo sysctl -w net.inet.tcp.sack.enable=1
# sudo sysctl -w net.inet.tcp.log_in_vain=0
# sudo sysctl -w net.inet.udp.log_in_vain=0
# sudo sysctl -w net.inet.udp.checksum=1
# sudo sysctl -w net.inet.tcp.icmp_may_rst=0
# sudo sysctl -w net.inet.icmp.icmplim=200
# sudo sysctl -w net.inet.icmp.icmplim_output=0
# sudo sysctl -w net.inet.tcp.nolocaltimewait=1
# sudo sysctl -w net.inet.tcp.fast_finwait2_recycle=1
# sudo sysctl -w net.inet.tcp.delayed_ack=1
# sudo sysctl -w net.inet.tcp.drop_synfin=1
# sudo sysctl -w net.inet.tcp.keepidle=60000
# sudo sysctl -w net.inet.tcp.ecn.enable=1

# Enable window scaling
# sudo sysctl -w net.inet.tcp.rfc1323=1

# Enable SYN cookies
# sudo sysctl -w net.inet.tcp.syncookies=1

# Disable forwarding
# sudo sysctl -w net.inet.ip.forwarding=0
# sudo sysctl -w net.inet6.ip6.forwarding=0

# Do not accept any redirects
# sudo sysctl -w net.inet.ip.redirect=0
# sudo sysctl -w net.inet.ip6.redirect=0
# sudo sysctl -w net.inet.ip.sourceroute=0
# sudo sysctl -w net.inet.ip.accept_sourceroute=0
# sudo sysctl -w net.inet.icmp.drop_redirect=1
# sudo sysctl -w net.inet.icmp.log_redirect=0
# sudo sysctl -w net.inet.icmp.bmcastecho=0
# sudo sysctl -w net.inet.icmp.maskrepl=0
# sudo sysctl -w net.inet.icmp.maskfake=0

# Some custom security settings for jailed environments
# sudo sysctl -w security.bsd.see_other_uids=0
# sudo sysctl -w security.jail.allow_raw_sockets=1
# sudo sysctl -w security.jail.enforce_statfs=2
# sudo sysctl -w security.jail.set_hostname_allowed=0
# sudo sysctl -w security.jail.socket_unixiproute_only=1
# sudo sysctl -w security.jail.sysvipc_allowed=0
# sudo sysctl -w security.jail.chflags_allowed=0

# filesystem options
# sudo sysctl -w vfs.ufs.dirhash_maxmem=67108864
# sudo sysctl -w vfs.read_max=32

# Increase some extra kernel features
# sudo sysctl -w kern.maxproc=16384
# sudo sysctl -w kern.maxprocperuid=16000
# sudo sysctl -w kern.maxfiles=262144
# sudo sysctl -w kern.maxfilesperproc=200000

# make things difficult for portscanners (this breaks RFCs)
# net.inet.tcp.blackhole=2
# net.inet.udp.blackhole=1
# net.inet.ip.random_id=1
# net.inet.ip.stealth=1
# net.inet.ip.portrange.randomized=1



# kernel loader options, put these in /boot/loader.conf
#
# kern.ipc.nsfbufs=10240
# net.inet.tcp.tcbhashsize=4096
# net.inet.tcp.syncache.hashsize=1024
# net.inet.tcp.syncache.bucketlimit=512
# net.inet.tcp.syncache.cachelimit=65536
# net.inet.tcp.hostcache.hashsize=1024
# net.inet.tcp.hostcache.bucketlimit=512
# net.inet.tcp.hostcache.cachelimit=65536