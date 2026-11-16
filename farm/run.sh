#!/bin/bash
# farm/run.sh REPO SUBDIR USERS RAMP HOLD DOWN  (as root, on a 4-vCPU GitHub runner)
# A model of the rig: the server alone on CPU 0 in its own network namespace, reached over a veth
# pair (MTU 1500) whose receive side is steered onto CPU 0 (RPS), so CPU 0 pays the server's own
# receive path the way the rig's single vCPU does, while the client's receive path runs on CPUs 1-3.
# The server runs as the unprivileged runner user, nofile 65535, memlock 8 MB, in a cgroup with
# memory.max=$MEM (the database copy is made inside it, so its page cache counts), no swap.
# Env: MEM (1800M), CPUMAX (max: or "4000 10000" for 40 % of a core), BURN (nice of a busy loop on CPU 0: a slower core), VENV ("K=V ..." server env), RUNUSER (runner), OUT (dir), NAME.
set -u
R=$1; SUB=$2; N=$3; RAMP=$4; HOLD=$5; DOWN=${6:-30}
MEM=${MEM:-1800M}; CPUMAX=${CPUMAX:-max}; VENV=${VENV:-}; RUNUSER=${RUNUSER:-runner}; OUT=${OUT:-/tmp/farm-out}; NAME=${NAME:-x}
HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$OUT"; LOG=$OUT/run.log; exec > >(tee -a "$LOG") 2>&1
echo "== $NAME: $SUB, $N users, ramp $RAMP s, hold $HOLD s, down $DOWN s, mem $MEM, cpu.max $CPUMAX, burn ${BURN:-none}, env [$VENV]"
echo "   kernel $(uname -r), $(nproc) CPUs, $(lscpu | sed -n 's/^Model name: *//p'), $(free -m | awk '/Mem:/{print $2" MB"}')"
gcc -O2 -pthread -o /tmp/loadc "$HERE/loadc.c" || exit 1

swapoff -a 2>/dev/null
sysctl -qw net.core.netdev_max_backlog=10000 net.ipv4.ip_local_port_range="1024 65000" net.ipv4.tcp_tw_reuse=1
# the rig has no conntrack; the runner (docker) does, with a 262,144-entry table: keep the test traffic out of it
sysctl -qw net.netfilter.nf_conntrack_max=4194304 2>/dev/null
for ch in PREROUTING OUTPUT; do iptables -t raw -I $ch -s 10.9.0.0/16 -j NOTRACK; iptables -t raw -I $ch -d 10.9.0.0/16 -j NOTRACK; done
# network: srv namespace with vs (10.9.0.100), host side vc (10.9.0.1 + 10.9.1.2.. for the client)
ip netns del srv 2>/dev/null; ip link del vc 2>/dev/null
ip netns add srv
ip link add vc type veth peer name vs
ip link set vs netns srv
ip addr add 10.9.0.1/16 dev vc
NIP=$(( (N + 24999) / 25000 + 1 ))
for i in $(seq 2 $((NIP + 1))); do ip addr add 10.9.1.$i/16 dev vc; done
ip -n srv addr add 10.9.0.100/16 dev vs
ip link set vc up; ip -n srv link set lo up; ip -n srv link set vs up
ip netns exec srv sh -c 'echo 1 > /sys/class/net/vs/queues/rx-0/rps_cpus'
echo e > /sys/class/net/vc/queues/rx-0/rps_cpus
for ch in PREROUTING OUTPUT; do ip netns exec srv iptables -t raw -I $ch -j NOTRACK; done
echo "   veth: vs rps=$(ip netns exec srv cat /sys/class/net/vs/queues/rx-0/rps_cpus) vc rps=$(cat /sys/class/net/vc/queues/rx-0/rps_cpus), offloads: $(ethtool -k vc 2>/dev/null | grep -E '^(tcp-segmentation-offload|generic-receive-offload):' | tr '\n' ' ')"

# cgroup
G=/sys/fs/cgroup/farm
rmdir $G 2>/dev/null; mkdir -p $G
for c in cpu memory; do grep -qw $c /sys/fs/cgroup/cgroup.subtree_control || echo +$c > /sys/fs/cgroup/cgroup.subtree_control; done
echo "$MEM" > $G/memory.max; echo 0 > $G/memory.swap.max 2>/dev/null; echo "$CPUMAX" > $G/cpu.max 2>/dev/null
D=$(mktemp -d /var/tmp/farmdb.XXXX); chown $RUNUSER $D
( echo $BASHPID > $G/cgroup.procs; exec setpriv --reuid=$RUNUSER --regid=$RUNUSER --init-groups cp "$R/seed/feed.db" $D/feed.db )
sync # written back, still cached (charged to the cgroup), as after the rig's copy
echo "   db $(du -m $D/feed.db | cut -f1) MB on $(df $D | tail -1 | awk '{print $1}'), cgroup memory after copy $(( $(cat $G/memory.current) >> 20 )) MB"

( echo $BASHPID > $G/cgroup.procs
  exec ip netns exec srv prlimit --nofile=65535:65535 --memlock=8388608:8388608 \
    setpriv --reuid=$RUNUSER --regid=$RUNUSER --init-groups \
    env SQLITE_PATH=$D/feed.db JWT_SECRET=twelve-dollar-challenge HOST=10.9.0.100 PORT=3000 $VENV \
    taskset -c 0 bash "$R/submissions/$SUB/start.sh" ) > $OUT/server.log 2>&1 &
SP=$!
ok=0; for _ in $(seq 240); do curl -sf -m 2 http://10.9.0.100:3000/health >/dev/null && { ok=1; break; }; sleep 0.5; done
[ $ok = 1 ] || { echo "SERVER DID NOT START"; tail -30 $OUT/server.log; echo "RESULT $NAME users=$N FAIL(no-start)" > $OUT/result.txt; exit 1; }
echo "   server up (pid $SP), memory $(( $(cat $G/memory.current) >> 20 )) MB"

nstat -n >/dev/null 2>&1
SN0=$(awk '{print strtonum("0x"$2)}' /proc/net/softnet_stat | paste -sd' ')
U0=$(awk '/usage_usec/{print $2}' $G/cpu.stat); T0=$(date +%s%N)
C0A=$(awk '/^cpu0 /{print $2+$3+$4, $5, $6, $7, $8}' /proc/stat)
if [ -n "${BURN:-}" ]; then ( exec taskset -c 0 nice -n $BURN sh -c 'while :; do :; done' ) & BURNER=$!; fi
( while sleep 10; do awk '/^oom_kill /{print $2}' $G/memory.events > $OUT/oom.last; echo $(( $(cat $G/memory.peak) >> 20 )) > $OUT/peak.last; echo "   [mon] cg_mem=$(( $(cat $G/memory.current) >> 20 ))MB $(grep -E '^(anon|file|sock|slab) ' $G/memory.stat | awk '{printf "%s=%dMB ", $1, $2/1048576}') free=$(awk '/MemFree/{print int($2/1024)}' /proc/meminfo)MB tcp:$(grep '^TCP:' /proc/net/sockstat | cut -d' ' -f2-) $(ip netns exec srv cat /proc/net/sockstat | grep '^TCP:' | cut -d' ' -f2-3)"; done ) & MON=$!

ulimit -n 1048576
SRC_BASE=10.9.1.2 CONNS_PER_IP=25000 TOKENS="$R/seed/tokens.json" taskset -c 1-3 /tmp/loadc 10.9.0.100 3000 $N $RAMP $HOLD 6 $DOWN 10 > $OUT/loadc.txt
kill $MON 2>/dev/null; [ -n "${BURNER:-}" ] && kill $BURNER 2>/dev/null
cat $OUT/loadc.txt
U1=$(awk '/usage_usec/{print $2}' $G/cpu.stat); T1=$(date +%s%N)
C0B=$(awk '/^cpu0 /{print $2+$3+$4, $5, $6, $7, $8}' /proc/stat)
CPU=$(( (U1-U0)*100 / ((T1-T0)/1000) ))
PEAK=$(cat $OUT/peak.last 2>/dev/null || echo 0) # last sample before the client's exit closed every connection
SN1=$(awk '{print strtonum("0x"$2)}' /proc/net/softnet_stat | paste -sd' ')
DROPS=$(echo "$SN0 $SN1" | awk '{n=NF/2; s=0; for(i=1;i<=n;i++) s+=$(i+n)-$i; print s}')
NST=$(ip netns exec srv nstat -az 2>/dev/null)
RETR=$(nstat -z 2>/dev/null | awk '/TcpRetransSegs/{print $2}')
SRETR=$(echo "$NST" | awk '/TcpRetransSegs/{print $2}')
C0=$(echo $C0A $C0B | awk '{t=($6-$1)+($7-$2)+($8-$3)+($9-$4)+($10-$5); printf "task=%.0f%% idle=%.0f%% softirq=%.0f%%", ($6-$1)*100/t, ($7-$2)*100/t, ($10-$5)*100/t}')
EV=$(grep -E '^(high|max|oom|oom_kill) ' $G/memory.events | awk '{printf "%s=%s ", $1, $2}')
echo "== cgroup cpu $CPU % of a core, memory.peak $PEAK MB, events: $EV"
echo "== CPU 0: $C0; softnet drops $DROPS; retrans client-ns=$RETR server-ns=$SRETR; conntrack count $(cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null), 'table full' messages $(dmesg 2>/dev/null | grep -c 'table full')"
echo "== server netns TCP counters (non-zero)"; echo "$NST" | awk '$2>0 && $1 ~ /^(Tcp|TcpExt)/ {printf "%s=%s ", $1, $2} END {print ""}' | fold -w 300
echo "== server log (last 12 lines)"; tail -12 $OUT/server.log | cut -c1-400
W=$(grep '^whole run' $OUT/loadc.txt); H=$(grep '^hold only' $OUT/loadc.txt)
num() { echo "$1" | grep -o "$2 [0-9.]* ms" | awk '{print $2}'; }
RPS=$(echo "$W" | grep -o '([0-9]* req/s)' | tr -dc 0-9)
FAILP=$(echo "$W" | grep -o 'failed [0-9.]* %' | awk '{print $2}')
VERD=$(echo "$W" | grep -o 'PASS\|FAIL')
LINE="RESULT $NAME users=$N $VERD whole_p95=$(num "$W" p95) whole_p99=$(num "$W" p99) hold_p99=$(num "$H" p99) max=$(echo "$W" | grep -o 'max [0-9]* ms' | awk '{print $2}') fail%=$FAILP req/s=$RPS cpu=$CPU% peak=${PEAK}MB oom=$(cat $OUT/oom.last 2>/dev/null) drops=$DROPS retr=$SRETR cpu0[$C0] kernel=$(uname -r) cpumodel=$(lscpu | sed -n 's/^Model name: *//p' | tr -d ' ()' | cut -c1-24)"
echo "$LINE" | tee $OUT/result.txt
kill -TERM $SP 2>/dev/null; pkill -TERM -f "submissions/$SUB" 2>/dev/null; sleep 2; rm -rf $D
exit 0
