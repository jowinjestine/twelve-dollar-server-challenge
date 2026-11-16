#!/bin/bash
# farm/job.sh CELL_JSON : one runner runs EVERY variant of farm/matrix.json back to back (order rotated by
# rep), so the variants are compared on the same hardware. Builds each variant from its own checkout.
set -u
CELL=$1
HERE=$(cd "$(dirname "$0")" && pwd); TOP=$(dirname "$HERE")
j() { echo "$CELL" | jq -r "$1"; }
REP=$(j .rep); USERS=$(j .users); MODEL=$(j .model)
export MEM=$(j .mem) CPUMAX=$(j .cpumax) BURN=$(j .burn)
RAMP=$(j .ramp); HOLD=$(j .hold); DOWN=$(j .down)
W=$TOP/work; OUT=$TOP/out; mkdir -p $W $OUT
NV=$(jq '.variants | length' $HERE/matrix.json)
echo "== job: model $MODEL (cpu.max [$CPUMAX] burn [$BURN] mem $MEM), $USERS users, rep $REP, $NV variants; kernel $(uname -r); $(lscpu | sed -n 's/^Model name: *//p')"

# compilers: on Ubuntu 22.04 the C entries need newer kernel uapi headers (io_uring) than 22.04 ships
CCX=""
if grep -q 22.04 /etc/os-release; then
  if [ ! -d /opt/hdr ]; then
    curl -fsSL --retry 3 -o /tmp/lld.deb http://archive.ubuntu.com/ubuntu/pool/main/l/linux/linux-libc-dev_6.8.0-31.31_amd64.deb && sudo dpkg-deb -x /tmp/lld.deb /opt/hdr
  fi
  G=gcc; for v in 13 12; do command -v gcc-$v >/dev/null && { G=gcc-$v; break; }; done
  CCX="$G -isystem /opt/hdr/usr/include -isystem /opt/hdr/usr/include/x86_64-linux-gnu"
  echo "   22.04: CC for C entries = $CCX"
fi

# sources, seed, install, build
SEED=""
for i in $(seq 0 $((NV-1))); do
  V=$(jq -c ".variants[$i]" $HERE/matrix.json); name=$(echo "$V" | jq -r .name); sha=$(echo "$V" | jq -r .sha); dir=$(echo "$V" | jq -r .dir)
  R=$W/$name
  if [ ! -d $R ]; then
    git init -q $R && git -C $R remote add up https://github.com/arjaythedev/twelve-dollar-server-challenge && git -C $R remote add fork https://github.com/${GITHUB_REPOSITORY:-jowinjestine/twelve-dollar-server-challenge}
    git -C $R fetch -q --depth 1 up $sha 2>/dev/null || git -C $R fetch -q --depth 1 fork $sha || { echo "fetch $sha failed"; exit 1; }
    git -C $R checkout -q FETCH_HEAD
  fi
  if [ -z "$SEED" ]; then
    (cd $R && bash seed/make-seed.sh) || exit 1
    SEED=$R/seed
  fi
  echo "== $name: $(git -C $R log --oneline -1 | cut -c1-90) ($dir)"
  sudo bash $R/submissions/$dir/install.sh > $OUT/install-$name.log 2>&1 || { tail -30 $OUT/install-$name.log; exit 1; }
  CCE=""; [ -n "$CCX" ] && [ -f $R/submissions/$dir/build.sh ] && grep -q 'CC=\${CC:-' $R/submissions/$dir/build.sh && CCE="$CCX"
  if [ -n "$CCE" ]; then CC="$CCE" bash $R/submissions/$dir/build.sh > $OUT/build-$name.log 2>&1; else bash $R/submissions/$dir/build.sh > $OUT/build-$name.log 2>&1; fi \
    || { echo "BUILD FAILED: $name"; tail -40 $OUT/build-$name.log; echo "RESULT $name users=$USERS FAIL(build)" > $OUT/result-$name.txt; continue; }
  tail -1 $OUT/build-$name.log
done

# runs, order rotated by rep
for k in $(seq 0 $((NV-1))); do
  i=$(( (k + REP - 1) % NV ))
  V=$(jq -c ".variants[$i]" $HERE/matrix.json); name=$(echo "$V" | jq -r .name); dir=$(echo "$V" | jq -r .dir)
  [ -f $OUT/result-$name.txt ] && continue # build failed
  [ $k -gt 0 ] && sleep 20
  sudo --preserve-env=MEM,CPUMAX,BURN NAME=$name VENV="$(echo "$V" | jq -r '.env // ""')" SEEDDIR=$SEED OUT=$OUT/$name RUNUSER=$(id -un) \
    bash $HERE/run.sh $W/$name $dir $USERS $RAMP $HOLD $DOWN > /dev/null 2>&1
  sudo chown -R $(id -u) $OUT/$name
  r=$(cat $OUT/$name/result.txt 2>/dev/null || echo "RESULT $name users=$USERS FAIL(no-result)")
  echo "$r order=$((k+1)) model=$MODEL rep=$REP env=[$(echo "$V" | jq -r '.env // ""')]" | tee $OUT/result-$name.txt
done
cat $OUT/result-*.txt | sed 's/^RESULT //' >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
exit 0
