# Idea: keep the kernel out of TCP memory pressure at the end of the ramp

Status: **not built, not tested.** This is a parked idea with the evidence behind it and a plan to test it.

## What we saw

In a local KVM guest built to match the rig (1 vCPU, 1,967 MB, Ubuntu 24.04, kernel 6.8.0-117, stock sysctls),
every 310,000-user run with a freshly booted guest showed the same pattern at the end of the 60 s ramp, when about
5,000 new connections a second land on top of the full request load:

| Counter (one 310k run each) | Run A | Run B | Run C |
|---|---|---|---|
| `TcpExtTCPMemoryPressures` | 1 | 1 | 1 |
| `TcpExtTCPMemoryPressuresChrono` (ms under pressure) | 2,343 | 2,438 | 2,455 |
| `TcpExtPruneCalled` (receive queues pruned) | 178,737 | 204,951 | 243,446 |
| `TcpRetransSegs` (almost all tail-loss probes) | 8,130 | 3,320 | 5,259 |

The server's own receive buffers never ran out (`enobufs` near 0), so the memory isn't requests waiting for a
buffer of ours. The server sends about 2 KB per request on average (10.8 GB out per run).

## Why it would matter

- The box's TCP memory limits come from its RAM at boot: `tcp_mem` = 22,365 / 29,823 / 44,730 pages, so the kernel
  enters pressure at about **116 MB** of socket buffer memory and starts dropping at about 175 MB. These are kernel
  parameters, so the rules don't let us change them.
- Under pressure the kernel collapses and prunes receive queues (CPU it spends exactly when the box is busiest),
  shrinks windows, and can drop packets. That turns a backlog of a few hundred milliseconds into retransmits and
  seconds of tail latency, and the score's p99 counts the whole run, ramp included.
- A backlog of ~30,000 requests is what the loop builds at the ramp end. Answering it in one burst parks roughly
  30,000 x 2 KB of unacknowledged responses (more in kernel buffer overhead) in send queues at the same moment as
  the new connections' first requests sit in receive queues: about the 116 MB line.

## What could be done

1. **Watch the kernel's TCP memory and pace answers near the line.** Read the `mem` field of
   `/proc/net/sockstat` (pages in use) once every few milliseconds and compare it with `tcp_mem[1]` from
   `/proc/sys/net/ipv4/tcp_mem`, read once at start (reading is allowed, tuning is not). Above ~80 % of the line,
   cap the answers per batch so unacknowledged send data can drain before more goes out.
2. **Under pressure, read before answering.** Reading a request moves its bytes out of a kernel receive queue,
   which lowers socket memory; answering adds send memory. Near the line, do the reads of a batch first and hold
   back part of the answers.
3. **Combine with admission at the door** (the `c-iouring-jowinjestine-admit` branch): fewer new connections at
   the peak also means fewer first requests sitting in receive queues.

## How to test it

1. Add `sockstat mem` (pages) and the per-interval change of `TCPMemoryPressures` and `PruneCalled` to the stats
   line.
2. In the local VM, at 310k with a freshly booted guest per run, compare interleaved pairs (switch off vs on).
3. It works if the ramp-end pressure episode disappears (`TCPMemoryPressures` 0, `PruneCalled` far below
   200k) and the ramp-end p99 drops, at no cost to the hold.

## Caveats

- All of the evidence comes from the local VM. The rig shares no logs, so it's unknown whether the rig's runs hit
  the same pressure episode.
- In the VM the same build ranges from passing at 665 ms to failing at 3.3 s at 310k, so a small effect needs many
  pairs to show.
