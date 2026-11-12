# AGENTS.md: p2p

Instructions for coding agents working in this repository. Read this before changing anything.

## What this is

A [Kademlia](https://en.wikipedia.org/wiki/Kademlia)-based peer-to-peer network library in C++17
(UDP discovery, a routing table of k-buckets, TCP connections, ban management, UPnP). It is the
transport of the CREDITS node: [CreditsCommunity/node](https://github.com/CreditsCommunity/node)
includes it as the git submodule `p2p/` and builds it with `add_subdirectory(p2p)`; the node's
`net/` layer sits on top of it.

Related repositories: `node` (the only consumer), `contract-executor`, `smart-contract-api`,
`thrift-api-client`, `wallet`, all in the [CreditsCommunity](https://github.com/CreditsCommunity)
organisation.

## Branches: read this first

- The node does not use `master`. Its submodule is pinned to a commit of the branch
  `community/p9` (check with `git ls-tree HEAD p2p` in the node). Fixes the node needs are based on
  the commit the node pins; the node then bumps its gitlink in a PR of its own.
- `master` is the default branch but has diverged from `community/p9`; an open PR proposes merging
  `community/p9` into it. Check the state of both before basing work on either.
- A change here reaches mainnet only through a node release. The wire format (datagrams, packet
  headers, message framing) is shared with every node on the network, including the official node
  4.2.532: changing it breaks compatibility and must not be done unilaterally.

## Layout

| Path | Contents |
|---|---|
| `include/p2p_network.h` | the public interface: `net::Manager`, `ManagerConfig`, `EventHandler`, `NodeId` |
| `include/arith_uint256.h` | 256-bit arithmetic for node ids and distances |
| `src/host.*`, `src/network.*` | the host: connections, message dispatch, start and shutdown |
| `src/routing_table.*`, `src/k_bucket.h`, `src/pinger.cc` | Kademlia routing table, k-buckets, liveness pings |
| `src/kademlia_datagram.*`, `src/udp.h`, `src/net_explorer.cc` | UDP discovery datagrams, node lookup |
| `src/connection.*` | TCP connections and packet framing |
| `src/banman.*` | banned peers |
| `src/utils/` | logging, local IP detection (POSIX and Windows), serialization |
| `src/third-party/` | vendored base58, sha1, UPnP glue |
| `third-party/` | submodules `leveldb`, `miniupnp`; `easyloggingpp` vendored |

## Build

Requirements (README.md): Boost 1.68+ (CMake asks for 1.66+ with `system`), CMake 3.13+, a C++17
compiler. Boost is linked statically.

```sh
git submodule update --init --recursive
cmake -S . -B build -DBOOST_ROOT=$BOOST_ROOT -DCMAKE_INSTALL_PREFIX=$P2P_INSTALL_ROOT
cmake --build build --target install
```

There are no unit tests and no linter in this repository. The real test is the node: build it with
this commit as its `p2p` submodule and run its CI (unit tests, Linux/Windows/macOS builds, and the
private test network `testnet.yml` for anything that changes network behaviour).

## CI

None here. Changes are validated through a node PR that bumps the submodule.

## Conventions

- One topic per pull request. Commit subjects name the part they change
  (`host: ...`, `routing table: ...`, `connection: ...`, `cmake: ...`).
- Follow the surrounding style (Google-like C++: `CamelCase` types and methods, `snake_case_`
  members); the build uses `/W4` on MSVC.
- Keep Windows (MSVC), Linux (GCC) and macOS (clang) building: the node ships on all three.
- Docs, comments and commit messages in English.

## Git pitfalls

- Never `git add -A` or `git add .`: stage explicit paths, and check that the `third-party`
  submodule gitlinks did not change (`git diff --cached --raw` shows no `160000` entry).
- Never delete branches after a merge.

## Pitfalls

- Code here handles data from untrusted peers: bound every size read from the network before
  allocating or copying (see the 256 MiB packet header cap on `community/p9`).
- The host runs a Boost.Asio `io_context` on its own thread and the net explorer runs discovery on
  another: shared state needs its lock (several fixes on both branches were data races).
- Network-facing changes go to the node's private test network before mainnet.

## More

- `README.md` (basic usage), `include/p2p_network.h` (interface comments)
- The node's `docs/differences.md` and `docs/CHANGES.md` record the p2p changes it ships.
- Maintainers' agents: a shared Basic Memory knowledge base (project `credits`) holds platform facts,
  procedures and incident notes; use it when your environment provides it.
