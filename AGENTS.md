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

- **`master`** (the default branch) has the tree of `community/p9`, the branch the node uses, plus
  AGENTS.md, CLAUDE.md and the CI. Its history also contains the earlier upstream commits of
  `master` (`08a835d`..`fde465a`: the `net::Manager` interface and install approach, a cmake fix,
  a host data race fix identical to `8fb3c51`, already in `community/p9`, and a pinger duplicate
  fix), merged in November 2026 without their changes: that
  interface still exposes the fragment API `community/p9` removed, and the node does not use it.
  Their code is in the history if one of them is ever wanted (the pinger fix `fde465a` has not
  been evaluated for the node).
- The node's submodule is pinned to commit `09252ec` of `community/p9` (check with
  `git ls-tree HEAD p2p` in the node). Do not move or rewrite `community/p9`. New work is based on
  `master`; the node then bumps its gitlink in a PR of its own.
- `creditscom` holds the original author's later line (`8fb3c51` + a pinger fix + leveldb removed).
- A change here reaches mainnet only through a node release. The wire format (datagrams, packet
  headers, message framing) is shared with every node on the network, including the official node
  4.2.532: changing it breaks compatibility and must not be done unilaterally.

## Layout

| Path | Contents |
|---|---|
| `include/p2p_network.h` | the public header: includes `src/host.h` |
| `src/host.*`, `src/network.*` | the interface the node uses (`net::Host`, `HostEventHandler`) and the host: connections, message dispatch, start and shutdown |
| `src/common.h` | `Config`, `NodeId` (`arith_uint256`), packet limits |
| `src/routing_table.*`, `src/k_bucket.h`, `src/pinger.cc` | Kademlia routing table, k-buckets, liveness pings |
| `src/kademlia_datagram.*`, `src/udp.h`, `src/net_explorer.cc` | UDP discovery datagrams, node lookup |
| `src/connection.*` | TCP connections and packet framing |
| `src/banman.*` | banned peers |
| `src/utils/` | logging, local IP detection (POSIX and Windows), serialization |
| `src/third-party/` | vendored sha1, UPnP glue |
| `third-party/` | submodules `leveldb`, `miniupnp`; `easyloggingpp` and `bitcoin_utils` (base58, `arith_uint256`) vendored |

## Build

Requirements (README.md): Boost 1.68+ (CMake asks for 1.66+ with `system`), CMake 3.13+, a C++17
compiler. Boost is linked statically with a static runtime (`Boost_USE_STATIC_RUNTIME`), which
distribution packages do not provide: build Boost with `runtime-link=static` (see the CI). GCC 13
needs `-include cstdint -include cstddef` (vendored base58 relies on transitive includes), as in
the node's CI.

```sh
git submodule update --init --recursive
cmake -S . -B build -DBoost_ROOT=$BOOST_ROOT -DBoost_NO_SYSTEM_PATHS=ON \
  -DCMAKE_CXX_FLAGS="-include cstdint -include cstddef" -DCMAKE_INSTALL_PREFIX=$P2P_INSTALL_ROOT
cmake --build build --target install
```

There are no unit tests and no linter in this repository. The real test is the node: build it with
this commit as its `p2p` submodule and run its CI (unit tests, Linux/Windows/macOS builds, and the
private test network `testnet.yml` for anything that changes network behaviour).

## CI

`.github/workflows/ci.yml` builds and installs the library on Linux (GCC 13, Boost 1.83 built with a
static runtime and cached) on every push to `master` and every pull request. It has no tests to
run: changes are validated through a node PR that bumps the submodule.

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
