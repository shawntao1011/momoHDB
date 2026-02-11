# momoDB

`momoDB` is a modular market data platform composed of:

1. **momo-feedhandler**  
   A C++ feedhandler that ingests upstream market data and publishes normalized messages.

2. **kafka_decoder**  
   A dynamic library for q, providing Kafka-based data consumption and decoding.

3. **q components**  
   q-side services built on a Kafka-based backbone, replacing the traditional `tick.q` pipeline.

## momo-feedhandler

`momo-feedhandler` is a C++ market data feedhandler.

It connects to upstream market data APIs (e.g. Futu OpenAPI), normalizes and encodes market data,
and publishes them to downstream systems (e.g. Kafka / Redpanda) for storage and further processing.

The project is designed to be:
- configuration-driven (YAML)
- production-oriented
- deployable to environments **without build capability**

---

# Build & Release (Build Machine)

> The build machine **must** have a compiler and all build dependencies installed.

```bash
# configure & build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

# install into a staging directory (no system pollution)
DESTDIR=$PWD/_stage cmake --install build --prefix /opt/momo_feed

# create release archive
tar -C _stage -czf momo_feed.tar.gz .
