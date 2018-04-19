Running throughput and latency measurement of Raft implementation PySyncObj with different |Q2| settings

Go to benchmarks directory:
cd benchmarks

Measure throughput
    with localhost:
    python benchmarks_throughput.py

    with mininet:
    python benchmarks_throughput_mininet.py

Measure latency:
    with localhost:
    python benchmarks_latency.py

    with mininet:
    python benchmarks_latency_mininet.py