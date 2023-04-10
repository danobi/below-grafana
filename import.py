#!/bin/env python3

import argparse
from collections import namedtuple
from enum import Enum
import json
import logging
import os
import string
import subprocess
import random
import tempfile
import time

DOCKER_BIN = os.environ.get("DOCKER", "docker")

TIMESTAMP_KEY = "Timestamp"
IGNORED_KEYS = {
    TIMESTAMP_KEY,
    "Datetime",
    "Hostname",
    "Kernel Version",
    "OS Release",
}

# Set of unknown keys we've already warned about
WARNED = set()


class MetricType(Enum):
    """
    The OpenMetrics type of the metric.

    Note we do not support all types, as below's data model is fairly simple.

    See also: https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#type
    """

    GAUGE = 1
    COUNTER = 2


Metric = namedtuple("Metric", "prometheus_key metric_type")


NETWORKING_METRICS = {
    "Icmp6InDestUnreachs": Metric("icmp6.in_dest_unreachs", MetricType.COUNTER),
    "Icmp6InErrs": Metric("icmp6.in_errors", MetricType.COUNTER),
    "Icmp6InMsg/s": Metric("icmp6.in_msgs_per_sec", MetricType.GAUGE),
    "Icmp6OutDestUnreachs": Metric("icmp6.out_dest_unreachs", MetricType.COUNTER),
    "Icmp6OutErrs": Metric("icmp6.out_errors", MetricType.COUNTER),
    "Icmp6OutMsg/s": Metric("icmp6.out_msgs_per_sec", MetricType.GAUGE),
    "IcmpInDestUnreachs": Metric("icmp.in_dest_unreachs", MetricType.COUNTER),
    "IcmpInErrs": Metric("icmp.in_errors", MetricType.COUNTER),
    "IcmpInMsg/s": Metric("icmp.in_msgs_per_sec", MetricType.GAUGE),
    "IcmpOutDestUnreachs": Metric("icmp.out_dest_unreachs", MetricType.COUNTER),
    "IcmpOutErrs": Metric("icmp.out_errors", MetricType.COUNTER),
    "IcmpOutMsg/s": Metric("icmp.out_msgs_per_sec", MetricType.GAUGE),
    "Ip6ForwDatagrams/s": Metric("ip6.out_forw_datagrams_per_sec", MetricType.GAUGE),
    "Ip6InAddrErrs": Metric("ip6.in_addr_errors", MetricType.COUNTER),
    "Ip6InBcastOctets/s": Metric("ip6.in_bcast_octets_per_sec", MetricType.GAUGE),
    "Ip6InDeliversPkts/s": Metric("ip6.in_delivers_pkts_per_sec", MetricType.GAUGE),
    "Ip6InDiscardsPkts/s": Metric("ip6.in_discards_pkts_per_sec", MetricType.GAUGE),
    "Ip6InHdrErrs": Metric("ip6.in_hdr_errors", MetricType.COUNTER),
    "Ip6InMcastOctets/s": Metric("ip6.in_mcast_octets_per_sec", MetricType.GAUGE),
    "Ip6InMcastPkts/s": Metric("ip6.in_mcast_pkts_per_sec", MetricType.GAUGE),
    "Ip6InNoRoutesPkts/s": Metric("ip6.in_no_routes_pkts_per_sec", MetricType.GAUGE),
    "Ip6InOctets/s": Metric("ip6.in_octets_per_sec", MetricType.GAUGE),
    "Ip6InPkts/s": Metric("ip6.in_receives_pkts_per_sec", MetricType.GAUGE),
    "Ip6OutBcastOctets/s": Metric("ip6.out_bcast_octets_per_sec", MetricType.GAUGE),
    "Ip6OutMcastOctets/s": Metric("ip6.out_mcast_octets_per_sec", MetricType.GAUGE),
    "Ip6OutMcastPkts/s": Metric("ip6.out_mcast_pkts_per_sec", MetricType.GAUGE),
    "Ip6OutNoRoutesPkts/s": Metric("ip6.out_no_routes_pkts_per_sec", MetricType.GAUGE),
    "Ip6OutOctets/s": Metric("ip6.out_octets_per_sec", MetricType.GAUGE),
    "Ip6OutReqs/s": Metric("ip6.out_requests_per_sec", MetricType.GAUGE),
    "IpForwDatagrams/s": Metric("ip.forw_datagrams_per_sec", MetricType.GAUGE),
    "IpForwPkts/s": Metric("ip.forwarding_pkts_per_sec", MetricType.GAUGE),
    "IpInBcastOctets/s": Metric("ip.in_bcast_octets_per_sec", MetricType.GAUGE),
    "IpInBcastPkts/s": Metric("ip.in_bcast_pkts_per_sec", MetricType.GAUGE),
    "IpInDeliversPkts/s": Metric("ip.in_delivers_pkts_per_sec", MetricType.GAUGE),
    "IpInDiscardPkts/s": Metric("ip.in_discards_pkts_per_sec", MetricType.GAUGE),
    "IpInMcastOctets/s": Metric("ip.in_mcast_octets_per_sec", MetricType.GAUGE),
    "IpInMcastPkts/s": Metric("ip.in_mcast_pkts_per_sec", MetricType.GAUGE),
    "IpInNoEctPkts/s": Metric("ip.in_no_ect_pkts_per_sec", MetricType.GAUGE),
    "IpInOctets/s": Metric("ip.in_octets_per_sec", MetricType.GAUGE),
    "IpInPkts/s": Metric("ip.in_receives_pkts_per_sec", MetricType.GAUGE),
    "IpOutBcastOctets/s": Metric("ip.out_bcast_octets_per_sec", MetricType.GAUGE),
    "IpOutBcastPkts/s": Metric("ip.out_bcast_pkts_per_sec", MetricType.GAUGE),
    "IpOutDiscardPkts/s": Metric("ip.out_discards_pkts_per_sec", MetricType.GAUGE),
    "IpOutMcastOctets/s": Metric("ip.out_mcast_octets_per_sec", MetricType.GAUGE),
    "IpOutMcastPkts/s": Metric("ip.out_mcast_pkts_per_sec", MetricType.GAUGE),
    "IpOutNoRoutesPkts/s": Metric("ip.out_no_routes_pkts_per_sec", MetricType.GAUGE),
    "IpOutOctets/s": Metric("ip.out_octets_per_sec", MetricType.GAUGE),
    "IpOutReqs/s": Metric("ip.out_requests_per_sec", MetricType.GAUGE),
}

SYSTEM_METRICS = {
    "Blocked Procs": Metric("stat.blocked_processes", MetricType.GAUGE),
    "Boot Time Epoch": Metric("stat.boot_time_epoch_secs", MetricType.COUNTER),
    "Context Switches": Metric("stat.context_switches", MetricType.COUNTER),
    "Free": Metric("mem.free", MetricType.GAUGE),
    "OOM Kills": Metric("vm.oom_kill", MetricType.COUNTER),
    "Page In": Metric("vm.pgpgin_per_sec", MetricType.GAUGE),
    "Page Out": Metric("vm.pgpgout_per_sec", MetricType.GAUGE),
    "Pgscan Direct": Metric("vm.pgscan_direct_per_sec", MetricType.GAUGE),
    "Pgscan Kswapd": Metric("vm.pgscan_kswapd_per_sec", MetricType.GAUGE),
    "Pgsteal Direct": Metric("vm.pgsteal_direct_per_sec", MetricType.GAUGE),
    "Pgsteal Kswapd": Metric("vm.pgsteal_kswapd_per_sec", MetricType.GAUGE),
    "Running Procs": Metric("stat.running_processes", MetricType.GAUGE),
    "Swap In": Metric("vm.pswpin_per_sec", MetricType.GAUGE),
    "Swap Out": Metric("vm.pswpout_per_sec", MetricType.GAUGE),
    "System": Metric("cpu.system_pct", MetricType.GAUGE),
    "Total": Metric("mem.total_gb", MetricType.GAUGE),
    "Total Interrupts": Metric("tat.total_interrupt_ct", MetricType.COUNTER),
    "Total Procs": Metric("stat.total_processes", MetricType.COUNTER),
    "Usage": Metric("cpu.usage_pct", MetricType.GAUGE),
    "User": Metric("cpu.user_pct", MetricType.GAUGE),
    "SoftIrq": Metric("cpu.softirq_pct", MetricType.GAUGE),
}

METRICS = [
    ("network", NETWORKING_METRICS),
    ("system", SYSTEM_METRICS),
]


def get_below_bin(snapshot):
    """Get the below "binary" to run"""
    env = os.environ.get("BELOW")
    if env:
        return env.split()
    else:
        volume_args = []
        if snapshot:
            volume_args += ["-v", f"{snapshot}:{snapshot}"]

        return [
            "docker",
            "run",
            "--rm",
            *volume_args,
            "below/below:latest",
        ]


def dump(source, category, begin, end):
    """Shells out to below and returns a decoded JSON blob of all data points"""
    if source.lower() == "host":
        below_source = None
        below_source_args = []
    else:
        below_source = source
        below_source_args = ["--snapshot", source]

    cmd = [
        *get_below_bin(below_source),
        "dump",
        *below_source_args,
        category,
        "--begin",
        begin,
        "--end",
        end,
        "--everything",
        "--output-format",
        "json",
    ]
    cmd_str = " ".join(cmd)
    logging.info(f"Dumping {category} data with cmd='{cmd_str}'")

    process = subprocess.run(cmd, capture_output=True, encoding="utf-8")
    if process.returncode != 0:
        logging.error(f"process stdout={process.stdout}, stderr={process.stderr}")
        raise RuntimeError(f"Failed to dump {category} data: {process.stderr}")

    return json.loads(process.stdout)


def sanitize_metric_name(raw):
    """Sanitizes a metric name to openmetrics specifications"""
    return raw.replace(".", "_")


def sanitize_metric_value(key, raw):
    """Sanitizes a metric value to openmetrics specifications"""
    parts = raw.split()
    if not parts:
        raise RuntimeError(f"Invalid value found for key={key}: '{raw}'")

    # We don't want the '%' for percentages
    val = parts[0].replace("%", "")
    # Usually the very first dataframe will have a bunch of '?'
    # which can safely be replaced with 0.
    val = val.replace("?", "0")

    return val


def convert_frame(frame, schema, prefix):
    """Converts a single dataframe"""
    converted = []

    # Extract unix timestamp that all metrics will need
    timestamp = frame.get(TIMESTAMP_KEY, None)
    if not timestamp:
        raise RuntimeError("Timestamp not found in frame")

    for k, v in frame.items():
        # Handle unknown metrics
        metric = schema.get(k, None)
        if not metric:
            if k not in IGNORED_KEYS and k not in WARNED:
                logging.warning(f"Unknown key='{k}' found during conversion")
                WARNED.add(k)
            continue

        # Generate prometheus key and value
        prom_key = sanitize_metric_name(f"{prefix}_{metric.prometheus_key}")
        prom_value = sanitize_metric_value(k, v)

        # Emit type descriptor
        if metric.metric_type == MetricType.GAUGE:
            type_str = "gauge"
        elif metric.metric_type == MetricType.COUNTER:
            type_str = "counter"
        else:
            logging.warning(f"Unknown metric type value: {metric.metric_type}")
            continue
        converted.append(f"# TYPE {prom_key} {type_str}\n")

        # Emit help descriptor placeholder
        converted.append(f"# HELP {prom_key} {k}\n")

        # Emit value
        converted.append(f"{prom_key} {prom_value} {timestamp}\n")

    return converted


def convert(data, schema, prefix):
    """
    Convert decoded below dump json data into openmetrics format.

    Returns an array where each entry is a line terminated with a newline.
    """
    logging.info(f"Converting {len(data)} frames to OpenMetrics")
    converted = []
    for frame in data:
        converted += convert_frame(frame, schema, prefix)

    return converted


def ingest(metrics_file):
    """Ingest metrics into prometheus"""
    subprocess.run(
        [DOCKER_BIN, "compose", "cp", metrics_file, "prometheus:/import.txt"],
        check=True,
    )
    subprocess.run(
        [
            DOCKER_BIN,
            "compose",
            "exec",
            "prometheus",
            "promtool",
            "tsdb",
            "create-blocks-from",
            "openmetrics",
            "/import.txt",
            "/prometheus",
        ],
        check=True,
    )
    subprocess.run([DOCKER_BIN, "compose", "restart", "prometheus"], check=True)


def do_import(begin, end, source, prefix):
    logging.info(f"Importing {source} with prefix {prefix}, from '{begin}' to '{end}'")
    with tempfile.NamedTemporaryFile(mode="w") as f:
        for category, schema in METRICS:
            data = dump(source, category, begin, end)
            metrics = convert(data, schema, prefix)
            f.writelines(metrics)

        f.write("# EOF\n")

        # Need to flush in-memory buffers before doing a cp
        f.flush()

        # Need to chmod b/c there's a uid/gid mismatch when copying into container
        os.chmod(f.name, 0o644)
        ingest(f.name)


def main():
    parser = argparse.ArgumentParser(description="Imports below data into prometheus")
    parser.add_argument("--begin", "-b", default="99 years ago", help="Import start")
    parser.add_argument("--end", "-e", default="now", help="Import end")
    parser.add_argument("--prefix", "-p", help="Prefix for all imported metrics")
    parser.add_argument("source", help="Path to snapshot or `host`, for local host")
    args = parser.parse_args()

    prefix = args.prefix or "".join(random.choices(string.ascii_lowercase, k=5))
    start = time.time()
    do_import(args.begin, args.end, args.source, prefix)
    logging.info(f"Done in {time.time() - start}s")

    print(f"=========================")
    print(f"Imported metrics under prefix '{prefix}'")
    print(f"=========================")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
