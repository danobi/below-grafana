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

# TODO: use below docker image
BELOW_BIN = os.environ.get("BELOW", "below")
DATETIME_KEY = "Datetime"
TIMESTAMP_KEY = "Timestamp"


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


def dump(source, category, begin, end):
    """Shells out to below and returns a decoded JSON blob of all data points"""
    if source.lower() == "host":
        below_source = []
    else:
        below_source = ["--snapshot", source]

    cmd = [
        BELOW_BIN,
        "dump",
        *below_source,
        category,
        "--begin",
        begin,
        "--end",
        end,
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


def convert_frame(frame, metrics, prefix):
    """Converts a single dataframe"""
    warned = set()
    converted = []

    # Extract unix timestamp that all metrics will need
    timestamp = frame.get(TIMESTAMP_KEY, None)
    if not timestamp:
        raise RuntimeError("Timestamp not found in frame")

    for k, v in frame.items():
        # Handle unknown metrics
        metric = metrics.get(k, None)
        if not metric:
            if k != DATETIME_KEY and k != TIMESTAMP_KEY and k not in warned:
                logging.warning(f"Unknown key={k} found during conversion")
                warned.add(k)
            continue

        # Extract value we'll tell prometheus about
        parts = v.split()
        if not parts:
            logging.warning(f"Invalid value found for key={k}: '{v}'")
            continue
        prom_value = parts[0]

        # Generate prometheus key
        prom_key = sanitize_metric_name(f"{prefix}_{metric.prometheus_key}")

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
        converted.append(f"# HELP {prom_key} {k}; TODO: generate metric help\n")

        # Emit value
        converted.append(f"{prom_key} {prom_value} {timestamp}\n")

    return converted


def convert(data, metrics, prefix):
    """
    Convert decoded below dump json data into openmetrics format.

    Returns an array where each entry is a line terminated with a newline.
    """
    logging.info(f"Converting {len(data)} frames to OpenMetrics")
    converted = []
    for frame in data:
        converted += convert_frame(frame, metrics, prefix)
    converted.append("# EOF\n")

    return converted


def ingest(metrics_file):
    """Ingest metrics into prometheus"""
    subprocess.run(
        ["docker", "compose", "cp", metrics_file, "prometheus:/import.txt"], check=True
    )
    subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "prometheus",
            "promtool",
            "tsdb",
            "create-blocks-from",
            "openmetrics",
            "/import.txt",
            "/prometheus/data",
        ],
        check=True,
    )
    subprocess.run(
        ["docker", "compose", "restart", "prometheus"], check=True
    )


def do_import(begin, end, source, prefix):
    logging.info(f"Importing {source} with prefix {prefix}, from '{begin}' to '{end}'")
    with tempfile.NamedTemporaryFile(mode="w") as f:
        data = dump(source, "network", begin, end)
        networking = convert(data, NETWORKING_METRICS, prefix)
        f.writelines(networking)

        ingest(f.name)


def main():
    parser = argparse.ArgumentParser(description="Imports below data into prometheus")
    parser.add_argument("--begin", "-b", help="Start of import interval")
    parser.add_argument("--end", "-e", help="End of import interval")
    parser.add_argument("--prefix", "-p", help="Prefix for all imported metrics")
    parser.add_argument("source", help="Path to snapshot or `local`, for local host")
    args = parser.parse_args()

    begin = args.begin if args.begin else "99 years ago"
    end = args.end if args.end else "now"
    if args.prefix:
        prefix = args.prefix
    else:
        prefix = "".join(random.choices(string.ascii_lowercase, k=5))

    start = time.time()
    do_import(begin, end, args.source, prefix)
    logging.info(f"Done in {time.time() - start}s")

    print(f"=========================")
    print(f"Imported metrics under prefix '{prefix}'")
    print(f"=========================")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
