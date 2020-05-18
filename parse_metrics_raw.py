import csv
import json
import re
import pandas as pd
import numpy as np
from sklearn.datasets import load_iris


exclude_keys = [
    'producer-node_request-latency-avg_-1',
    'producer-node_request-latency-avg_-2',
    'producer-node_request-latency-avg_-3',
    'producer-node_incoming-byte-rate_-1',
    'producer-node_incoming-byte-rate_-2',
    'producer-node_incoming-byte-rate_-3',
    'producer-node_incoming-byte-total_-1',
    'producer-node_incoming-byte-total_-2',
    'producer-node_incoming-byte-total_-3',
    'producer-node_outgoing-byte-rate_-1',
    'producer-node_outgoing-byte-rate_-2',
    'producer-node_outgoing-byte-rate_-3',
    'producer-node_outgoing-byte-total_-1',
    'producer-node_outgoing-byte-total_-2',
    'producer-node_outgoing-byte-total_-3',
    'producer-node_request-latency-max_-1',
    'producer-node_request-latency-max_-2',
    'producer-node_request-latency-max_-3',
    'producer-node_request-rate_-1',
    'producer-node_request-rate_-2',
    'producer-node_request-rate_-3',
    'producer-node_request-size-avg_-1',
    'producer-node_request-size-avg_-2',
    'producer-node_request-size-avg_-3',
    'producer-node_request-size-max_-1',
    'producer-node_request-size-max_-2',
    'producer-node_request-size-max_-3',
    'producer-node_request-total_-1',
    'producer-node_request-total_-2',
    'producer-node_request-total_-3',
    'producer-node_response-rate_-1',
    'producer-node_response-rate_-2',
    'producer-node_response-rate_-3',
    'producer-node_response-total_-1',
    'producer-node_response-total_-2',
    'producer-node_response-total_-3',
    'producer_reauthentication-latency-avg',
    'producer_reauthentication-latency-max',
    'producer_successful-authentication-no-reauth-total',
    'producer_successful-authentication-rate',
    'producer_successful-authentication-total',
    'producer_successful-reauthentication-rate',
    'producer_successful-reauthentication-total',
    'producer_failed-authentication-rate',
    'producer_failed-authentication-total',
    'producer_failed-reauthentication-rate',
    'producer_failed-reauthentication-total',

    "producer-topic_record-send-total",
    "producer-topic_record-send-rate",
    "producer-topic_record-retry-total",
    "producer-topic_record-retry-rate",
    "producer-topic_compression-rate",
    "producer-topic_byte-total",
    "producer-topic_byte-rate",
    "producer-node_response-total_3",
    "producer-node_response-total_2",
    "producer-node_response-total_1",
    "producer-node_response-rate_3",
    "producer-node_response-rate_2",
    "producer-node_response-rate_1",
    "producer-node_request-total_3",
    "producer-node_request-total_2",
    "producer-node_request-total_1",
    "producer-node_request-size-max_3",
    "producer-node_request-size-max_2",
    "producer-node_request-size-max_1",
    "producer-node_request-size-avg_3",
    "producer-node_request-size-avg_2",
    "producer-node_request-size-avg_1",
    "producer-node_request-rate_3",
    "producer-node_request-rate_2",
    "producer-node_request-rate_1",
    "producer-node_request-latency-max_3",
    "producer-node_request-latency-max_2",
    "producer-node_request-latency-max_1",
    "producer-node_request-latency-avg_3",
    "producer-node_request-latency-avg_2",
    "producer-node_request-latency-avg_1",
    "producer-node_outgoing-byte-total_3",
    "producer-node_outgoing-byte-total_2",
    "producer-node_outgoing-byte-total_1",
    "producer-node_outgoing-byte-rate_3",
    "producer-node_outgoing-byte-rate_2",
    "producer-node_outgoing-byte-rate_1",
    "producer-node_incoming-byte-total_3",
    "producer-node_incoming-byte-total_2",
    "producer-node_incoming-byte-total_1",
    "producer-node_incoming-byte-rate_3",
    "producer-node_incoming-byte-rate_2",
    "producer-node_incoming-byte-rate_1",
    "producer_select-total",
    "producer_response-total",
    "producer_response-rate",
    "producer_request-total",
    "producer_request-size-max",
    "producer_request-size-avg",
    "producer_request-rate",
    "producer_record-size-max",
    "producer_record-size-avg",
    "producer_record-send-total",
    "producer_record-send-rate",
    "producer_record-retry-total",
    "producer_record-queue-time-max",
    "producer_record-error-total",
    "producer_record-error-rate",
    "producer_outgoing-byte-total",
    "producer_outgoing-byte-rate",
    "producer_network-io-total",
    "producer_metadata-age",
    "producer_iotime-total",
    "producer_io-waittime-total",
    "producer_io-time-ns-avg",
    "producer_incoming-byte-total",
    "producer_connection-creation-total",
    "producer_connection-creation-rate",
    "producer_connection-count",
    "producer_connection-close-total",
    "producer_bufferpool-wait-time-total",
    "producer_batch-split-total",
    "producer_batch-size-max",
    "latency_999th_ms",
    "latency_99th_ms",
    "latency_95th_ms",
    "producer-topic_record-error-rate",
    "producer-topic_record-error-total",
    "producer_select-rate",
    "producer_request-latency-max",
    "producer_records-per-request-avg",
    "producer_record-retry-rate",
    "producer_produce-throttle-time-max",
    "producer_produce-throttle-time-avg",
    "producer_network-io-rate",
    "producer_io-wait-time-ns-avg",
    "producer_io-ratio",
    "producer_io-wait-ratio",
    "producer_incoming-byte-rate",
    "producer_connection-close-rate",
    "producer_compression-rate-avg",
    "producer_bufferpool-wait-ratio",
    "producer_buffer-total-bytes",
    "producer_buffer-exhausted-total",
    "producer_buffer-exhausted-rate",
    "producer_buffer-available-bytes",
    "latency_50th_ms",
    "latency_max_ms",
    "records_persec",
    "producer_batch-split-rate",
    "producer_waiting-threads",
    "producer_requests-in-flight",
    "producer_request-latency-avg",
    "producer_record-queue-time-avg",

    "producer_batch-size-avg",
    "producer_record-queue-time-avg",
    "time_ms",
]


def delete_keys(m, keys):
    for key in keys:
        if key in m:
            del m[key]
    return m


def try_float(s):
    try:
        f = float(s)
        return f
    except:
        return s


def parse_header(metric, line):
    match = re.match(r'(\d+),(\d+),([\d\-]+),"(\w+)",(\d+),(\d+),(\d+),(\d+)', line)
    if not match:
        raise Exception(f"header not matched: {line}")

    metric['records'] = try_float(match.group(1))
    metric['size_b'] = try_float(match.group(2))
    metric['acks'] = match.group(3)
    metric['compression'] = match.group(4)
    metric['inflight'] = try_float(match.group(5))
    metric['batch_b'] = try_float(match.group(6))
    metric['linger_ms'] = try_float(match.group(7))
    metric['time_ms'] = try_float(match.group(8))

    return metric


def parse_metric_result(metric, line):
    match = re.match(r'\d+ records sent, ([\d.]+) records/sec \(([\d.]+) MB/sec\), ([\d.]+) ms avg latency, ([\d.]+) ms max latency, (\d+) ms 50th, (\d+) ms 95th, (\d+) ms 99th, (\d+) ms 99.9th.', line)
    if not match:
        raise Exception(f"metric result not matched: {line}")

    metric['records_persec'] = try_float(match.group(1))
    metric['mb_persec'] = try_float(match.group(2))
    metric['latency_avg_ms'] = try_float(match.group(3))
    metric['latency_max_ms'] = try_float(match.group(4))
    metric['latency_50th_ms'] = try_float(match.group(5))
    metric['latency_95th_ms'] = try_float(match.group(6))
    metric['latency_99th_ms'] = try_float(match.group(7))
    metric['latency_999th_ms'] = try_float(match.group(8))

    return metric


def parse_metric(metric, line):
    if re.match(r'producer-node-metrics.*', line):
        match = re.match(r'producer-node-metrics:([\w\-]+):\{client-id=test, node-id=node-([\d\-]+)\}\s+: (.*)',line)
        if not match:
            raise Exception(f'producer node metrics not matched: {line}')

        metric[f"producer-node_{match.group(1)}_{match.group(2)}"] = try_float(match.group(3))
    elif re.match(r'producer-topic-metrics.*', line):
        match = re.match(r'producer-topic-metrics:([\w\-]+):\{client-id=test, topic=test-([\w\-]+)\}\s+: (.*)', line)
        if not match:
            raise Exception(f'producer topic metrics not matched: {line}')

        metric[f"producer-topic_{match.group(1)}"] = try_float(match.group(3))
        metric["topic"] = match.group(2)
    elif re.match(r'[\w\-]+-metrics.*', line):
        match = re.match(r'([\w\-]+)-metrics:([\w\-]+):\{.*\}\s+: (.*)',line)
        if not match:
            raise Exception(f'producer metrics not matched: {line}')

        metric[f"{match.group(1)}_{match.group(2)}"] = try_float(match.group(3))
    elif line == "":
        return metric
    else:
        raise Exception(f"line not matched by any pattern: {line}")
    return metric


def process_files(files):
    metrics = []
    metric = {}
    for file in files:
        with open(file, 'r+') as f:
            for line in f:
                line = line.strip()

                if re.match(r'.*,"snappy",.*', line) or re.match(r'.*,"none",.*', line) or re.match(r'.*,"gzip",.*', line):
                    # if 'producer_batch-size-avg' in metric and 'batch_b' in metric and 'mb_persec' in metric and metric['batch_b'] != 0:
                    #     metric['batch_ratio'] = float(metric['producer_batch-size-avg']) / float(metric['batch_b'])
                    # else:
                    #     metric['batch_ratio'] = 0
                    delete_keys(metric, exclude_keys)
                    metrics.append(metric)
                    metric = {}
                    metric = parse_header(metric, line)
                elif re.match(r'.*records sent.*', line):
                    metric = parse_metric_result(metric, line)
                else:
                    metric = parse_metric(metric, line)
    return metrics


def find_correlation(metrics):
    df = pd.DataFrame(metrics)

    print(); print(df)

    corr_matrix = df.corr().abs()
    print(); print(corr_matrix)

    pd.set_option("display.max_rows", None, "display.max_columns", None)

    upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape),
                                      k=1).astype(np.bool))
    print(); print(upper)

    to_drop = [column for column in reversed(upper.columns) if any(upper[column] > 0.9)]
    print(); print(to_drop)
    print(len(to_drop))

    for column in to_drop:
        print(f'"{column}",')

    # df1 = df.drop(df.columns[to_drop], axis=1)
    # print(); print(df1)


def main():
    metrics = process_files(['metrics-1.data', 'metrics-2.data'])
    metrics.pop(0)

    with open('metrics.json', 'w') as f:
        json.dump(metrics, f)

    with open('metrics.csv', 'w') as f:
        writer = csv.DictWriter(f, metrics[1].keys())
        writer.writeheader()
        for metric in metrics:
            writer.writerow(metric)

    find_correlation(metrics)


if __name__ == '__main__':
    main()