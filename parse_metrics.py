import json
import re


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
    elif re.match(r'[\w\-]+-metrics.*', line):
        match = re.match(r'([\w\-]+)-metrics:([\w\-]+):\{.*\}\s+: (.*)',line)
        if not match:
            raise Exception(f'producer node metrics not matched: {line}')

        metric[f"{match.group(1)}_{match.group(2)}"] = try_float(match.group(3))
    return metric


def process_files(files):
    metrics = []
    metric = {}
    for file in files:
        with open(file, 'r+') as f:
            for line in f:
                line = line.strip()

                if re.match(r'.*,"snappy",.*', line) or re.match(r'.*,"none",.*', line):
                    delete_keys(metric, exclude_keys)
                    metrics.append(metric)
                    metric = {}
                    metric = parse_header(metric, line)
                elif re.match(r'.*records sent.*', line):
                    metric = parse_metric_result(metric, line)
                else:
                    metric = parse_metric(metric, line)
    return metrics


def main():
    metrics = process_files(['metrics-raw-500', 'metrics-raw-wo-timeout', 'metrics-raw-fix-timeout'])
    
    print(json.dumps(metrics).replace("NaN" , "null"))


if __name__ == '__main__':
    main()