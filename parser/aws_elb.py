import re
import pandas as pd

# https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-entry-format
def parse_log(file_path):
    pattern = re.compile(
        r'(?P<method>\S+)\s+'                          # Capture method/type (first field)
        r'(?P<timestamp>\S+)\s+'                       # Capture timestamp
        r'\S+\s+'                                      # Skip ELB name
        r'(?P<client>[\d\.]+:\d+)\s+'                  # Capture client IP:port
        r'(?P<target>[\d\.]+:\d+)\s+'                  # Capture target IP:port
        r'\S+\s+'                                      # Skip request_processing_time
        r'\S+\s+'                                      # Skip backend_processing_time
        r'\S+\s+'                                      # Skip response_processing_time
        r'\S+\s+'                                      # Skip elb_status_code
        r'\S+\s+'                                      # Skip target_status_code
        r'(?P<received_bytes>\d+)\s+'                  # Capture received_bytes
        r'(?P<sent_bytes>\d+)'                         # Capture sent_bytes
    )

    records = []
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            match = pattern.search(line)
            if match:
                record = match.groupdict()
                record['received_bytes'] = int(record['received_bytes'])
                record['sent_bytes'] = int(record['sent_bytes'])
                records.append(record)

    return pd.DataFrame(records)

def format_bytes(num_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if num_bytes < 1024:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.2f} PB"

file_path = 'tmp/merged_log_file.log'
df = parse_log(file_path)

summary = df.groupby('client').agg({
    'received_bytes': 'sum',
    'sent_bytes': 'sum'
}).reset_index()

summary['received_bytes'] = summary['received_bytes'].apply(format_bytes)
summary['sent_bytes'] = summary['sent_bytes'].apply(format_bytes)

print(summary)

total_received = df['received_bytes'].sum()
total_sent = df['sent_bytes'].sum()

total_received_fmt = format_bytes(total_received)
total_sent_fmt = format_bytes(total_sent)

print("\nSummary:")
print(f"Total Received Bytes: {total_received_fmt}")
print(f"Total Sent Bytes: {total_sent_fmt}")
