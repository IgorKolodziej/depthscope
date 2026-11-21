"""Capture acquisition, validation and storage evidence for the Data milestone.

Run after the Compose stack has collected at least two minutes of data.
"""

import ast
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / 'milestones/data/evidence'


def command(*arguments, check=True):
    result = subprocess.run(arguments, cwd=ROOT, capture_output=True, text=True, timeout=180)
    if check and result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result


def query(sql):
    code = f'from depthscope.storage import query; print(query({sql!r}).decode(), end="")'
    output = command('docker', 'compose', 'exec', '-T', 'processor', 'python', '-c', code).stdout
    return [json.loads(line) for line in output.splitlines()]


def capture():
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).isoformat()
    for name, arguments in (
        ('unit-tests', ['make', 'test']),
        ('compose-config', ['docker', 'compose', 'config', '--quiet']),
        ('lint', ['ruff', 'check', 'depthscope', 'tests', 'scripts']),
        ('fee-loader', [sys.executable, '-m', 'depthscope', 'fees', '--path', 'data/fees.csv',
                        '--at', '2026-11-21T12:00:00Z']),
    ):
        result = command(*arguments, check=False)
        (EVIDENCE / f'{name}.txt').write_text(f'UTC {stamp}\n$ {" ".join(arguments)}\n' + result.stdout + result.stderr)
        if result.returncode:
            raise RuntimeError(f'{name} failed; see evidence')

    schema_code = """
import json
import time
from depthscope import storage
original = storage.query
table = 'data_schema_check_' + str(time.time_ns())
def temporary_query(sql, data=None, parameters=None):
    return original(sql.replace('observations', table), data, parameters)
storage.query = temporary_query
try:
    storage.initialize()
    storage.insert('schema-check', 'frames', [dict(second=1, venue='coinbase', asset='BTC', status='valid')])
    columns = [json.loads(line)['name'] for line in original('DESCRIBE TABLE ' + table + ' FORMAT JSONEachRow').splitlines()]
    assert columns == ['run', 'kind', 'second', 'venue', 'asset', 'payload'], columns
    count = int(original('SELECT count() FROM ' + table + ' FINAL FORMAT TSV').strip())
    assert count == 1
    print(json.dumps(dict(status='PASS', columns=columns, inserted_rows=count)))
finally:
    original('DROP TABLE IF EXISTS ' + table, b'')
"""
    result = command('docker', 'compose', 'exec', '-T', 'processor', 'python', '-c', schema_code)
    (EVIDENCE / 'schema-check.json').write_text(result.stdout)

    bounds = query("SELECT min(second) AS first, max(second) AS last FROM observations FINAL "
                   "WHERE run='live' AND kind='frames' AND NOT JSONHas(payload,'features') FORMAT JSONEachRow")[0]
    end = int(bounds['last']) - 15
    start = end - 119
    if start < int(bounds['first']):
        raise RuntimeError('Collect at least 135 seconds before capturing live evidence')
    raw = query(f"""
        SELECT venue, countIf(kind='message') AS messages, countIf(kind='connected') AS connects,
               countIf(kind='disconnected') AS disconnects, sum(length(payload)) AS payload_bytes,
               countIf(kind='tick') AS ticks
        FROM raw_events FINAL
        WHERE receipt_ns >= {start}*1000000000 AND receipt_ns < {end+1}*1000000000
        GROUP BY venue ORDER BY venue FORMAT JSONEachRow
    """)
    frames = query(f"""
        SELECT venue, asset, count() AS frames, uniqExact(second) AS unique_seconds,
            countIf(JSONExtractString(payload,'status')='valid') AS valid,
            countIf(JSONHas(payload,'features') OR JSONHas(payload,'buy_usd') OR JSONHas(payload,'mid')) AS analytics_fields
        FROM observations FINAL WHERE run='live' AND kind='frames'
          AND second BETWEEN {start} AND {end}
        GROUP BY venue,asset ORDER BY venue,asset FORMAT JSONEachRow
    """)
    assert len(frames) == 4 and all(int(row['unique_seconds']) == 120 for row in frames), frames
    assert all(int(row['valid']) > 0 and int(row['analytics_fields']) == 0 for row in frames), frames
    assert {row['venue'] for row in raw if int(row['messages']) > 0} == {'coinbase', 'kraken'}, raw
    def iso(second):
        return datetime.fromtimestamp(second, timezone.utc).isoformat()
    log = command('docker', 'compose', 'logs', '--no-color', '--since', iso(start), '--until', iso(end+1), 'collector').stdout
    (EVIDENCE / 'collector.txt').write_text(log)
    lags, rates = [], []
    pattern = re.compile(r'messages/s=(\{.*?\}) bytes/s=(\{.*?\}) pipeline lag \(s\)=(\{.*?\})')
    for line in log.splitlines():
        if match := pattern.search(line):
            rates.append(ast.literal_eval(match[1]))
            lags.append(ast.literal_eval(match[3]))
    assert rates and lags, 'missing rate samples'
    summary = dict(verified_at=stamp, interval_start=iso(start), interval_end=iso(end), seconds=120,
                   raw=raw, frames=frames,
                   max_combined_messages_per_second=max(sum(row.values()) for row in rates),
                   max_reported_lag_seconds={stage:max(row[stage] for row in lags) for stage in lags[0]})
    (EVIDENCE / 'live-summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    (EVIDENCE / 'compose-status.txt').write_text(command('docker', 'compose', 'ps', '-a').stdout)
    (EVIDENCE / 'test-environment.txt').write_text(command('python3', '--version').stdout +
                                                command('docker', 'compose', 'version').stdout)
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    capture()
