# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

EXE = str(Path(sys.argv[1]).resolve())
ROOT = Path(os.environ['RUNNER_TEMP']) / 'issue10455'
OUT = Path('evidence').resolve()
OUT.mkdir(exist_ok=True)
ROOT.mkdir(exist_ok=True)
COPY = ROOT / 'source' / 'copy'
LOG = OUT / 'sql.ndjson'
RESULTS = []
PROCESSES = []
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')


def command(label, args):
    run = subprocess.run(args, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=180)
    result = {'args': args, 'exit_code': run.returncode, 'stdout': run.stdout, 'stderr': run.stderr}
    write_json(OUT / (label + '.json'), result)
    print(label, run.returncode, run.stdout[-1500:], run.stderr[-500:], flush=True)
    return result


def sql(statement, port=18455, required=True):
    req = urllib.request.Request(f'http://127.0.0.1:{port}/v1/sql', data=urllib.parse.urlencode({'sql': statement}).encode())
    try:
        with HTTP.open(req, timeout=60) as response:
            result = json.load(response)
    except urllib.error.HTTPError as error:
        result = json.loads(error.read())
    with LOG.open('a', encoding='utf-8') as log:
        log.write(json.dumps({'port': port, 'sql': statement, 'result': result}) + '\n')
    if required and result.get('code', 0):
        raise RuntimeError(f'{statement}: {result}')
    return result


def rows(result):
    if result.get('code', 0):
        return result
    return result['output'][0].get('records', {}).get('rows', [])


def start(name, port):
    data = ROOT / name
    config = ROOT / (name + '.toml')
    config.write_text(f'''enable_telemetry = false
[http]
addr = "127.0.0.1:{port}"
[grpc]
bind_addr = "127.0.0.1:{port + 100}"
[mysql]
enable = false
[postgres]
enable = false
[storage]
data_home = "{data.as_posix()}"
copy_root = "{COPY.as_posix()}"
''', encoding='utf-8')
    with (OUT / (name + '.stdout.log')).open('w') as stdout, (OUT / (name + '.stderr.log')).open('w') as stderr:
        process = subprocess.Popen([EXE, 'standalone', 'start', '-c', str(config), '--log-dir', str(OUT / (name + '-logs')), '--log-level', 'warn'], stdout=stdout, stderr=stderr)
    PROCESSES.append(process)
    for _ in range(120):
        if process.poll() is not None:
            raise RuntimeError(f'{name} exited {process.returncode}')
        try:
            sql('SELECT 1', port)
            return
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(1)
    raise RuntimeError(f'{name} startup timed out')


def create(schema, tables, port=18455, insert=True):
    sql(f'CREATE DATABASE {schema}', port)
    for name, host, count in tables:
        sql(f'CREATE TABLE {schema}."{name}" (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host))', port)
        if insert:
            values = ','.join(f"('{host}',{i + 1},{i * 1000})" for i in range(count))
            sql(f'INSERT INTO {schema}."{name}" VALUES {values}', port)


def content(schema, tables, port=18455):
    return {name: rows(sql(f'SELECT host,val,ts FROM {schema}."{name}" ORDER BY host,val,ts', port, False)) for name, _, _ in tables}


def inventory(label, directory, names):
    directory = Path(directory)
    result = {'directory': str(directory), 'exists': directory.exists(), 'entries': [], 'requested_paths': {}}
    if directory.exists():
        command(label + '-dir', ['cmd', '/c', 'dir', '/a', '/r', '/s', str(directory)])
        escaped = str(directory).replace("'", "''")
        command(label + '-streams', ['pwsh', '-NoProfile', '-Command', f"$p='{escaped}'; @((Get-Item -LiteralPath $p -Stream * -ErrorAction Continue); (Get-ChildItem -LiteralPath $p -Recurse -Force | Get-Item -Stream * -ErrorAction Continue)) | Select-Object FileName,Stream,Length | ConvertTo-Json -Depth 4"])
        for path in sorted(directory.rglob('*')):
            if path.is_file():
                payload = path.read_bytes()
                result['entries'].append({'path': str(path.relative_to(directory)), 'bytes': len(payload), 'sha256': hashlib.sha256(payload).hexdigest()})
        for index, name in enumerate(names):
            if name.lower() in {'con', 'nul', 'aux', 'prn'} | {f'{prefix}{n}' for prefix in ('com', 'lpt') for n in range(10)}:
                continue
            path = Path(str(directory) + os.sep + name + '.parquet')
            try:
                payload = path.read_bytes()
                result['requested_paths'][name] = {'bytes': len(payload), 'sha256': hashlib.sha256(payload).hexdigest()}
                (OUT / f'{label}-requested-{index}.bin').write_bytes(payload)
            except OSError as error:
                result['requested_paths'][name] = {'error': str(error)}
    write_json(OUT / (label + '-inventory.json'), result)
    return result


def roundtrip(label, tables, options='parallelism=1'):
    create(label, tables)
    create(label + '_dst', tables, insert=False)
    before = content(label, tables)
    export = sql(f"COPY DATABASE {label} TO '{label}/' WITH (FORMAT='parquet'{',' + options if options else ''})", required=False)
    inv = inventory(label, COPY / label, [t[0] for t in tables])
    restored = sql(f"COPY DATABASE {label}_dst FROM '{label}/' WITH (FORMAT='parquet')", required=False)
    after = content(label + '_dst', tables)
    result = {'case': label, 'export': export, 'import': restored, 'before': before, 'after': after, 'roundtrip_equal': before == after, 'inventory': inv}
    RESULTS.append(result)
    write_json(OUT / 'results.json', RESULTS)
    print(json.dumps(result, ensure_ascii=False), flush=True)


def varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def field(number, value):
    return varint(number * 8 + 2) + varint(len(value)) + value


def prometheus():
    sql('CREATE DATABASE promcase')
    body = b''
    for metric in ['up', 'cluster:cpu_ratio', 'job:http_requests:rate5m']:
        label = field(1, b'__name__') + field(2, metric.encode())
        sample = b'\x09' + struct.pack('<d', 1.0) + b'\x10' + varint(1000)
        body += field(1, field(1, label) + field(2, sample))
    n = len(body) - 1
    length_bytes = max(1, (n.bit_length() + 7) // 8)
    literal = bytes([n << 2]) if n < 60 else bytes([(59 + length_bytes) << 2]) + n.to_bytes(length_bytes, 'little')
    compressed = varint(len(body)) + literal + body
    req = urllib.request.Request('http://127.0.0.1:18455/v1/prometheus/write?db=promcase', data=compressed, headers={'Content-Type': 'application/x-protobuf', 'Content-Encoding': 'snappy', 'X-Prometheus-Remote-Write-Version': '0.1.0'})
    try:
        with HTTP.open(req, timeout=60) as response:
            ingest = {'status': response.status, 'body': response.read().decode()}
    except urllib.error.HTTPError as error:
        ingest = {'status': error.code, 'body': error.read().decode()}
    result = {'case': 'prometheus', 'ingest': ingest, 'tables': sql('SHOW TABLES FROM promcase', required=False), 'export': sql("COPY DATABASE promcase TO 'promcase/' WITH (FORMAT='parquet',parallelism=1)", required=False)}
    RESULTS.append(result)
    inventory('promcase', COPY / 'promcase', ['up', 'cluster:cpu_ratio', 'job:http_requests:rate5m'])


def cli_cases(tables):
    for version, port in [('v1', 18456), ('v2', 18457)]:
        schema = 'cli' + version
        create(schema, tables)
        before = content(schema, tables)
        directory = COPY / schema
        common = ['--log-dir', str(OUT / 'cli-logs'), '--log-level', 'warn']
        if version == 'v1':
            export = command('cli-v1-export', [EXE, 'cli', 'data', 'export', '--addr', '127.0.0.1:18455', '--database', 'greptime-' + schema, '--output-dir', str(directory), '--table-parallelism', '1', '--no-proxy'] + common)
        else:
            export = command('cli-v2-export', [EXE, 'cli', 'data', 'export-v2', 'create', '--addr', '127.0.0.1:18455', '--schemas', schema, '--to', directory.as_uri(), '--parallelism', '1', '--no-proxy', '--progress', 'never'] + common)
        inv = inventory('cli-' + version, directory, [])
        result = {'case': 'cli_' + version, 'export_exit': export['exit_code'], 'before': before, 'inventory': inv}
        if version == 'v2':
            verify = command('cli-v2-verify', [EXE, 'cli', 'data', 'export-v2', 'verify', '--snapshot', directory.as_uri()] + common)
            result['verify_exit'] = verify['exit_code']
            result['verify_stdout'] = verify['stdout']
            manifest = directory / 'manifest.json'
            if manifest.exists():
                result['manifest'] = json.loads(manifest.read_text())
        start('restore-' + version, port)
        if version == 'v1':
            imported = command('cli-v1-import', [EXE, 'cli', 'data', 'import', '--addr', f'127.0.0.1:{port}', '--database', 'greptime-' + schema, '--input-dir', str(directory), '--no-proxy'] + common)
        else:
            imported = command('cli-v2-import', [EXE, 'cli', 'data', 'import-v2', '--addr', f'127.0.0.1:{port}', '--from', directory.as_uri(), '--no-proxy', '--progress', 'never', '--state-path', str(ROOT / 'import-state.json')] + common)
        result['import_exit'] = imported['exit_code']
        result['after'] = content(schema, tables, port)
        result['roundtrip_equal'] = result['before'] == result['after']
        RESULTS.append(result)


try:
    command('version', [EXE, '--version'])
    command('environment', ['pwsh', '-NoProfile', '-Command', 'Get-CimInstance Win32_OperatingSystem | Select-Object Caption,Version,OSArchitecture | ConvertTo-Json; Get-Volume | Select-Object DriveLetter,FileSystem,FileSystemLabel | ConvertTo-Json'])
    start('source', 18455)
    tables = [('job:rate5m', 'job', 2), ('Events', 'Upper', 2), ('events', 'lower', 3)]
    roundtrip('control', [('up', 'up', 2), ('other', 'other', 3)])
    assert RESULTS[-1]['roundtrip_equal'], 'safe-name control failed'
    roundtrip('serial', tables)
    for index in range(10):
        roundtrip(f'parallel{index}', tables, '')
    for name in ['job:http_requests:rate5m', 'con', 'nul', 'aux', 'prn'] + [f'com{i}' for i in range(10)] + [f'lpt{i}' for i in range(10)] + ['a:b', ':leading']:
        label = 'special' + str(len(RESULTS))
        roundtrip(label, [(name, 'special', 2)])
    roundtrip('mixedinvalid', [('aaa_valid', 'valid', 2), ('job:http_requests:rate5m', 'invalid', 2), ('zzz_valid', 'valid2', 3)], '')
    roundtrip('preparedcase', [('Events', 'Upper', 2), ('events', 'lower', 3)], "parallelism=1,experimental_metric_export='true'")
    roundtrip('preparedcolon', [('job:rate5m', 'job', 2)], "parallelism=1,experimental_metric_export='true'")
    prometheus()
    cli_cases([('up', 'up', 1)] + tables)
    inventory('copy-root', COPY, ['b'])
finally:
    write_json(OUT / 'results.json', RESULTS)
    for process in reversed(PROCESSES):
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    summary = ['# Issue 10455 Windows observations', '', '| Case | Export | Import | Roundtrip matches |', '|---|---|---|---|']
    for result in RESULTS:
        export = result.get('export', {})
        imported = result.get('import', {})
        summary.append(f"| {result['case']} | {export.get('error', export.get('output', result.get('export_exit', 'n/a')))} | {imported.get('error', imported.get('output', result.get('import_exit', 'n/a')))} | {result.get('roundtrip_equal', 'n/a')} |")
    text = '\n'.join(summary) + '\n'
    (OUT / 'summary.md').write_text(text, encoding='utf-8')
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a', encoding='utf-8') as output:
            output.write(text)
