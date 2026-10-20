#!/usr/bin/env python3
"""Reconcile corpus-only Forgejo push hooks; secrets arrive on stdin, never argv.

Run as root inside the verified Forgejo guest. Input is exactly an object with
corpus (active gateway configuration) and webhook_secret (existing gateway HMAC
secret). This uses supported Forgejo APIs and a dedicated persistent scoped PAT.
SQLite is opened read-only to verify configuration and delivery receipts.
With --preflight, stdin requires only corpus; an existing PAT is used for GET
requests without creating credentials, locks, receipts, hooks or configuration.
"""
import argparse
import configparser
from contextlib import closing
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

CONFIG = Path('/etc/forgejo/app.ini')
DATABASE = Path('/var/lib/forgejo/data/forgejo.db')
TOKEN = Path('/etc/forgejo/retrieval-hooks.token')
TOKEN_NAME = 'retrieval-corpus-webhooks'
BACKUPS = Path('/var/backups/forgejo-retrieval-hooks')  # pre-change app.ini copies, not Forgejo data
RECEIPT = Path('/var/tmp/retrieval-hook-setup/acceptance.json')  # consumed and removed by the MGR installer
HOST = 'duckdb-fidelity-pgq.spkez.dev'
ENDPOINT = 'https://' + HOST + '/v1/index/events/forgejo'
API_BASE = 'http://127.0.0.1:3000/api/v1'
MAX_INPUT = 256 * 1024


class HookError(RuntimeError):
    """A sanitized error safe to report without request/configuration contents."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, new_url):
        raise HookError('Unexpected Forgejo API redirect')


def opener():
    return urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())


def command(*args, timeout=90):
    result = subprocess.run(args, capture_output=True, timeout=timeout)
    if result.returncode:
        raise HookError('Forgejo administration command failed')
    return result.stdout


def connect():
    connection = sqlite3.connect('file:' + str(DATABASE) + '?mode=ro', uri=True, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA query_only=ON')
    return connection


def validate_input(value, *, preflight=False):
    expected = {'corpus'} if preflight else {'corpus', 'webhook_secret'}
    if not isinstance(value, dict) or set(value) != expected:
        raise HookError('Input fields differ from the selected operation')
    secret, corpus = value.get('webhook_secret'), value['corpus']
    if not preflight and (not isinstance(secret, str) or not 32 <= len(secret) <= 256
            or any(ord(c) < 33 or ord(c) > 126 for c in secret)):
        raise HookError('Webhook secret has invalid format')
    if (not isinstance(corpus, dict) or set(corpus) != {'schema_version', 'sources'}
            or type(corpus['schema_version']) is not int or corpus['schema_version'] != 1
            or not isinstance(corpus['sources'], list) or not 1 <= len(corpus['sources']) <= 64):
        raise HookError('Unsupported corpus configuration')
    sources, identifiers, repos = [], set(), set()
    for item in corpus['sources']:
        if not isinstance(item, dict):
            raise HookError('Invalid corpus source')
        source = {key: item.get(key) for key in ('id', 'repo', 'ref')}
        patterns = {'id': r'[a-z][a-z0-9_-]{0,47}',
                    'repo': r'FidelityFramework/[A-Za-z0-9_.-]{1,80}',
                    'ref': r'refs/heads/[A-Za-z0-9][A-Za-z0-9_./-]{0,100}'}
        if any(not isinstance(source[k], str) or not re.fullmatch(pattern, source[k])
               for k, pattern in patterns.items()):
            raise HookError('Invalid repository or explicit branch identity')
        if (source['id'] in identifiers or source['repo'].casefold() in repos or '..' in source['ref']
                or '//' in source['ref'] or source['ref'].endswith('/')):
            raise HookError('Repeated source or invalid explicit branch')
        identifiers.add(source['id'])
        repos.add(source['repo'].casefold())
        sources.append(source)
    return sorted(sources, key=lambda source: source['id']), secret


def allowlist_text(text):
    """Change one INI value without reserializing unrelated settings/secrets."""
    lines = text.splitlines(keepends=True)
    sections = [(i, match.group(1).strip().lower()) for i, line in enumerate(lines)
                if (match := re.match(r'^\s*\[([^]]+)\]\s*(?:[;#].*)?$', line.rstrip('\r\n')))]
    positions = [i for i, name in sections if name == 'webhook']
    if len(positions) > 1:
        raise HookError('Multiple webhook configuration sections')
    if not positions:
        return text.rstrip('\r\n') + '\n\n[webhook]\nALLOWED_HOST_LIST = external,' + HOST + '\n'
    start = positions[0]
    end = next((i for i, _ in sections if i > start), len(lines))
    values = [(i, match) for i in range(start + 1, end)
              if (match := re.match(r'^(\s*ALLOWED_HOST_LIST\s*=\s*)(.*?)(\r?\n)?$', lines[i], re.I))]
    if len(values) > 1:
        raise HookError('Multiple webhook allowlist values')
    if values:
        i, match = values[0]
        pieces = re.split(r'(\s+[;#].*)', match.group(2), maxsplit=1)
        current = pieces[0].strip()
        hosts = [part.strip() for part in current.split(',') if part.strip()]
        if HOST in {host.lower() for host in hosts}:
            return text
        updated = ','.join([*hosts, HOST])
        lines[i] = match.group(1) + updated + ''.join(pieces[1:]) + (match.group(3) or '\n')
    else:
        if end and not lines[end - 1].endswith(('\n', '\r')):
            lines[end - 1] += '\n'
        lines.insert(end, 'ALLOWED_HOST_LIST = external,' + HOST + '\n')
    return ''.join(lines)


def atomic_write(path, data, uid, gid, mode):
    descriptor, name = tempfile.mkstemp(prefix='.' + path.name + '-', dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            os.fchown(stream.fileno(), uid, gid)
            os.fchmod(stream.fileno(), mode)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def health():
    deadline = time.monotonic() + 45
    last = 'no response'
    while True:
        try:
            with opener().open(API_BASE + '/version', timeout=5) as response:
                version = json.loads(response.read(4096))['version']
            if version == '15.0.9+gitea-1.22.0':
                return
            last = 'version ' + str(version)
        except (OSError, ValueError, KeyError) as error:
            last = type(error).__name__ + ': ' + str(getattr(error, 'reason', error))
        if time.monotonic() >= deadline:
            raise HookError('Forgejo15.0.9 health check failed (last: ' + last[:300] + ')')
        time.sleep(1)


def enable_destination():
    info = CONFIG.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o022:
        raise HookError('Unsafe Forgejo configuration file')
    original = CONFIG.read_bytes()
    updated = allowlist_text(original.decode()).encode()
    if updated == original:
        return None
    BACKUPS.mkdir(mode=0o700, parents=False, exist_ok=True)
    backup_info = BACKUPS.lstat()
    if not stat.S_ISDIR(backup_info.st_mode) or backup_info.st_uid != 0 or stat.S_IMODE(backup_info.st_mode) != 0o700:
        raise HookError('Unsafe configuration backup directory')
    backup = BACKUPS / ('app.ini.before-' + time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()) + '-' + uuid.uuid4().hex[:8])
    with backup.open('xb') as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(original)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        atomic_write(CONFIG, updated, info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode))
        command('systemctl', 'restart', 'forgejo.service')
        health()
    except BaseException:
        atomic_write(CONFIG, original, info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode))
        command('systemctl', 'restart', 'forgejo.service')
        health()
        raise
    return str(backup)


def credential(*, create=True):
    with closing(connect()) as db:
        records = db.execute('SELECT t.uid,t.scope FROM access_token t JOIN user u ON u.id=t.uid '
                             'WHERE t.name=? AND u.name=?', (TOKEN_NAME, 'hhh')).fetchall()
    if TOKEN.exists() or TOKEN.is_symlink():
        info = TOKEN.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_size > 128 or len(records) != 1 or records[0]['scope'] != 'write:repository'):
            raise HookError('Dedicated credential file or token metadata differs')
        token = TOKEN.read_text().strip()
        created = False
    else:
        if not create:
            raise HookError('Read-only preflight requires the existing dedicated credential file')
        if records:
            raise HookError('Dedicated token name already exists without its private credential file')
        token = command('runuser', '-u', 'hhh', '--', '/usr/local/bin/forgejo', 'admin', 'user',
                        'generate-access-token', '--config', str(CONFIG), '--work-path', '/var/lib/forgejo',
                        '--username', 'hhh', '--token-name', TOKEN_NAME,
                        '--scopes', 'write:repository', '--raw').decode().strip()
        if not re.fullmatch(r'[0-9a-f]{40}', token):
            raise HookError('Token generator returned an unexpected format; inspect dedicated token metadata')
        # Publish only complete durable bytes, without replacing an existing
        # credential even if another operator creates it during this command.
        descriptor, name = tempfile.mkstemp(prefix='.retrieval-hooks-', dir=TOKEN.parent)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, 'w') as stream:
                os.fchmod(stream.fileno(), 0o600)
                stream.write(token + '\n')
                stream.flush()
                os.fsync(stream.fileno())
            os.link(temporary, TOKEN)
            directory = os.open(TOKEN.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            temporary.unlink(missing_ok=True)
        created = True
    if not re.fullmatch(r'[0-9a-f]{40}', token):
        raise HookError('Dedicated credential file has invalid format')
    return token, created


class API:
    def __init__(self, token):
        self.token = token
        self.client = opener()

    def call(self, method, path, data=None, expected=200):
        # Forgejo's API stalled past 20 s on a plain GET during the October 16, 2026
        # activation; reads are safe to repeat once, writes never are (a second
        # POST could create a second hook), so only GET retries here.
        for attempt in (1, 2):
            try:
                return self.once(method, path, data, expected)
            except HookError as error:
                if method != 'GET' or attempt == 2 or 'TimeoutError' not in str(error):
                    raise
                time.sleep(2)

    def once(self, method, path, data=None, expected=200):
        headers = {'Authorization': 'token ' + self.token, 'Content-Type': 'application/json'}
        body = json.dumps(data, separators=(',', ':')).encode() if data is not None else None
        request = urllib.request.Request(API_BASE + path, data=body, headers=headers, method=method)
        try:
            with self.client.open(request, timeout=20) as response:
                raw = response.read(1024 * 1024 + 1)
                if response.status != expected or len(raw) > 1024 * 1024:
                    raise HookError('Forgejo API status or response bound differs')
        except urllib.error.HTTPError as error:
            raise HookError('Forgejo API rejected operation with HTTP ' + str(error.code) + ' for ' + method + ' ' + path) from None
        except OSError as error:
            # Name the failing request and the socket-level reason; never the token or body.
            reason = getattr(error, 'reason', None)
            detail = type(reason).__name__ + ': ' + str(reason) if reason is not None else type(error).__name__ + ': ' + str(error)
            raise HookError('Forgejo API connection failed for ' + method + ' ' + path + ' (' + detail[:300] + ')') from None
        return json.loads(raw) if raw else None

    def hooks(self, repo):
        result = []
        for page in range(1, 21):
            batch = self.call('GET', '/repos/' + repo + '/hooks?limit=50&page=' + str(page))
            if not isinstance(batch, list):
                raise HookError('Invalid repository hook inventory')
            result.extend(batch)
            if len(batch) < 50:
                return result
        raise HookError('Repository hook inventory exceeds bound')


def hook_url(hook):
    return hook.get('url') or hook.get('config', {}).get('url')


def matching_hook(hook, source):
    return (hook_url(hook) == ENDPOINT and hook.get('type') == 'forgejo' and hook.get('active') is True
            and hook.get('events') == ['push'] and hook.get('branch_filter') == source['ref'][len('refs/heads/'):]
            and (hook.get('content_type') or hook.get('config', {}).get('content_type')) == 'json'
            and not hook.get('authorization_header'))


def upsert(api, source, secret):
    matches = [hook for hook in api.hooks(source['repo']) if hook_url(hook) == ENDPOINT]
    if len(matches) > 1:
        raise HookError('Multiple hooks already target the corpus endpoint for ' + source['repo'])
    data = {'type': 'forgejo', 'active': True, 'events': ['push'], 'authorization_header': '',
            'branch_filter': source['ref'][len('refs/heads/'):],
            'config': {'url': ENDPOINT, 'content_type': 'json', 'secret': secret}}
    base = '/repos/' + source['repo'] + '/hooks'
    if matches:
        hook = matches[0]
        if hook.get('type') != 'forgejo' or type(hook.get('id')) is not int:
            raise HookError('Existing corpus endpoint hook has an unexpected type')
        with closing(connect()) as db:
            row = db.execute('SELECT secret FROM webhook WHERE id=? AND url=?', (hook['id'], ENDPOINT)).fetchone()
        if row is None:
            raise HookError('Hook inventory changed during reconciliation')
        if matching_hook(hook, source) and hmac.compare_digest(row['secret'], secret):
            change = 'unchanged'
        else:
            api.call('PATCH', base + '/' + str(hook['id']), {k:v for k,v in data.items() if k != 'type'})
            change = 'updated'
    else:
        hook = api.call('POST', base, data, expected=201)
        change = 'created'
    hook = api.call('GET', base + '/' + str(hook['id']))
    if not matching_hook(hook, source):
        raise HookError('Configured hook metadata differs for ' + source['repo'])
    with closing(connect()) as db:
        row = db.execute('SELECT secret FROM webhook WHERE id=? AND url=?', (hook['id'], ENDPOINT)).fetchone()
    if row is None or not hmac.compare_digest(row['secret'], secret):
        raise HookError('Configured hook signature secret differs')
    return hook['id'], change


def delivery_receipt(row, source, secret):
    if not row['is_delivered'] or not row['response_content']:
        return None
    request, response = json.loads(row['request_content']), json.loads(row['response_content'])
    headers = {k.lower():v for k,v in request.get('headers', {}).items()}
    body = request.get('body', '')
    if not isinstance(body, str) or len(body.encode()) > 524288:
        raise HookError('Actual hook request body exceeds receiver bound')
    payload = json.loads(body)
    forgejo_repo = payload.get('repository', {}).get('full_name')
    signature = headers.get('x-forgejo-signature') or headers.get('x-gitea-signature', '')
    delivered_id = headers.get('x-forgejo-delivery') or headers.get('x-gitea-delivery')
    if (request.get('url') != ENDPOINT or request.get('http_method') != 'POST'
            or payload.get('ref') != source['ref'] or not isinstance(forgejo_repo, str)
            or forgejo_repo.casefold() != source['repo'].casefold()
            or ('forgejo_repo' in source and forgejo_repo != source['forgejo_repo'])
            or delivered_id != row['uuid'] or (headers.get('x-forgejo-event') or headers.get('x-gitea-event')) != 'push'
            or not hmac.compare_digest(signature, hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest())):
        raise HookError('Actual signed hook request identity differs for ' + source['repo'])
    try:
        result = json.loads(response.get('body', ''))
        accepted = result.get('accepted') is True and result.get('duplicate') is False
    except (ValueError, AttributeError):
        accepted = False
    if not row['is_succeed'] or response.get('status') != 202 or not accepted:
        raise HookError('Hook delivery was not accepted by the current corpus for ' + source['repo'])
    return {'source_id': source['id'], 'repo': source['repo'], 'forgejo_repo': forgejo_repo, 'ref': source['ref'],
            'hook_id': row['hook_id'], 'task_id': row['id'], 'delivery_uuid': row['uuid'],
            'status': 202, 'accepted': True, 'duplicate': False, 'signature_verified': True}


def test_delivery(api, source, hook_id, secret):
    with closing(connect()) as db:
        baseline = db.execute('SELECT coalesce(max(id),0) FROM hook_task WHERE hook_id=?', (hook_id,)).fetchone()[0]
    # Forgejo delivers the test synchronously through the public ingress and only
    # then answers; one bounded retry covers a slow first delivery (seen live on
    # October 16, 2026) without masking a dead endpoint.
    for attempt in (1, 2):
        try:
            api.call('POST', '/repos/' + source['repo'] + '/hooks/' + str(hook_id) + '/tests?' +
                     urllib.parse.urlencode({'ref': source['ref']}), expected=204)
            break
        except HookError as error:
            if attempt == 2 or 'TimeoutError' not in str(error):
                raise
            time.sleep(2)
    deadline = time.monotonic() + 60
    while True:
        with closing(connect()) as db:
            rows = db.execute('SELECT id,hook_id,uuid,is_delivered,is_succeed,substr(request_content,1,600000) AS request_content,'
                              'substr(response_content,1,65536) AS response_content FROM hook_task '
                              'WHERE hook_id=? AND id>? AND event_type=? ORDER BY id LIMIT 20',
                              (hook_id, baseline, 'push')).fetchall()
        for row in rows:
            receipt = delivery_receipt(row, source, secret)
            if receipt is not None:
                return receipt
        if time.monotonic() >= deadline:
            raise HookError('Timed out waiting for actual hook delivery for ' + source['repo'])
        time.sleep(1)


def validate_host():
    if (os.geteuid() != 0 or os.uname().nodename.split('.')[0] != 'forge'
            or Path('/sys/class/net/eth0/address').read_text().strip() != '02:00:0a:00:32:d3'):
        raise HookError('Run only as root inside the verified Forgejo guest')
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    parser.read_string('[DEFAULT]\n' + CONFIG.read_text())
    if parser.get('database', 'DB_TYPE') != 'sqlite3' or parser.get('database', 'PATH') != str(DATABASE):
        raise HookError('Forgejo database configuration differs')
    health()


def inspect_sources(api, sources):
    """Resolve all explicit branches and admin inventories before any mutation."""
    results = []
    for source in sources:
        base = '/repos/' + source['repo']
        repo = api.call('GET', base)
        canonical = repo.get('full_name')
        if (not isinstance(canonical, str) or canonical.casefold() != source['repo'].casefold()
                or type(repo.get('id')) is not int or repo['id'] < 1):
            raise HookError('Repository identity differs for ' + source['repo'])
        inventory = api.hooks(source['repo'])
        can_manage = repo.get('permissions', {}).get('admin') is True
        if not can_manage:
            raise HookError('Repository administrative permission is missing for ' + source['repo'])
        branch_name = source['ref'][len('refs/heads/'):]
        branch = api.call('GET', base + '/branches/' + urllib.parse.quote(branch_name, safe=''))
        revision = branch.get('commit', {}).get('id')
        if (branch.get('name') != branch_name or not isinstance(revision, str)
                or not re.fullmatch('[0-9a-f]{40}', revision)):
            raise HookError('Explicit branch resolution differs for ' + source['repo'])
        endpoint_hooks = [hook for hook in inventory if hook_url(hook) == ENDPOINT]
        if len(endpoint_hooks) > 1 or any(hook.get('type') != 'forgejo' for hook in endpoint_hooks):
            raise HookError('Existing corpus endpoint hook inventory conflicts for ' + source['repo'])
        results.append({'id': source['id'], 'configured': source['repo'], 'canonical': canonical,
                        'repo_id': repo['id'], 'ref': source['ref'], 'revision': revision,
                        'hook_count': len(inventory), 'can_manage': can_manage})
    return results


def preflight(value):
    sources, _ = validate_input(value, preflight=True)
    validate_host()
    token, _ = credential(create=False)
    results = inspect_sources(API(token), sources)
    return {'status': 'corpus_hooks_preflight', 'read_only': True,
            'repository_count': len(results), 'repositories': results}


def reconcile(value):
    sources, secret = validate_input(value)
    validate_host()
    token, created = credential()
    api = API(token)
    # Confirm every target exists and is accessible before changing hooks/config.
    inspected = inspect_sources(api, sources)
    canonicals = {item['id']: item['canonical'] for item in inspected}
    backup = enable_destination()
    receipts = []
    for source in sources:
        source = {**source, 'forgejo_repo': canonicals[source['id']]}
        hook_id, change = upsert(api, source, secret)
        receipts.append({**test_delivery(api, source, hook_id, secret), 'change': change})
    return {'status': 'corpus_hooks_accepted', 'endpoint': ENDPOINT, 'hooks': receipts,
            'repository_count': len(receipts), 'credential_created': created,
            'credential_scope': 'write:repository', 'configuration_backup': backup}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--preflight', action='store_true', help='Read-only corpus/ref/hook checks using the existing credential')
    args = parser.parse_args()
    os.umask(0o077)
    raw = sys.stdin.buffer.read(MAX_INPUT + 1)
    if len(raw) > MAX_INPUT:
        raise HookError('Input exceeds limit')
    value = json.loads(raw)
    if args.preflight:
        print(json.dumps(preflight(value), sort_keys=True), flush=True)
        return
    locks = []
    try:
        for path in ('/run/lock/forgejo-upgrade.lock', '/run/lock/retrieval-corpus-hooks.lock'):
            descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            locks.append(descriptor)
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
                raise HookError('Unsafe Forgejo operation lock')
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        receipt = reconcile(value)
        RECEIPT.parent.mkdir(mode=0o700, exist_ok=True)
        info = RECEIPT.parent.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o700:
            raise HookError('Unsafe acceptance receipt directory')
        atomic_write(RECEIPT, (json.dumps(receipt, sort_keys=True, indent=2) + '\n').encode(), 0, 0, 0o600)
    finally:
        for descriptor in reversed(locks):
            os.close(descriptor)
    print(json.dumps(receipt, sort_keys=True), flush=True)


if __name__ == '__main__':
    try:
        main()
    except HookError as error:
        print(json.dumps({'status': 'failed', 'error': str(error)}), file=sys.stderr)
        raise SystemExit(1)
    except Exception as error:
        # Do not emit arbitrary exception bodies: API/config objects may contain
        # secrets. The exception class and retained local state aid diagnosis.
        print(json.dumps({'status': 'failed', 'error_type': type(error).__name__}), file=sys.stderr)
        raise SystemExit(1)
