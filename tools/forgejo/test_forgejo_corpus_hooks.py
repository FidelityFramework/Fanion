from contextlib import closing
import hashlib
import hmac
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

import forgejo_corpus_hooks as hooks


class HookSetupTests(unittest.TestCase):
    def setUp(self):
        self.source = {'id': 'hello-wayland', 'repo': 'FidelityFramework/HelloWayland',
                       'ref': 'refs/heads/pre-light-mode'}
        self.secret = 'fixture-hmac-secret-' * 3
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.database = Path(temporary.name) / 'forgejo.db'
        with closing(sqlite3.connect(self.database)) as db:
            db.execute('CREATE TABLE webhook (id INTEGER PRIMARY KEY, url TEXT, secret TEXT)')
            db.commit()
        self.database_patch = patch.object(hooks, 'DATABASE', self.database)
        self.database_patch.start()
        self.addCleanup(self.database_patch.stop)

    def hook(self, identifier=7):
        return {'id': identifier, 'url': hooks.ENDPOINT, 'type': 'forgejo', 'active': True,
                'events': ['push'], 'branch_filter': 'pre-light-mode', 'content_type': 'json',
                'authorization_header': '', 'config': {'url': hooks.ENDPOINT, 'content_type': 'json'}}

    def register(self, identifier=7, secret=None):
        with closing(sqlite3.connect(self.database)) as db:
            db.execute('INSERT INTO webhook VALUES(?,?,?)', (identifier, hooks.ENDPOINT, secret or self.secret))
            db.commit()

    def delivery(self):
        identifier = '11760026-6e93-4a8d-a6b6-75b5316488ce'
        payload = {'repository': {'full_name': self.source['repo']}, 'ref': self.source['ref']}
        body = json.dumps(payload, separators=(',', ':'))
        request = {'url': hooks.ENDPOINT, 'http_method': 'POST', 'body': body, 'headers': {
            'X-Forgejo-Event': 'push', 'X-Forgejo-Delivery': identifier,
            'X-Forgejo-Signature': hmac.new(self.secret.encode(), body.encode(), hashlib.sha256).hexdigest()}}
        return {'id': 17, 'hook_id': 7, 'uuid': identifier, 'is_delivered': 1, 'is_succeed': 1,
                'request_content': json.dumps(request),
                'response_content': json.dumps({'status': 202, 'body': '{"accepted":true,"duplicate":false}'})}

    def test_allowlist_is_surgical_and_idempotent(self):
        before = 'APP_NAME = example\n[server]\nSECRET = preserve-exactly\n[webhook]\nDELIVER_TIMEOUT = 5\n[other]\nKEY = retained\n'
        after = hooks.allowlist_text(before)
        self.assertEqual(after, before.replace('[other]', 'ALLOWED_HOST_LIST = external,' + hooks.HOST + '\n[other]'))
        self.assertEqual(hooks.allowlist_text(after), after)
        absent = '[server]\nSECRET = preserve-exactly\n'
        self.assertEqual(hooks.allowlist_text(absent), absent + '\n[webhook]\nALLOWED_HOST_LIST = external,' + hooks.HOST + '\n')
        explicit = '[webhook]\nALLOWED_HOST_LIST = external,example.invalid  ; existing comment\n'
        result = hooks.allowlist_text(explicit)
        self.assertEqual(result, '[webhook]\nALLOWED_HOST_LIST = external,example.invalid,' + hooks.HOST + '  ; existing comment\n')
        self.assertEqual(hooks.allowlist_text(result), result)
        with self.assertRaises(hooks.HookError):
            hooks.allowlist_text('[webhook]\nALLOWED_HOST_LIST=x\nALLOWED_HOST_LIST=y\n')

    def test_validated_input_retains_each_explicit_ref(self):
        value = {'corpus': {'schema_version': 1, 'sources': [self.source]}, 'webhook_secret': self.secret}
        self.assertEqual(hooks.validate_input(value), ([self.source], self.secret))
        for key, value in [('ref', 'main'), ('ref', 'refs/heads/*'), ('repo', 'OtherOrg/repo')]:
            invalid = {**self.source, key: value}
            with self.subTest(key=key, value=value), self.assertRaises(hooks.HookError):
                hooks.validate_input({'corpus': {'schema_version': 1, 'sources': [invalid]}, 'webhook_secret': self.secret})
        duplicate = {**self.source, 'id': 'duplicate', 'repo': 'FidelityFramework/HELLOWAYLAND'}
        with self.assertRaises(hooks.HookError):
            hooks.validate_input({'corpus': {'schema_version': 1, 'sources': [self.source, duplicate]}}, preflight=True)

    def test_upsert_leaves_unrelated_hooks_and_matching_hook_unchanged(self):
        self.register()
        unrelated = {**self.hook(3), 'url': 'https://elsewhere.invalid/hook'}
        api = Mock()
        api.hooks.return_value = [unrelated, self.hook()]
        api.call.return_value = self.hook()
        self.assertEqual(hooks.upsert(api, self.source, self.secret), (7, 'unchanged'))
        api.call.assert_called_once_with('GET', '/repos/FidelityFramework/HelloWayland/hooks/7')
        self.assertEqual(unrelated['url'], 'https://elsewhere.invalid/hook')

    def test_new_hook_and_secret_update_target_only_exact_endpoint(self):
        api = Mock()
        api.hooks.return_value = [{**self.hook(3), 'url': 'https://elsewhere.invalid/hook'}]

        def create(method, path, data=None, expected=200):
            if method == 'POST':
                self.assertEqual(expected, 201)
                self.assertEqual(data['branch_filter'], 'pre-light-mode')
                self.assertEqual(data['events'], ['push'])
                self.assertEqual(data['config'], {'url': hooks.ENDPOINT, 'content_type': 'json', 'secret': self.secret})
                self.register()
            return self.hook()

        api.call.side_effect = create
        self.assertEqual(hooks.upsert(api, self.source, self.secret), (7, 'created'))
        self.assertEqual([call.args[0] for call in api.call.call_args_list], ['POST', 'GET'])
        with closing(sqlite3.connect(self.database)) as db:
            db.execute('UPDATE webhook SET secret=? WHERE id=7', ('old-secret',))
            db.commit()
        api.reset_mock()
        api.hooks.return_value = [self.hook()]

        def update(method, path, data=None, expected=200):
            self.assertEqual(path, '/repos/FidelityFramework/HelloWayland/hooks/7')
            if method == 'PATCH':
                with closing(sqlite3.connect(self.database)) as db:
                    db.execute('UPDATE webhook SET secret=? WHERE id=7', (data['config']['secret'],))
                    db.commit()
            return self.hook()

        api.call.side_effect = update
        self.assertEqual(hooks.upsert(api, self.source, self.secret), (7, 'updated'))
        self.assertEqual([call.args[0] for call in api.call.call_args_list], ['PATCH', 'GET'])

    def test_duplicate_endpoint_hooks_fail_without_mutation(self):
        api = Mock()
        api.hooks.return_value = [self.hook(1), self.hook(2)]
        with self.assertRaises(hooks.HookError):
            hooks.upsert(api, self.source, self.secret)
        api.call.assert_not_called()

    def test_api_timeouts_retry_reads_once_and_never_writes(self):
        api = hooks.API('token')
        timeout = hooks.HookError('Forgejo API connection failed for GET /repos/x (TimeoutError: timed out)')
        with patch.object(hooks.time, 'sleep'):
            with patch.object(api, 'once', side_effect=[timeout, {'ok': True}]) as once:
                self.assertEqual(api.call('GET', '/repos/x'), {'ok': True})
                self.assertEqual(once.call_count, 2)
            with patch.object(api, 'once', side_effect=[timeout, timeout]) as once:
                with self.assertRaisesRegex(hooks.HookError, 'TimeoutError'):
                    api.call('GET', '/repos/x')
                self.assertEqual(once.call_count, 2)
            for method in ('POST', 'PATCH', 'DELETE'):
                with patch.object(api, 'once', side_effect=[timeout]) as once:
                    with self.assertRaisesRegex(hooks.HookError, 'TimeoutError'):
                        api.call(method, '/repos/x/hooks', data={})
                    self.assertEqual(once.call_count, 1)
            with patch.object(api, 'once', side_effect=[hooks.HookError('Forgejo API rejected operation with HTTP 404 for GET /repos/x')]) as once:
                with self.assertRaisesRegex(hooks.HookError, 'HTTP 404'):
                    api.call('GET', '/repos/x')
                self.assertEqual(once.call_count, 1)

    def test_actual_signed_delivery_returns_only_sanitized_identity(self):
        row = self.delivery()
        receipt = hooks.delivery_receipt(row, self.source, self.secret)
        self.assertEqual(receipt, {**{'source_id': self.source['id'], 'repo': self.source['repo'], 'forgejo_repo': self.source['repo'], 'ref': self.source['ref']},
            'hook_id': 7, 'task_id': 17, 'delivery_uuid': row['uuid'], 'status': 202,
            'accepted': True, 'duplicate': False, 'signature_verified': True})
        self.assertNotIn(self.secret, json.dumps(receipt))
        self.assertNotIn('headers', receipt)
        self.assertIsNone(hooks.delivery_receipt({**row, 'response_content': ''}, self.source, self.secret))

    def test_canonical_repo_case_preserves_configured_mapping_and_exact_ref(self):
        configured = {**self.source, 'id': 'wren', 'repo': 'FidelityFramework/WrenHello', 'ref': 'refs/heads/main',
                      'forgejo_repo': 'FidelityFramework/WRENHello'}
        row = self.delivery()
        request = json.loads(row['request_content'])
        request['body'] = json.dumps({'repository': {'full_name': configured['forgejo_repo']}, 'ref': configured['ref']})
        request['headers']['X-Forgejo-Signature'] = hmac.new(self.secret.encode(), request['body'].encode(), hashlib.sha256).hexdigest()
        row['request_content'] = json.dumps(request)
        receipt = hooks.delivery_receipt(row, configured, self.secret)
        self.assertEqual(receipt['repo'], 'FidelityFramework/WrenHello')
        self.assertEqual(receipt['forgejo_repo'], 'FidelityFramework/WRENHello')
        with self.assertRaises(hooks.HookError):
            hooks.delivery_receipt(row, {**configured, 'ref': 'refs/heads/Main'}, self.secret)

    def test_preflight_only_reads_existing_credential_repo_hooks_and_exact_branch(self):
        source = {**self.source, 'id': 'wren', 'repo': 'FidelityFramework/WrenHello', 'ref': 'refs/heads/main'}
        api = Mock()
        api.hooks.return_value = []
        api.call.side_effect = [
            {'id': 29, 'full_name': 'FidelityFramework/WRENHello', 'permissions': {'admin': True}},
            {'name': 'main', 'commit': {'id': 'a' * 40}},
        ]
        with patch.object(hooks, 'validate_host'), patch.object(hooks, 'credential', return_value=('a' * 40, False)) as credential, \
                patch.object(hooks, 'API', return_value=api), patch.object(hooks, 'enable_destination') as config, \
                patch.object(hooks, 'upsert') as upsert, patch.object(hooks, 'test_delivery') as delivery:
            result = hooks.preflight({'corpus': {'schema_version': 1, 'sources': [source]}})
        credential.assert_called_once_with(create=False)
        config.assert_not_called()
        upsert.assert_not_called()
        delivery.assert_not_called()
        self.assertTrue(result['read_only'])
        self.assertEqual(result['repositories'], [{'id': 'wren', 'configured': 'FidelityFramework/WrenHello',
                         'canonical': 'FidelityFramework/WRENHello', 'repo_id': 29, 'ref': 'refs/heads/main',
                         'revision': 'a' * 40, 'hook_count': 0, 'can_manage': True}])
        self.assertEqual([call.args for call in api.call.call_args_list], [
            ('GET', '/repos/FidelityFramework/WrenHello'), ('GET', '/repos/FidelityFramework/WrenHello/branches/main')])
        api.hooks.assert_called_once_with('FidelityFramework/WrenHello')

    def test_preflight_rejects_case_changed_branch_or_unrelated_repository(self):
        for repo, branch in [('FidelityFramework/Other', 'pre-light-mode'),
                             ('FidelityFramework/HELLOWAYLAND', 'Pre-light-mode')]:
            api = Mock()
            api.hooks.return_value = []
            api.call.side_effect = [
                {'id': 1, 'full_name': repo, 'permissions': {'admin': True}},
                {'name': branch, 'commit': {'id': 'a' * 40}},
            ]
            with self.subTest(repo=repo, branch=branch), self.assertRaises(hooks.HookError):
                hooks.inspect_sources(api, [self.source])

    def test_ignored_duplicate_wrong_uuid_ref_signature_or_status_never_accept(self):
        cases = [('accepted', False), ('duplicate', True), ('status', 200), ('uuid', 'another-delivery'),
                 ('ref', 'main'), ('signature', 'bad')]
        for kind, value in cases:
            row = self.delivery()
            request = json.loads(row['request_content'])
            response = json.loads(row['response_content'])
            if kind in ('accepted', 'duplicate'):
                body = json.loads(response['body'])
                body[kind] = value
                response['body'] = json.dumps(body)
            elif kind == 'status':
                response['status'] = value
            elif kind == 'uuid':
                row['uuid'] = value
            elif kind == 'signature':
                request['headers']['X-Forgejo-Signature'] = value
            else:
                payload = json.loads(request['body'])
                payload['ref'] = value
                request['body'] = json.dumps(payload)
                request['headers']['X-Forgejo-Signature'] = hmac.new(self.secret.encode(), request['body'].encode(), hashlib.sha256).hexdigest()
            row.update(request_content=json.dumps(request), response_content=json.dumps(response))
            with self.subTest(kind=kind), self.assertRaises(hooks.HookError):
                hooks.delivery_receipt(row, self.source, self.secret)


if __name__ == '__main__':
    unittest.main()
