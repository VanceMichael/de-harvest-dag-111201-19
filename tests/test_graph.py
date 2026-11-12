"""动态依赖图引擎与持久化的单元测试。

只使用进程内脚本化客户端与临时 SQLite，不访问网络、不依赖时序睡眠；
并发不提前交付通过多轮竞争迭代来验证。
"""

import json
import tempfile
import threading
import unittest
from pathlib import Path

from collector.engine import Engine
from collector.graphstore import (
    CANCELED, FAILED, KIND_ASSET, KIND_DETAIL, KIND_WORK, OPTIONAL, PENDING,
    REQUIRED, RUNNING, ST_AVAILABLE_PARTIAL, ST_CORE_INCOMPLETE,
    ST_SUPERSEDED, SUCCEEDED, CycleError, GraphStore,
)
from collector.scenario import ScriptedClient


def make_store():
    directory = tempfile.TemporaryDirectory()
    store = GraphStore(Path(directory.name) / 'graph.sqlite3')
    return directory, store


class FakeClient:
    """可编排响应的最小客户端；支持按调用次数返回不同状态。"""

    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    def request(self, url, method='GET', headers=None, body=None):
        path = '/' + url.split('/', 3)[-1]
        self.calls.append(dict(path=path, headers=dict(headers or {})))
        replies = self.routes[path]
        index = sum(1 for c in self.calls if c['path'] == path) - 1
        # 越界后固定重复最后一个响应
        reply = replies[min(index, len(replies) - 1)]
        return reply['status'], dict(reply.get('headers', {})), \
            reply.get('body', '').encode()

def build_simple_graph(store, required=('/a',), optional=('/opt',)):
    gen = store.next_gen()
    store.ensure_node('w@1', KIND_WORK, gen=gen)
    store.ensure_node('detail:/d', KIND_DETAIL, url='/d', gen=gen)
    store.add_edge('w@1', 'w@1', 'detail:/d', REQUIRED, gen)
    store.subscribe('w@1', 'detail:/d')
    for url in required:
        node = 'asset:' + url
        store.ensure_node(node, KIND_ASSET, url=url, gen=gen)
        store.add_edge('w@1', 'detail:/d', node, REQUIRED, gen)
        store.subscribe('w@1', node)
    for url in optional:
        node = 'asset:' + url
        store.ensure_node(node, KIND_ASSET, url=url, gen=gen)
        store.add_edge('w@1', 'detail:/d', node, OPTIONAL, gen)
        store.subscribe('w@1', node)
    return gen


class GraphStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory, self.store = make_store()

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def test_add_edge_rejects_direct_and_indirect_cycle(self):
        self.store.ensure_node('a', KIND_ASSET)
        self.store.ensure_node('b', KIND_ASSET)
        self.store.add_edge('w@1', 'a', 'b', REQUIRED, 1)
        with self.assertRaises(CycleError):
            self.store.add_edge('w@1', 'b', 'a', REQUIRED, 1)
        with self.assertRaises(CycleError):
            self.store.add_edge('w@1', 'a', 'a', REQUIRED, 1)

    def test_subscribe_is_idempotent_and_unsubscribe_refcounts(self):
        self.store.ensure_node('x', KIND_ASSET)
        self.store.subscribe('w@1', 'x')
        self.store.subscribe('w@1', 'x')  # 重复发现不应累加
        self.assertEqual(self.store.active_subscriber_count('x'), 1)
        self.store.subscribe('w@2', 'x')
        self.assertEqual(self.store.active_subscriber_count('x'), 2)
        self.assertEqual(self.store.unsubscribe('w@1', 'x'), 1)
        self.assertEqual(self.store.active_subscriber_count('x'), 1)
        self.assertEqual(self.store.unsubscribe('w@2', 'x'), 0)

    def test_work_view_distinguishes_optional_and_required_gaps(self):
        build_simple_graph(self.store)
        view = self.store.work_view('w@1')
        self.assertIn(view['status'], ('采集中', ST_CORE_INCOMPLETE))
        # 必需附件齐全、可选附件缺失 => 可用但缺可选项
        self.store.set_node_state('detail:/d', SUCCEEDED)
        self.store.set_node_state('asset:/a', SUCCEEDED)
        view = self.store.work_view('w@1')
        self.assertEqual(view['status'], ST_AVAILABLE_PARTIAL)
        self.assertEqual([i['node'] for i in view['missing_optional']],
                         ['asset:/opt'])
        # 全部齐全 => 完整
        self.store.set_node_state('asset:/opt', SUCCEEDED)
        self.assertEqual(self.store.work_view('w@1')['status'], '完整')
        # 必需附件失败 => 核心资料不齐
        self.store.set_node_state('asset:/a', FAILED, 'HTTP 500')
        self.assertEqual(self.store.work_view('w@1')['status'],
                         ST_CORE_INCOMPLETE)


class EngineBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.directory, self.store = make_store()

    def tearDown(self):
        self.engine.shutdown(wait=True)
        self.store.close()
        self.directory.cleanup()

    def test_detail_304_keeps_previously_discovered_dependencies(self):
        gen = self.store.next_gen()
        self.store.ensure_node('w@1', KIND_WORK, gen=gen)
        self.store.ensure_node('detail:/d', KIND_DETAIL, url='/d', gen=gen)
        self.store.add_edge('w@1', 'w@1', 'detail:/d', REQUIRED, gen)
        self.store.subscribe('w@1', 'detail:/d')
        # 上一轮已经发现 /keep 并持久化（产物与边都在库里）
        self.store.ensure_node('asset:/keep', KIND_ASSET, url='/keep', gen=gen)
        self.store.add_edge('w@1', 'detail:/d', 'asset:/keep', REQUIRED, gen)
        self.store.subscribe('w@1', 'asset:/keep')
        self.store.save_product('detail:/d', '"d"', gen, 'application/json', b'old')
        client = FakeClient({
            '/d': [dict(status=304, headers={'ETag': '"d"'})],
            '/keep': [dict(status=200, body='ok')],
        })
        self.engine = Engine(self.store, client, max_attempts=2,
                             retry_interval=0.001)
        self.engine.start()
        self.engine.wait_idle(timeout=5)
        # 304 后 /keep 仍被请求并成功，证明依赖没有被当成“空集合”
        self.assertEqual(self.store.get_node('asset:/keep')['state'], SUCCEEDED)
        self.assertEqual(self.store.work_view('w@1')['status'], '完整')
        journals = [j['kind'] for j in self.store.list_journals()]
        self.assertIn('detail_not_modified', journals)

    def test_failure_retries_only_failed_node_and_succeeds(self):
        build_simple_graph(self.store, optional=())
        client = FakeClient({
            '/d': [dict(status=200, body=json.dumps(dict(required=['/a'])),
                        headers={'ETag': '"d"'})],
            '/a': [dict(status=500), dict(status=500), dict(status=200, body='ok')],
        })
        self.engine = Engine(self.store, client, max_attempts=5,
                             retry_interval=0.001)
        self.engine.start()
        self.engine.wait_idle(timeout=5)
        self.assertEqual(self.store.get_node('asset:/a')['state'], SUCCEEDED)
        # 详情只请求一次，失败没有触发整条链重跑
        self.assertEqual(sum(1 for c in client.calls if c['path'] == '/d'), 1)
        self.assertEqual(sum(1 for c in client.calls if c['path'] == '/a'), 3)
        self.assertEqual(self.store.work_view('w@1')['status'], '完整')

    def test_exhausted_retries_mark_core_incomplete_and_persist(self):
        build_simple_graph(self.store, optional=())
        client = FakeClient({
            '/d': [dict(status=200, body=json.dumps(dict(required=['/a'])))],
            '/a': [dict(status=500)] * 5,
        })
        self.engine = Engine(self.store, client, max_attempts=3,
                             retry_interval=0.001)
        self.engine.start()
        self.engine.wait_idle(timeout=5)
        node = self.store.get_node('asset:/a')
        self.assertEqual(node['state'], FAILED)
        self.assertEqual(node['attempts'], 3)
        self.assertEqual(self.store.work_view('w@1')['status'],
                         ST_CORE_INCOMPLETE)
        # 请求流水持久化
        self.assertEqual(self.store.request_counts().get('asset:/a'), 3)

    def test_cancel_one_subscriber_keeps_shared_asset_running(self):
        # 两个成果共享 /shared
        build_simple_graph(self.store)
        gen = self.store.next_gen()
        self.store.ensure_node('w2@1', KIND_WORK, gen=gen)
        self.store.add_edge('w2@1', 'w2@1', 'detail:/d', REQUIRED, gen)
        self.store.subscribe('w2@1', 'detail:/d')
        self.store.add_edge('w2@1', 'detail:/d', 'asset:/a', REQUIRED, gen)
        self.store.subscribe('w2@1', 'asset:/a')
        gate = threading.Event()

        class GatedClient(FakeClient):
            def request(self, url, method='GET', headers=None, body=None):
                path = '/' + url.split('/', 3)[-1]
                if path == '/a':
                    gate.wait(timeout=5)
                return super().request(url, method, headers, body)

        client = GatedClient({
            '/d': [dict(status=200, body=json.dumps(dict(required=['/a'])))],
            '/a': [dict(status=200, body='shared')],
            '/opt': [dict(status=200, body='opt')],
        })
        self.engine = Engine(self.store, client, workers=4,
                             retry_interval=0.001)
        self.engine.start()
        self.engine.wait_until(
            lambda: self.store.get_node('asset:/a')['state'] == RUNNING, timeout=5)
        self.engine.cancel_work('w@1')
        gate.set()
        self.engine.wait_idle(timeout=5)
        # 共享节点仍被 w2@1 订阅，最终成功而不是被取消
        self.assertEqual(self.store.get_node('asset:/a')['state'], SUCCEEDED)
        self.assertEqual(self.store.get_node('w@1')['state'], CANCELED)
        self.assertEqual(self.store.work_view('w2@1')['status'], '完整')

    def test_shared_node_stops_only_after_all_subscribers_cancel(self):
        build_simple_graph(self.store, optional=())
        gen = self.store.next_gen()
        self.store.ensure_node('w2@1', KIND_WORK, gen=gen)
        self.store.add_edge('w2@1', 'w2@1', 'detail:/d', REQUIRED, gen)
        self.store.subscribe('w2@1', 'detail:/d')
        self.store.add_edge('w2@1', 'detail:/d', 'asset:/a', REQUIRED, gen)
        self.store.subscribe('w2@1', 'asset:/a')
        gate = threading.Event()

        class GatedFailClient(FakeClient):
            def request(self, url, method='GET', headers=None, body=None):
                path = '/' + url.split('/', 3)[-1]
                if path == '/a':
                    gate.wait(timeout=10)
                return super().request(url, method, headers, body)

        client = GatedFailClient({
            '/d': [dict(status=200, body=json.dumps(dict(required=['/a'])))],
            '/a': [dict(status=500)] * 10,
        })
        self.engine = Engine(self.store, client, max_attempts=8,
                             retry_interval=0.02)
        self.engine.start()
        # 等待共享附件的第一次请求在途（被门闩阻塞）
        self.engine.wait_until(
            lambda: self.store.get_node('asset:/a')['state'] == RUNNING, timeout=10)
        # cancel_work 同步落库，订阅计数可立即断言，无需轮询
        self.engine.cancel_work('w@1')
        self.assertEqual(self.store.active_subscriber_count('asset:/a'), 1)
        self.assertNotEqual(self.store.get_node('asset:/a')['state'], CANCELED)
        self.engine.cancel_work('w2@1')
        self.assertEqual(self.store.active_subscriber_count('asset:/a'), 0)
        # 放行在途请求；失败返回时守卫保持取消状态，不复活
        gate.set()
        self.engine.wait_until(
            lambda: self.store.get_node('asset:/a')['state'] == CANCELED, timeout=10)

    def test_dynamic_detail_discovery_adds_edges_and_delivers(self):
        gen = self.store.next_gen()
        self.store.ensure_node('w@1', KIND_WORK, gen=gen)
        self.store.ensure_node('detail:/d', KIND_DETAIL, url='/d', gen=gen)
        self.store.add_edge('w@1', 'w@1', 'detail:/d', REQUIRED, gen)
        self.store.subscribe('w@1', 'detail:/d')
        client = FakeClient({
            # 详情动态发现一个目录阶段并不知道的新附件
            '/d': [dict(status=200,
                        body=json.dumps(dict(required=['/discovered'])),
                        headers={'ETag': '"d"'})],
            '/discovered': [dict(status=200, body='new')],
        })
        self.engine = Engine(self.store, client, retry_interval=0.001)
        self.engine.start()
        self.engine.wait_idle(timeout=5)
        self.assertEqual(self.store.get_node('asset:/discovered')['state'],
                         SUCCEEDED)
        edges = {e['to_node'] for e in self.store.edges_for_revision('w@1')}
        self.assertIn('asset:/discovered', edges)
        self.assertEqual(self.store.work_view('w@1')['status'], '完整')

    def test_late_old_generation_product_does_not_complete_new_revision(self):
        gen1 = build_simple_graph(self.store, optional=())
        # 成果在更晚代次（gen1 之后）重新冻结对 /a 的依赖，但 /a 当前失败
        later_gen = self.store.next_gen()
        self.store.set_node_state('detail:/d', SUCCEEDED)
        self.store.set_node_state('asset:/a', FAILED, 'HTTP 500')
        # 把依赖边的冻结代次提升到 later_gen，模拟新修订重新冻结
        self.store.connection.execute(
            'UPDATE edges SET frozen_gen=? WHERE to_node=?',
            (later_gen, 'asset:/a'))
        client = FakeClient({'/d': [], '/a': [dict(status=500)]})
        self.engine = Engine(self.store, client, retry_interval=0.001,
                             validate_details=False)
        self.engine.start()
        # 旧代次（gen=gen1）的 /a 晚到
        self.engine.ingest_late_product('asset:/a', gen=gen1, payload=b'late')
        self.engine.wait_idle(timeout=3)
        view = self.store.work_view('w@1')
        self.assertEqual(view['status'], ST_CORE_INCOMPLETE)
        kinds = [j['kind'] for j in self.store.list_journals()]
        self.assertIn('late_product', kinds)
        self.assertIn('stale_product_ignored', kinds)

    def test_revision_supersede_freezes_independent_dependency_sets(self):
        build_simple_graph(self.store, optional=())
        client = FakeClient({
            '/d': [dict(status=200, body=json.dumps(dict(required=['/a'])))],
            '/a': [dict(status=500)] * 8,
            '/index': [
                dict(status=200, body=json.dumps(dict(works=[
                    dict(id='w', revision=1, detail='/d', required=['/a'])]))),
                dict(status=200, body=json.dumps(dict(works=[
                    dict(id='w', revision=2, detail='/d2', required=['/b'])]))),
            ],
            '/d2': [dict(status=200, body=json.dumps(dict(required=['/b'])))],
            '/b': [dict(status=200, body='b')],
        })
        self.store.ensure_node('catalog:/index', 'catalog', url='/index')
        self.engine = Engine(self.store, client, max_attempts=8,
                             retry_interval=0.001, validate_details=False)
        self.engine.start()
        # 第一次目录响应给出修订1（与已存在修订幂等）；再次轮询才发布修订2
        self.engine.wait_until(
            lambda: self.store.get_node('catalog:/index')['attempts'] >= 1,
            timeout=5)
        self.engine.poll_catalog('catalog:/index')
        self.engine.wait_until(
            lambda: self.store.get_node('w@2') is not None, timeout=5)
        self.engine.wait_idle(timeout=5)
        self.assertEqual(self.store.get_node('w@1')['state'], 'superseded')
        view2 = self.store.work_view('w@2')
        self.assertEqual(view2['status'], '完整')
        # 新修订的冻结集合不含旧附件 /a
        to_nodes = {e['to_node'] for e in self.store.edges_for_revision('w@2')}
        self.assertIn('asset:/b', to_nodes)
        self.assertNotIn('asset:/a', to_nodes)
        self.assertEqual(view2['status'], '完整')


class RecoveryTests(unittest.TestCase):
    def test_engine_restart_resumes_without_re_requesting_succeeded_assets(self):
        directory = tempfile.TemporaryDirectory()
        path = Path(directory.name) / 'graph.sqlite3'
        store = GraphStore(path)
        build_simple_graph(store, optional=())
        client = FakeClient({
            '/d': [dict(status=200, body=json.dumps(dict(required=['/a'])),
                        headers={'ETag': '"d"'}),
                   dict(status=304, headers={'ETag': '"d"'})],
            '/a': [dict(status=200, body='a'),
                   dict(status=200, body='a-again')],
        })
        engine = Engine(store, client, retry_interval=0.001)
        engine.start()
        engine.wait_idle(timeout=5)
        engine.shutdown(wait=True)
        first_asset_calls = sum(1 for c in client.calls if c['path'] == '/a')
        self.assertEqual(first_asset_calls, 1)
        store.close()

        # 新进程：仅凭 SQLite 恢复
        store = GraphStore(path)
        engine = Engine(store, client, retry_interval=0.001, recovery=True)
        engine.start()
        engine.wait_idle(timeout=5)
        engine.shutdown(wait=True)
        # 附件 /a 已成功，重启后绝不重发；详情做一次 304 条件验证
        self.assertEqual(sum(1 for c in client.calls if c['path'] == '/a'), 1)
        self.assertEqual(sum(1 for c in client.calls if c['path'] == '/d'), 2)
        second = [c for c in client.calls if c['path'] == '/d'][1]
        self.assertEqual(second['headers'].get('If-None-Match'), '"d"')
        self.assertEqual(store.work_view('w@1')['status'], '完整')
        store.close()
        directory.cleanup()


class ConcurrencyTests(unittest.TestCase):
    def test_no_early_delivery_under_completion_vs_dependency_race(self):
        for iteration in range(30):
            directory, store = make_store()
            gen = store.next_gen()
            store.ensure_node('w@1', KIND_WORK, gen=gen)
            store.ensure_node('detail:/d', KIND_DETAIL, url='/d', gen=gen)
            store.add_edge('w@1', 'w@1', 'detail:/d', REQUIRED, gen)
            store.subscribe('w@1', 'detail:/d')
            store.ensure_node('asset:/a', KIND_ASSET, url='/a', gen=gen)
            store.add_edge('w@1', 'detail:/d', 'asset:/a', REQUIRED, gen)
            store.subscribe('w@1', 'asset:/a')
            client = FakeClient({
                '/d': [dict(status=200,
                            body=json.dumps(dict(required=['/a', '/late'])))],
                '/a': [dict(status=200, body='a')],
                '/late': [dict(status=200, body='late')],
            })
            engine = Engine(store, client, workers=4, retry_interval=0.001)
            violations = []

            def listener(event):
                if event.get('event') == 'delivered':
                    view = store.work_view('w@1')
                    required_states = [
                        store.get_node(i['node'])['state']
                        for i in view['missing_required'] + view['failed_required']]
                    if required_states:
                        violations.append(required_states)

            engine.on_change(listener)
            engine.start()
            engine.wait_idle(timeout=10)
            engine.shutdown(wait=True)
            self.assertEqual(violations, [], f'迭代 {iteration} 出现提前交付')
            self.assertEqual(store.work_view('w@1')['status'], '完整')
            store.close()
            directory.cleanup()


if __name__ == '__main__':
    unittest.main()
