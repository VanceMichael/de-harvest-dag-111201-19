"""基于 fixtures/graph.json 的端到端可恢复采集演示。

场景脚本（与 fixtures 中的事件一一对应）：
1. 目录给出 r1@1、r2@1 两个成果修订，各自冻结依赖；/shared 是共享必需附件。
2. 详情 /d1 返回 304 —— 引擎必须沿用此前已发现并持久化的依赖，而不是当作无附件。
3. /shared 反复失败（500），只重跑 /shared 这一个节点，目录与详情请求不重发。
4. 取消 r1：r1 自身在途的可选附件 /o1 立即停；/shared 仍被 r2 订阅，继续重试不中断。
5. 目录更新，r2 发布修订 2，必需附件变为 /new；r2@1 被替代，/shared 失去全部订阅者后停。
6. /new 成功，r2@2 交付；晚到的 /shared 旧代次（revision 1）产物入库，但不把任何新成果标完整。
7. 中途关闭并重建引擎（新进程语义）：已成功节点不重复请求，仅详情做一次 304 条件验证。

默认使用进程内脚本化客户端，每个请求的响应与计数完全确定；
传入 network=True 可切换为真实回环 HTTP 来源（collector.transport.replay_server）。
"""

import json
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from .engine import Engine
from .graphstore import (
    CANCELED, KIND_ASSET, KIND_CATALOG, KIND_DETAIL, KIND_WORK,
    OPTIONAL, REQUIRED, SUCCEEDED, GraphStore,
)
from .transport import Client, replay_server


class ScriptedClient:
    """按路径依次消费响应脚本的进程内客户端，记录每一次“真正发出的请求”。

    响应脚本支持：
    - delay: 返回前固定睡眠；
    - gate:  阻塞到外部调用 release_gate(path) 才返回，用于确定性地制造
             “请求在途期间发生取消”的场景。
    """

    def __init__(self, routes):
        self.routes = routes
        self.calls = []
        self._positions = {}
        # 门闩按 (路径, 响应序号) 惰性建立，避免构造期事件被共享或提前释放
        self._gates = {}
        self._gate_lock = threading.Lock()

    def release_gate(self, name):
        """放行所有使用该门闩名称、且已经开始等待的请求。"""
        with self._gate_lock:
            events = [event for (path, index, gate_name), event
                      in self._gates.items() if gate_name == name]
        for event in events:
            event.set()

    def wait_gate_active(self, name, timeout=5.0):
        """阻塞到指定门闩至少有一个请求正在等待，避免“先释放后等待”竞态。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._gate_lock:
                if any(gate_name == name for (_, _, gate_name) in self._gates):
                    return True
            time.sleep(0.01)
        raise TimeoutError(f'门闩 {name} 未在限定时间内被请求等待')

    def request(self, url, method='GET', headers=None, body=None):
        # url 形如 http://127.0.0.1:0/index，取路径部分
        path = '/' + url.split('/', 3)[-1]
        self.calls.append(dict(method=method, path=path,
                               headers=dict(headers or {}), body=body))
        replies = self.routes.get(path, [dict(status=404, body='未找到资源')])
        index = self._positions.get(path, 0)
        self._positions[path] = index + 1
        reply = replies[min(index, len(replies) - 1)]
        status = reply.get('status', 200)
        # 先登记响应状态，再进入可能阻塞的门闩等待
        self.calls[-1]['status'] = status
        if reply.get('delay'):
            time.sleep(reply['delay'])
        gate_name = reply.get('gate')
        if gate_name:
            key = (path, index, gate_name)
            with self._gate_lock:
                event = self._gates.get(key)
                if event is None:
                    event = threading.Event()
                    self._gates[key] = event
            event.wait(timeout=10)
        data = reply.get('body', '').encode('utf-8')
        return status, dict(reply.get('headers', {})), data


def _catalog_payload(works):
    return dict(works=[dict(id=w['id'], revision=w['revision'],
                            detail=w['detail'], required=w.get('required', []),
                            optional=w.get('optional', [])) for w in works])


def _routes(works_v1, works_v2):
    detail_d1 = dict(required=['/shared'], optional=['/o1'])
    detail_d2 = dict(required=['/new'])
    return {
        '/index': [
            dict(status=200, body=json.dumps(_catalog_payload(works_v1))),
            dict(status=200, body=json.dumps(_catalog_payload(works_v2))),
            dict(status=304, headers={'ETag': '"catalog"'}),
        ],
        '/d1': [
            dict(status=200, body=json.dumps(detail_d1), headers={'ETag': '"d1"'}),
            dict(status=304, headers={'ETag': '"d1"'}),
            dict(status=304, headers={'ETag': '"d1"'}),
        ],
        '/d2': [
            dict(status=200, body=json.dumps(detail_d2), headers={'ETag': '"d2"'}),
            dict(status=304, headers={'ETag': '"d2"'}),
        ],
        # /shared 在整个演示期间持续故障：重试与取消/替代的全过程都不会收到成功，
        # 从而干净展示“反复失败只重跑该节点、最终随订阅者归零而停止”
        '/shared': [dict(status=500, body='临时故障')] * 12,
        # 门闩响应：/o1 请求会在途阻塞，直到演示显式放行，
        # 用来确定性地制造“取消发生在可选附件请求在途期间”的场景
        '/o1': [dict(status=200, body='可选附件内容', gate='after_cancel')],
        '/new': [dict(status=200, body='新修订核心附件')],
    }


class NetworkClient:
    """真实回环客户端，并把服务端门闩释放暴露成与 ScriptedClient 相同的接口。"""

    def __init__(self, client, release_gate):
        self._client = client
        self._release_gate = release_gate
        self.calls = []

    def request(self, url, method='GET', headers=None, body=None):
        result = self._client.request(url, method=method, headers=headers, body=body)
        self.calls.append(dict(path='/' + url.split('/', 3)[-1],
                               headers=dict(headers or {}),
                               status=result[0]))
        return result

    def release_gate(self, name):
        self._release_gate(name)


@contextmanager
def _source(network, routes):
    if network:
        with replay_server(routes) as (base, calls, release_gate):
            yield NetworkClient(Client(), release_gate), base, calls
    else:
        client = ScriptedClient(routes)
        yield client, '', client.calls


def _preload_previous_run(store, works):
    """模拟上一轮进程已经入库的发现结果（用于验证 304 沿用与重启恢复）。"""
    gen = store.next_gen()
    store.ensure_node('catalog:/index', KIND_CATALOG, url='/index', gen=gen)
    store.save_product('catalog:/index', '"catalog"', gen,
                       'application/json', b'previous catalog')
    store.set_node_state('catalog:/index', SUCCEEDED)
    for work in works:
        work_rev = f"{work['id']}@{work['revision']}"
        store.ensure_node(work_rev, KIND_WORK, gen=gen)
        detail_id = 'detail:' + work['detail']
        store.ensure_node(detail_id, KIND_DETAIL, url=work['detail'],
                          detail_for=work_rev, gen=gen)
        store.add_edge(work_rev, work_rev, detail_id, REQUIRED, gen)
        store.subscribe(work_rev, detail_id)
        store.save_product(detail_id, f'"{work["detail"].strip("/")}"', gen,
                           'application/json', b'previous detail discovery')
        for url in work.get('required', []):
            asset_id = 'asset:' + url
            store.ensure_node(asset_id, KIND_ASSET, url=url, gen=gen)
            store.add_edge(work_rev, detail_id, asset_id, REQUIRED, gen)
            store.subscribe(work_rev, asset_id)
        for url in work.get('optional', []):
            asset_id = 'asset:' + url
            store.ensure_node(asset_id, KIND_ASSET, url=url, gen=gen)
            store.add_edge(work_rev, detail_id, asset_id, OPTIONAL, gen)
            store.subscribe(work_rev, asset_id)
    store.journal('preloaded', gen, dict(works=[w['id'] for w in works]))


def run_demo(fixture_path='fixtures/graph.json', db_path=None, network=False,
             verbose=True):
    fixture = json.loads(Path(fixture_path).read_text(encoding='utf-8'))
    works_v1 = fixture['works']
    update = next(e for e in fixture['events'] if e['type'] == 'detail_update')
    works_v2 = [w for w in works_v1 if w['id'] != update['work']] + [
        dict(id=update['work'], revision=update['revision'],
             detail='/d2', required=update['required'])]
    routes = _routes(works_v1, works_v2)

    temporary = None
    if db_path is None:
        temporary = tempfile.TemporaryDirectory()
        db_path = Path(temporary.name) / 'graph.sqlite3'
    else:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    try:
        with _source(network, routes) as (client, base, calls):
            store = GraphStore(db_path)
            _preload_previous_run(store, works_v1)

            # ---- 第一阶段：启动、304 沿用、共享附件反复失败、取消 r1 ----
            # 数据库已含上一轮进程的发现结果，因此本次启动属于“恢复”
            engine = Engine(store, client, base_url=base, workers=6,
                            max_attempts=5, retry_interval=0.01, recovery=True)
            engine.start()
            # 等待可选附件 /o1 的请求进入在途（它被门闩阻塞，不会提前完成）
            engine.wait_until(
                lambda: store.get_node('asset:/o1')['state'] == 'running', timeout=5)
            engine.wait_until(
                lambda: store.get_node('asset:/shared')['attempts'] >= 3, timeout=10)
            requests_at_cancel = len(calls)

            # 事件：cancel r1（只取消 r1 的订阅，/shared 仍被 r2 订阅）
            engine.cancel_work('r1@1')
            # 此时 /o1 确定仍在途；放行后它应在成功落库前的守卫处停为 canceled
            client.release_gate('after_cancel')
            engine.wait_until(
                lambda: store.get_node('asset:/o1')['state'] == CANCELED, timeout=5)
            # /shared 仍被 r2 订阅：取消 r1 后它必须继续重试，而不是被一起停掉
            shared_after_cancel = store.get_node('asset:/shared')
            assert shared_after_cancel['state'] != CANCELED

            # 事件：目录更新，r2@2 出现并替代 r2@1
            engine.poll_catalog('catalog:/index')
            engine.wait_until(
                lambda: store.get_node('r2@2') is not None, timeout=5)
            engine.wait_until(
                lambda: store.get_node('r2@1')['state'] == 'superseded', timeout=5)
            engine.wait_until(
                lambda: store.get_node('asset:/new')['state'] == SUCCEEDED, timeout=10)
            engine.wait_idle(timeout=10)

            snapshot_one = {n['node_id']: dict(state=n['state'],
                                               attempts=n['attempts'])
                            for n in store.list_nodes()}
            requests_one = store.request_counts()
            engine.shutdown(wait=True)

            # ---- 第二阶段：模拟进程重启，从 SQLite 恢复调度 ----
            # 重启只对目录/详情做一次条件再验证；已成功的附件（如 /new）不重发
            engine = Engine(store, client, base_url=base, workers=6,
                            max_attempts=5, retry_interval=0.01, recovery=True)
            engine.start()
            engine.wait_idle(timeout=10)
            engine.shutdown(wait=True)
            requests_after_restart = len(calls)

            # 事件：old_asset_complete —— 晚到的旧代次附件（revision 1）
            old_event = next(e for e in fixture['events']
                             if e['type'] == 'old_asset_complete')
            # 该引擎不再发起任何请求，只演示旧产物入库与代次甄别
            engine = Engine(store, client, base_url=base, workers=2,
                            max_attempts=5, retry_interval=0.01,
                            validate_details=False)
            engine.ingest_late_product(
                'asset:/shared', gen=old_event['revision'],
                payload=b'late shared payload', etag='"shared-old"')
            engine.shutdown(wait=True)
            assert len(calls) == requests_after_restart, '晚期产物处理不得触发请求'

            report = _build_report(store, calls, snapshot_one, requests_one,
                                   requests_at_cancel)
            store.close()
        if verbose:
            print(json.dumps(report, ensure_ascii=False, indent=2))
        return report
    finally:
        if temporary is not None:
            temporary.cleanup()


def _build_report(store, calls, snapshot_one, requests_one, requests_at_cancel):
    summary = {}
    for call in calls:
        summary.setdefault(call['path'], []).append(call)
    request_table = []
    for path in sorted(summary):
        request_table.append(dict(
            path=path,
            total_requests=len(summary[path]),
            statuses=[c.get('status') for c in summary[path]],
            conditional=sum(1 for c in summary[path]
                            if c['headers'].get('If-None-Match')),
        ))
    return dict(
        requests=request_table,
        requests_at_cancel=requests_at_cancel,
        first_run_request_counts=dict(sorted(requests_one.items())),
        first_run_node_states=snapshot_one,
        final_works=store.list_works(),
        journals=store.list_journals(),
        request_log=store.list_requests(),
    )
