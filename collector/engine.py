"""可恢复的动态依赖图采集引擎。

节点种类：目录(catalog) -> 成果修订(work) -> 详情(detail) -> 附件(asset)。
附件集合只有在解析详情之后才知道，因此图在运行中动态生长，每条新边都经过防环检查。

关键语义：
- 每个成果修订在自己的修订代次（gen）冻结依赖集合；晚到的旧代次附件产物
  只落库记录，不能把新修订标记为完整。
- 详情返回 304 时沿用上次发现的依赖（边已经持久化在库中），不会被当成“没有附件”。
- 失败重试只重跑失败节点本身；其成功后继不受影响，恢复时未完成的必要后继才会继续。
- 共享附件节点被多个成果订阅，取消一个成果只减少引用计数，全部订阅者取消后才停。
- 所有“节点完成 + 新增依赖 + 交付判定”都在同一把图锁下串行提交，
  worker 完成节点与另一路新增依赖并发时，不会出现提前交付。
- 节点产物、依赖、订阅、请求流水、运行进度全部持久化，重建 Engine 即可恢复。

调度模型：单一调度线程维护就绪队列与退避重试堆，只负责把可运行节点派发给
worker 线程池；worker 不再向线程池嵌套提交任务，因此不会出现池饥饿死锁。
"""

import heapq
import json
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

from .graphstore import (
    CANCELED, FAILED, KIND_ASSET, KIND_CATALOG, KIND_DETAIL, KIND_WORK,
    OPTIONAL, PENDING, REQUIRED, RUNNING, SUCCEEDED, CycleError, GraphStore,
)


class Engine:
    def __init__(self, store: GraphStore, client, base_url='', workers=4,
                 max_attempts=5, retry_interval=0.05, validate_details=True,
                 recovery=False):
        self.store = store
        self.client = client
        self.base_url = base_url
        self.max_attempts = max_attempts
        self.retry_interval = retry_interval
        self.validate_details = validate_details
        self.recovery = recovery
        # 存储层与引擎共用同一把可重入锁：图结构变更、订阅计数、状态读取
        # 全部在这把锁上串行，worker 与外部 cancel 不可能读到交错的中间态
        self._lock = threading.RLock()
        if getattr(store, '_tx_lock', None) is None:
            store._tx_lock = self._lock
        else:
            self._lock = store._tx_lock
        self._cond = threading.Condition(self._lock)
        self._ready = deque()           # 等待派发的节点
        self._ready_set = set()
        self._inflight = set()          # 正在被 worker 执行的节点
        self._retry_heap = []           # (最早可执行时间, 序号, 节点)
        self._retry_nodes = set()
        self._delivered = set()         # 已交付的成果修订
        self._stop = False
        self._workers = workers
        self._pool = None
        self._scheduler = None
        self._seq = 0
        self._change_listeners = []

    # ------------------------------------------------------------ 事件监听
    def on_change(self, listener):
        with self._lock:
            self._change_listeners.append(listener)

    def _emit(self, event):
        self._cond.notify_all()
        for listener in list(self._change_listeners):
            listener(event)

    # ------------------------------------------------------------ 生命周期
    def start(self):
        """启动（或恢复）调度：把库中所有仍可运行的节点重新入队。"""
        with self._lock:
            self._stop = False
            self._pool = ThreadPoolExecutor(
                max_workers=self._workers, thread_name_prefix='dag-worker')
            self._scheduler = threading.Thread(
                target=self._schedule_loop, name='dag-scheduler', daemon=True)
            for node in self.store.list_nodes():
                if node['kind'] == KIND_WORK:
                    if node['state'] not in (CANCELED, 'superseded', SUCCEEDED):
                        self._recompute_work_locked(node['node_id'])
                    continue
                # 上次崩溃时停在 RUNNING 的节点视为中断，回退为 PENDING 后重新派发，
                # 避免与任何在途执行重复
                if node['state'] == RUNNING:
                    self.store.set_node_state(node['node_id'], PENDING)
                    node = self.store.get_node(node['node_id'])
                if node['state'] in (PENDING, FAILED):
                    self._enqueue_locked(node['node_id'])
                # 仅在进程恢复时，对已成功的目录/详情做一次条件再验证
                # （If-None-Match/304）；全新运行不重复验证成功节点
                if (self.recovery and self.validate_details
                        and node['kind'] in (KIND_DETAIL, KIND_CATALOG)
                        and node['state'] == SUCCEEDED):
                    self._enqueue_locked(node['node_id'])
            self._scheduler.start()
            self._cond.notify_all()

    def shutdown(self, wait=True):
        with self._lock:
            self._stop = True
            self._cond.notify_all()
        if self._pool is not None:
            self._pool.shutdown(wait=wait, cancel_futures=True)
        if self._scheduler is not None:
            self._scheduler.join(timeout=2)

    def wait_idle(self, timeout=10.0):
        """等待图进入静止：没有排队中、执行中、退避重试中的节点。"""
        deadline = time.monotonic() + timeout
        with self._cond:
            while self._ready or self._inflight or self._retry_heap:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('引擎未在限定时间内静止')
                self._cond.wait(min(0.1, remaining))

    def wait_until(self, predicate, timeout=10.0):
        deadline = time.monotonic() + timeout
        with self._cond:
            while not predicate():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('等待条件未满足')
                self._cond.wait(min(0.1, remaining))

    # ------------------------------------------------------------ 调度线程
    def _schedule_loop(self):
        while True:
            with self._lock:
                if self._stop:
                    return
                now = time.monotonic()
                # 到期的重试节点回到就绪队列
                while self._retry_heap and self._retry_heap[0][0] <= now:
                    _, _, node_id = heapq.heappop(self._retry_heap)
                    self._retry_nodes.discard(node_id)
                    self._enqueue_locked(node_id)
                delay = None
                if self._retry_heap:
                    delay = max(0.0, self._retry_heap[0][0] - now)
                # 有空闲 worker 且有就绪节点时派发
                available = self._workers - len(self._inflight)
                while available > 0 and self._ready:
                    node_id = self._ready.popleft()
                    self._ready_set.discard(node_id)
                    if node_id in self._inflight:
                        continue
                    node = self._claim_node_locked(node_id)
                    if node is None:
                        continue
                    self._inflight.add(node_id)
                    self.store.set_node_state(node_id, RUNNING)
                    available -= 1
                    self._pool.submit(self._worker, node_id)
                if not self._ready and not self._inflight and not self._retry_heap:
                    self._cond.notify_all()
                self._cond.wait(timeout=min(delay, 0.1) if delay is not None else 0.1)

    def _enqueue_locked(self, node_id):
        node = self.store.get_node(node_id)
        if node is None or node['state'] in (CANCELED, 'superseded'):
            return
        if (node_id in self._ready_set or node_id in self._inflight
                or node_id in self._retry_nodes):
            return
        # 控制节点（目录、详情）优先于附件，避免附件重试占满 worker 时
        # 目录更新这类关键动作被饿死
        if node['kind'] in (KIND_CATALOG, KIND_DETAIL):
            self._ready.appendleft(node_id)
        else:
            self._ready.append(node_id)
        self._ready_set.add(node_id)
        self._cond.notify_all()

    def _claim_node_locked(self, node_id):
        """取出排队节点前的统一守卫：已取消/被替代/已成功，或失去全部订阅者则不执行。"""
        node = self.store.get_node(node_id)
        if node is None or node['state'] in (CANCELED, 'superseded'):
            return None
        # 恢复再验证，或显式目录轮询，都允许已成功的目录/详情重新执行
        is_revalidation = (
            (self.recovery and node['kind'] in (KIND_DETAIL, KIND_CATALOG))
            or node['kind'] == KIND_CATALOG)
        if node['state'] == SUCCEEDED and not is_revalidation:
            return None
        if node['kind'] in (KIND_ASSET, KIND_DETAIL):
            if self.store.active_subscriber_count(node_id) == 0:
                if node['state'] != SUCCEEDED:
                    self.store.set_node_state(node_id, CANCELED)
                    self.store.journal('shared_node_stopped', 0,
                                       dict(node=node_id, reason='无活跃订阅者'))
                return None
        return node

    def _worker(self, node_id):
        try:
            self._execute(node_id)
        except Exception as error:  # 防御性兜底：异常也要落库，不能丢节点
            with self._lock:
                self.store.mark_attempt(node_id, FAILED, repr(error))
                self._emit(dict(node=node_id, event='error', error=repr(error)))
        finally:
            with self._lock:
                self._inflight.discard(node_id)
                self._cond.notify_all()

    # ------------------------------------------------------------ 节点执行
    def _execute(self, node_id):
        node = self.store.get_node(node_id)
        kind = node['kind']
        if kind == KIND_CATALOG:
            self._run_catalog(node_id, node)
        elif kind == KIND_DETAIL:
            self._run_detail(node_id, node)
        elif kind == KIND_ASSET:
            self._run_asset(node_id, node)
        # work 节点不发起请求，状态由依赖聚合得到

    def _do_request(self, node_id, url, etag):
        headers = {'If-None-Match': etag} if etag else {}
        attempt = self.store.get_node(node_id)['attempts'] + 1
        status, response_headers, body = self.client.request(
            self.base_url + url, headers=headers)
        self.store.log_request(node_id, 'GET', url, status, attempt)
        return status, response_headers, body, attempt

    def _run_catalog(self, node_id, node):
        previous = self.store.get_product(node_id)
        status, headers, body, attempt = self._do_request(
            node_id, node['url'], previous['etag'] if previous else None)
        if status == 304:
            # 目录未变化：不解析新内容，已登记的成果修订与冻结依赖保持不变
            with self._lock:
                self._finalize_success_locked(node_id)
                self.store.journal('catalog_not_modified',
                                   previous['gen'] if previous else 0,
                                   dict(node=node_id))
                self._emit(dict(node=node_id, event='catalog_304'))
            return
        if status == 200:
            gen = self.store.next_gen()
            self.store.save_product(node_id, headers.get('ETag'), gen,
                                    headers.get('Content-Type'), body)
            with self._lock:
                self.store.mark_attempt(node_id, SUCCEEDED)
                self._apply_catalog_locked(node_id, json.loads(body.decode('utf-8')))
                self._emit(dict(node=node_id, event='catalog_ok', gen=gen))
        else:
            self._record_failure(node_id, attempt, status, body)

    def _apply_catalog_locked(self, catalog_id, payload):
        """解析目录并登记成果修订；按修订号幂等，新修订替代旧修订。"""
        for item in payload.get('works', []):
            work_id, revision = item['id'], int(item['revision'])
            work_rev = f'{work_id}@{revision}'
            gen = self.store.next_gen()
            already = self.store.get_node(work_rev)
            if already is None:
                self.store.ensure_node(work_rev, KIND_WORK, url=None, gen=gen)
                # 新修订第一次出现：在当前代次冻结它自己的依赖集合
                self._freeze_revision_locked(work_rev, item, gen)
                self.store.journal('revision_discovered', gen,
                                   dict(work_rev=work_rev, source='catalog'))
            # 同一成果的旧修订全部标记为被替代，并撤掉它们的订阅
            for older in self.store.list_nodes(kind=KIND_WORK):
                if (older['node_id'].startswith(work_id + '@')
                        and older['node_id'] != work_rev
                        and older['state'] not in (CANCELED, 'superseded')):
                    self.store.mark_work_superseded(older['node_id'])
                    self._retire_orphans_locked(older['node_id'])
                    self.store.journal('revision_superseded', gen,
                                       dict(work_rev=older['node_id'],
                                            successor=work_rev))

    def _freeze_revision_locked(self, work_rev, item, gen):
        detail_id = 'detail:' + item['detail']
        self.store.ensure_node(detail_id, KIND_DETAIL, url=item['detail'],
                               detail_for=work_rev, gen=gen)
        self.store.add_edge(work_rev, work_rev, detail_id, REQUIRED, gen)
        self.store.subscribe(work_rev, detail_id)
        for url in item.get('required', []):
            asset_id = 'asset:' + url
            self.store.ensure_node(asset_id, KIND_ASSET, url=url, gen=gen)
            self.store.add_edge(work_rev, detail_id, asset_id, REQUIRED, gen)
            self.store.subscribe(work_rev, asset_id)
        for url in item.get('optional', []):
            asset_id = 'asset:' + url
            self.store.ensure_node(asset_id, KIND_ASSET, url=url, gen=gen)
            self.store.add_edge(work_rev, detail_id, asset_id, OPTIONAL, gen)
            self.store.subscribe(work_rev, asset_id)
        for dependency in (detail_id, *(f'asset:{u}' for u in item.get('required', [])),
                           *(f'asset:{u}' for u in item.get('optional', []))):
            self._enqueue_locked(dependency)
        self._recompute_work_locked(work_rev)

    def _run_detail(self, node_id, node):
        previous = self.store.get_product(node_id)
        etag = previous['etag'] if previous else None
        status, headers, body, attempt = self._do_request(
            node_id, node['url'], etag)
        if status == 304:
            # 详情未变化：沿用之前发现并已持久化的依赖，绝不视为“没有附件”
            with self._lock:
                still_active = self._finalize_success_locked(node_id)
                self.store.journal('detail_not_modified',
                                   previous['gen'] if previous else 0,
                                   dict(node=node_id))
                self._emit(dict(node=node_id, event='detail_304'))
                if still_active:
                    # 304 沿用已冻结依赖：只推进尚未完成的附件，
                    # 已成功的附件不重复入队、不重复请求
                    for edge in self.store.edges_from(node_id):
                        dependency = self.store.get_node(edge['to_node'])
                        if dependency['state'] in (PENDING, FAILED, RUNNING):
                            self._enqueue_locked(edge['to_node'])
                    for work_rev in self._subscribing_works_locked(node_id):
                        self._recompute_work_locked(work_rev)
            return
        if status == 200:
            gen = self.store.next_gen()
            self.store.save_product(node_id, headers.get('ETag'), gen,
                                    headers.get('Content-Type'), body)
            with self._lock:
                still_active = self._finalize_success_locked(node_id)
                discovered = json.loads(body.decode('utf-8'))
                if still_active:
                    self._apply_detail_locked(node_id, discovered, gen)
                self._emit(dict(node=node_id, event='detail_ok', gen=gen))
            return
        self._record_failure(node_id, attempt, status, body)

    def _apply_detail_locked(self, detail_id, discovered, gen):
        """动态发现附件：新节点即时入图、入队，边经过防环检查。

        只服务于仍活跃（未取消、未被新修订替代）的成果修订；
        已终结的修订保持其冻结快照，不再追加新边。
        """
        for work_rev in self._subscribing_works_locked(detail_id):
            work_node = self.store.get_node(work_rev)
            # 已取消或被新修订替代的成果保持冻结快照，不再接收新发现的边
            if work_node is None or work_node['state'] in (CANCELED, 'superseded'):
                continue
            for requirement, urls in ((REQUIRED, discovered.get('required', [])),
                                      (OPTIONAL, discovered.get('optional', []))):
                for url in urls:
                    asset_id = 'asset:' + url
                    self.store.ensure_node(asset_id, KIND_ASSET, url=url, gen=gen)
                    try:
                        self.store.add_edge(work_rev, detail_id, asset_id,
                                            requirement, gen)
                    except CycleError:
                        self.store.journal('cycle_rejected', gen,
                                           dict(from_node=detail_id, to_node=asset_id))
                        continue
                    self.store.subscribe(work_rev, asset_id)
                    # 已成功的附件不重复入队、不重复请求；只有未完成节点才调度
                    if self.store.get_node(asset_id)['state'] in (
                            PENDING, FAILED, RUNNING):
                        self._enqueue_locked(asset_id)
            self._recompute_work_locked(work_rev)

    def _subscribing_works_locked(self, node_id):
        return sorted({row['work_rev'] for row in
                       self.store.connection.execute(
                           'SELECT work_rev FROM subscriptions WHERE node_id=?',
                           (node_id,))})

    def _run_asset(self, node_id, node):
        previous = self.store.get_product(node_id)
        status, headers, body, attempt = self._do_request(
            node_id, node['url'], previous['etag'] if previous else None)
        if status in (200, 304):
            with self._lock:
                # 先做取消守卫：在途请求返回时订阅者可能已经全部撤订
                if not self._finalize_success_locked(node_id):
                    # 产物仍可留档，但节点不标成功、不触发任何成果交付
                    if status == 200:
                        self.store.save_product(
                            node_id, headers.get('ETag'), self.store.next_gen(),
                            headers.get('Content-Type'), body)
                    self._emit(dict(node=node_id, event='asset_canceled_inflight',
                                    status=status))
                    return
                if status == 200:
                    gen = self.store.next_gen()
                    self.store.save_product(node_id, headers.get('ETag'), gen,
                                            headers.get('Content-Type'), body)
                for work_rev in self._subscribing_works_locked(node_id):
                    self._recompute_work_locked(work_rev)
                self._emit(dict(node=node_id, event='asset_ok', status=status))
        else:
            self._record_failure(node_id, attempt, status, body)

    def _record_failure(self, node_id, attempt, status, body):
        message = f'HTTP {status}'
        with self._lock:
            node = self.store.get_node(node_id)
            # 在途请求失败返回时订阅者可能已全部撤订：状态已取消，或没有任何
            # 活跃订阅者，都必须保持取消，不能被失败重试复活
            if (node['state'] == CANCELED
                    or (node['kind'] in (KIND_ASSET, KIND_DETAIL)
                        and self.store.active_subscriber_count(node_id) == 0)):
                if node['state'] != CANCELED:
                    self.store.set_node_state(node_id, CANCELED)
                # 撤订后可能仍有退避中的重试，一并清除，杜绝取消后复活
                self._retry_heap[:] = [item for item in self._retry_heap
                                       if item[2] != node_id]
                self._retry_nodes.discard(node_id)
                self._emit(dict(node=node_id, event='failure_after_cancel'))
                return
            if attempt >= self.max_attempts:
                self.store.mark_attempt(node_id, FAILED, message)
                self.store.journal('node_failed', 0,
                                   dict(node=node_id, attempts=attempt, error=message))
                for work_rev in self._subscribing_works_locked(node_id):
                    self._recompute_work_locked(work_rev)
                self._emit(dict(node=node_id, event='failed', attempts=attempt))
            else:
                # 只重跑这一个节点，成功的后继不会被重跑
                self.store.mark_attempt(node_id, PENDING, message)
                self._retry_nodes.add(node_id)
                self._seq += 1
                heapq.heappush(self._retry_heap,
                               (time.monotonic() + self.retry_interval,
                                self._seq, node_id))
                self._emit(dict(node=node_id, event='retry_scheduled',
                                attempt=attempt))

    # ------------------------------------------------------------ 成果聚合
    def _recompute_work_locked(self, work_rev):
        """在图锁内重算成果修订状态，并只在依赖真正齐备时交付。

        与动态加边共用同一把锁：若另一个线程正在为该成果添加新依赖，
        它要么在本方法之前完成（新边已计入），要么在之后执行（交付状态会被重算），
        因此不可能在依赖未冻结齐备时提前交付。
        """
        view = self.store.work_view(work_rev)
        if view is None:
            return
        if view['state'] in (CANCELED, 'superseded'):
            return
        if view['status'] in ('完整', '可用但缺可选项'):
            node = self.store.get_node(work_rev)
            if node['state'] != SUCCEEDED:
                self.store.set_node_state(work_rev, SUCCEEDED)
                self._delivered.add(work_rev)
                self.store.journal('delivered', self.store.next_gen(),
                                   dict(work_rev=work_rev, status=view['status']))
                self._emit(dict(node=work_rev, event='delivered',
                                status=view['status']))
        else:
            node = self.store.get_node(work_rev)
            if node['state'] == SUCCEEDED and work_rev not in self._delivered:
                self.store.set_node_state(work_rev, PENDING)

    # ------------------------------------------------------------ 外部操作
    def _retire_orphans_locked(self, work_rev):
        """某个成果撤订后，把已无任何活跃订阅者的共享节点停下来（产物保留）。"""
        for sub in self.store.subscriptions_for(work_rev):
            node_id = sub['node_id']
            if self.store.active_subscriber_count(node_id) == 0:
                node = self.store.get_node(node_id)
                # RUNNING 的在途请求由 _finalize_success_locked / 失败路径兜底，
                # 不会因为响应返回而把已取消节点复活
                if node['state'] in (PENDING, FAILED, RUNNING):
                    self.store.set_node_state(node_id, CANCELED)
                    self.store.journal('shared_node_stopped', 0,
                                       dict(node=node_id, reason='无活跃订阅者'))
                # 退避堆中尚未触发的重试与就绪队列都一并撤销
                self._retry_heap[:] = [item for item in self._retry_heap
                                       if item[2] != node_id]
                self._retry_nodes.discard(node_id)
                self._ready = deque(n for n in self._ready if n != node_id)
                self._ready_set.discard(node_id)

    def _finalize_success_locked(self, node_id):
        """成功落库前再确认订阅关系；执行期间被全部取消则保留产物但不标成功。"""
        node = self.store.get_node(node_id)
        if node['state'] == CANCELED:
            return False
        if (node['kind'] in (KIND_ASSET, KIND_DETAIL)
                and self.store.active_subscriber_count(node_id) == 0):
            self.store.set_node_state(node_id, CANCELED)
            self.store.journal('shared_node_stopped', 0,
                               dict(node=node_id, reason='完成时已无订阅者'))
            return False
        self.store.mark_attempt(node_id, SUCCEEDED)
        return True

    def cancel_work(self, work_rev):
        """取消一个成果：只撤它自己的订阅；共享节点在所有订阅者取消后才停。"""
        with self._lock:
            node = self.store.get_node(work_rev)
            if node is None or node['state'] in (CANCELED, 'superseded'):
                return
            self.store.set_node_state(work_rev, CANCELED)
            for sub in self.store.subscriptions_for(work_rev):
                self.store.unsubscribe(work_rev, sub['node_id'])
            self._retire_orphans_locked(work_rev)
            self.store.journal('work_canceled', 0, dict(work_rev=work_rev))
            self._emit(dict(node=work_rev, event='canceled'))

    def poll_catalog(self, catalog_id):
        """再次拉取目录，发现新修订（对应演示中的 detail_update 事件）。

        目录轮询是显式外部动作，即使目录此前已成功也要重新入队；
        派发守卫中的恢复再验证规则同样放行目录节点。
        """
        with self._lock:
            node = self.store.get_node(catalog_id)
            if node is None or node['state'] in (CANCELED, 'superseded'):
                return
            if (catalog_id not in self._ready_set
                    and catalog_id not in self._inflight
                    and catalog_id not in self._retry_nodes):
                self._ready.appendleft(catalog_id)
                self._ready_set.add(catalog_id)
                self._cond.notify_all()

    def ingest_late_product(self, asset_id, gen, payload=b'late', etag=None):
        """接收“晚到的旧附件”：产物只记录在它所属的旧代次，不触发新修订交付。"""
        with self._lock:
            node = self.store.get_node(asset_id)
            if node is None:
                raise KeyError(asset_id)
            self.store.save_product(asset_id, etag, gen, 'application/octet-stream',
                                    payload)
            self.store.journal('late_product', gen,
                               dict(node=asset_id, gen=gen))
            # 关键：不调用 _recompute_work_locked —— 旧代次产物不能把新成果标完整。
            # 若某成果在更晚的代次冻结了对该附件的依赖，旧产物对它不作数，只记审计事件。
            for edge in self.store.connection.execute(
                    'SELECT DISTINCT work_rev, frozen_gen FROM edges '
                    'WHERE to_node=? AND frozen_gen>?', (asset_id, gen)):
                self.store.journal('stale_product_ignored', gen,
                                   dict(node=asset_id, work_rev=edge['work_rev'],
                                        frozen_gen=edge['frozen_gen']))
            self._emit(dict(node=asset_id, event='late_product', gen=gen))
