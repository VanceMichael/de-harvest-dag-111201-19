"""动态依赖图的持久化层。

所有图结构（节点、依赖边、订阅者）、节点产物与运行进度都存放在 SQLite 中，
进程重启后可以仅凭数据库恢复调度，已成功的节点不会重复请求。

表结构要点：
- nodes        图节点（目录、详情、附件、成果修订），节点按 URL/标识全局共享
- edges        依赖边，带 frozen_gen：成果修订在自己的修订代次冻结依赖集合
- subscriptions 订阅关系，带引用计数；共享节点的多个订阅者各自独立取消
- products     节点产物（含来源版本标识，用于 304 条件请求与旧代次甄别）
- requests     实际发出的 HTTP 请求流水，用于核对“哪些请求真正重发”
- journals     领域事件流水，记录取消、替代、晚期产物等关键决策
"""

import json
import sqlite3
import threading
from contextlib import contextmanager

# 节点种类
KIND_CATALOG = 'catalog'
KIND_DETAIL = 'detail'
KIND_ASSET = 'asset'
KIND_WORK = 'work'

# 节点运行状态
PENDING = 'pending'
RUNNING = 'running'
SUCCEEDED = 'succeeded'
FAILED = 'failed'
CANCELED = 'canceled'

# 附件要求级别
REQUIRED = 'required'
OPTIONAL = 'optional'

# 成果修订对外可见状态
ST_COLLECTING = '采集中'
ST_CORE_INCOMPLETE = '核心资料不齐'
ST_AVAILABLE_PARTIAL = '可用但缺可选项'
ST_COMPLETE = '完整'
ST_CANCELED = '已取消'
ST_SUPERSEDED = '已被新修订替代'


class CycleError(ValueError):
    """新增依赖会在图中形成环。"""


class GraphStore:
    def __init__(self, path, external_lock=None):
        self.connection = sqlite3.connect(path, timeout=15, isolation_level=None,
                                          check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        # 显式事务标志：autocommit 模式下 BEGIN 之前 in_transaction 仍为 False，
        # 不能依赖连接属性判断嵌套，必须自己维护
        self._in_tx = False
        # 可复用引擎的图锁：所有读写与引擎状态变更在同一把锁上串行，
        # 杜绝 worker 线程的 subscribe 与外部 cancel 的读计数交错
        self._tx_lock = external_lock or threading.RLock()
        self.connection.execute('PRAGMA foreign_keys=ON')
        self.connection.execute('PRAGMA journal_mode=WAL')
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS nodes(
                node_id     TEXT PRIMARY KEY,
                kind        TEXT NOT NULL,
                url         TEXT,
                state       TEXT NOT NULL DEFAULT 'pending',
                attempts    INTEGER NOT NULL DEFAULT 0,
                last_error  TEXT,
                detail_for  TEXT,
                created_gen INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS edges(
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                work_rev     TEXT NOT NULL,
                from_node    TEXT NOT NULL REFERENCES nodes(node_id),
                to_node      TEXT NOT NULL REFERENCES nodes(node_id),
                requirement  TEXT NOT NULL,
                frozen_gen   INTEGER NOT NULL,
                UNIQUE(work_rev, to_node));
            CREATE TABLE IF NOT EXISTS subscriptions(
                work_rev   TEXT NOT NULL,
                node_id    TEXT NOT NULL REFERENCES nodes(node_id),
                refcount   INTEGER NOT NULL DEFAULT 1,
                canceled   INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(work_rev, node_id));
            CREATE TABLE IF NOT EXISTS products(
                node_id    TEXT PRIMARY KEY REFERENCES nodes(node_id),
                etag       TEXT,
                gen        INTEGER NOT NULL,
                media_type TEXT,
                payload    BLOB,
                produced_at TEXT NOT NULL DEFAULT (datetime('now')));
            CREATE TABLE IF NOT EXISTS requests(
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id    TEXT NOT NULL,
                method     TEXT NOT NULL,
                url        TEXT NOT NULL,
                status     INTEGER,
                attempt    INTEGER NOT NULL,
                sent_at    TEXT NOT NULL DEFAULT (datetime('now')));
            CREATE TABLE IF NOT EXISTS journals(
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                kind    TEXT NOT NULL,
                gen     INTEGER NOT NULL DEFAULT 0,
                payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS meta(
                key TEXT PRIMARY KEY, value TEXT NOT NULL);
            """
        )

    # ------------------------------------------------------------------ 事务
    @contextmanager
    def transaction(self):
        # 同一把可重入锁串行化所有写事务；引擎常在图锁内连续调用多个写方法，
        # 外层已开启事务时内层直接复用，由最外层统一提交或回滚。
        with self._tx_lock:
            if self._in_tx:
                yield
                return
            self.connection.execute('BEGIN IMMEDIATE')
            self._in_tx = True
            try:
                yield
            except BaseException:
                self.connection.rollback()
                self._in_tx = False
                raise
            else:
                self.connection.commit()
                self._in_tx = False

    def next_gen(self):
        with self.transaction():
            row = self.connection.execute(
                "SELECT value FROM meta WHERE key='gen'").fetchone()
            gen = 1 if row is None else int(row['value']) + 1
            self.connection.execute(
                "INSERT INTO meta(key,value) VALUES('gen',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(gen),))
            return gen

    def journal(self, kind, gen, payload):
        with self.transaction():
            self.connection.execute(
                'INSERT INTO journals(kind,gen,payload) VALUES(?,?,?)',
                (kind, gen, json.dumps(payload, ensure_ascii=False, sort_keys=True)))

    def list_journals(self):
        return [dict(kind=row['kind'], gen=row['gen'],
                     payload=json.loads(row['payload']))
                for row in self.connection.execute(
                    'SELECT kind,gen,payload FROM journals ORDER BY id')]

    # ------------------------------------------------------------------ 节点
    def ensure_node(self, node_id, kind, url=None, detail_for=None, gen=0):
        with self.transaction():
            self.connection.execute(
                'INSERT INTO nodes(node_id,kind,url,detail_for,created_gen) '
                'VALUES(?,?,?,?,?) ON CONFLICT(node_id) DO NOTHING',
                (node_id, kind, url, detail_for, gen))
        return self.get_node(node_id)

    def get_node(self, node_id):
        with self._tx_lock:
            row = self.connection.execute(
                'SELECT * FROM nodes WHERE node_id=?', (node_id,)).fetchone()
            return dict(row) if row else None

    def list_nodes(self, kind=None):
        if kind is None:
            rows = self.connection.execute('SELECT * FROM nodes ORDER BY node_id')
        else:
            rows = self.connection.execute(
                'SELECT * FROM nodes WHERE kind=? ORDER BY node_id', (kind,))
        return [dict(row) for row in rows]

    def set_node_state(self, node_id, state, error=None):
        with self.transaction():
            self.connection.execute(
                'UPDATE nodes SET state=?, last_error=? WHERE node_id=?',
                (state, error, node_id))

    def mark_attempt(self, node_id, state, error=None):
        with self.transaction():
            self.connection.execute(
                'UPDATE nodes SET attempts=attempts+1, state=?, last_error=? '
                'WHERE node_id=?', (state, error, node_id))

    def save_product(self, node_id, etag, gen, media_type, payload):
        with self.transaction():
            self.connection.execute(
                'INSERT INTO products(node_id,etag,gen,media_type,payload) '
                'VALUES(?,?,?,?,?) ON CONFLICT(node_id) DO UPDATE SET '
                'etag=excluded.etag, gen=excluded.gen, '
                'media_type=excluded.media_type, payload=excluded.payload',
                (node_id, etag, gen, media_type, payload))

    def get_product(self, node_id):
        row = self.connection.execute(
            'SELECT * FROM products WHERE node_id=?', (node_id,)).fetchone()
        return dict(row) if row else None

    def log_request(self, node_id, method, url, status, attempt):
        with self.transaction():
            cur = self.connection.execute(
                'INSERT INTO requests(node_id,method,url,status,attempt) '
                'VALUES(?,?,?,?,?)', (node_id, method, url, status, attempt))
            return cur.lastrowid

    def list_requests(self):
        return [dict(row) for row in self.connection.execute(
            'SELECT node_id,method,url,status,attempt,sent_at FROM requests ORDER BY id')]

    def request_counts(self):
        return {row['node_id']: row['n'] for row in self.connection.execute(
            'SELECT node_id, count(*) AS n FROM requests GROUP BY node_id')}

    # ------------------------------------------------------------------ 边
    def _reaches(self, start, target):
        """图中从 start 沿依赖方向（to_node）是否能到达 target。"""
        seen = set()
        stack = [start]
        while stack:
            current = stack.pop()
            if current == target:
                return True
            seen.add(current)
            for row in self.connection.execute(
                    'SELECT to_node FROM edges WHERE from_node=?', (current,)):
                if row['to_node'] not in seen:
                    stack.append(row['to_node'])
        return False

    def add_edge(self, work_rev, from_node, to_node, requirement, frozen_gen):
        """新增依赖边，带防环检查。

        若 to_node 已经（传递地）依赖 from_node，再添加 from_node -> to_node
        就会形成环，此时拒绝并抛出 CycleError。
        """
        with self.transaction():
            if from_node == to_node or self._reaches(to_node, from_node):
                raise CycleError(
                    f'拒绝添加依赖 {from_node} -> {to_node}：会形成环')
            self.connection.execute(
                'INSERT INTO edges(work_rev,from_node,to_node,requirement,frozen_gen) '
                'VALUES(?,?,?,?,?) ON CONFLICT(work_rev,to_node) DO NOTHING',
                (work_rev, from_node, to_node, requirement, frozen_gen))

    def edges_from(self, node_id):
        return [dict(row) for row in self.connection.execute(
            'SELECT * FROM edges WHERE from_node=? ORDER BY id', (node_id,))]

    def edges_for_revision(self, work_rev):
        return [dict(row) for row in self.connection.execute(
            'SELECT * FROM edges WHERE work_rev=? ORDER BY id', (work_rev,))]

    # ------------------------------------------------------------------ 订阅
    def subscribe(self, work_rev, node_id):
        """登记订阅。同一成果对同一节点只计一次（幂等），重复发现不增加引用。"""
        with self.transaction():
            row = self.connection.execute(
                'SELECT refcount,canceled FROM subscriptions WHERE work_rev=? AND node_id=?',
                (work_rev, node_id)).fetchone()
            if row is None:
                self.connection.execute(
                    'INSERT INTO subscriptions(work_rev,node_id,refcount,canceled) '
                    'VALUES(?,?,1,0)', (work_rev, node_id))
            elif row['canceled']:
                # 曾被取消后重新订阅：恢复为单个活跃引用
                self.connection.execute(
                    'UPDATE subscriptions SET refcount=1, canceled=0 '
                    'WHERE work_rev=? AND node_id=?', (work_rev, node_id))

    def unsubscribe(self, work_rev, node_id):
        """取消一个成果对节点的订阅，返回节点剩余的活跃订阅数。"""
        with self.transaction():
            self.connection.execute(
                'UPDATE subscriptions SET canceled=1, refcount=0 '
                'WHERE work_rev=? AND node_id=? AND canceled=0', (work_rev, node_id))
        return self.active_subscriber_count(node_id)

    def active_subscriber_count(self, node_id):
        # 活跃订阅数直接统计未取消的订阅行，与 refcount 数值解耦，
        # 避免重复订阅/重新订阅过程中引用计数中间态造成误判
        with self._tx_lock:
            row = self.connection.execute(
                'SELECT COUNT(*) AS n FROM subscriptions '
                'WHERE node_id=? AND canceled=0', (node_id,)).fetchone()
            return row['n']

    def subscriptions_for(self, work_rev):
        return [dict(row) for row in self.connection.execute(
            'SELECT * FROM subscriptions WHERE work_rev=? ORDER BY node_id', (work_rev,))]

    def mark_work_superseded(self, old_rev):
        with self.transaction():
            self.connection.execute(
                "UPDATE nodes SET state='superseded' WHERE node_id=?", (old_rev,))
            self.connection.execute(
                'UPDATE subscriptions SET canceled=1 WHERE work_rev=?', (old_rev,))

    # ------------------------------------------------------------------ 成果视图
    def work_view(self, work_rev):
        """对外查看者需要的就绪视图：区分缺可选与核心不齐。"""
        node = self.get_node(work_rev)
        if node is None:
            return None
        edges = self.edges_for_revision(work_rev)
        missing_required, missing_optional, failed_required, failed_optional = [], [], [], []
        for edge in edges:
            dependency = self.get_node(edge['to_node'])
            state = dependency['state']
            if state == SUCCEEDED:
                continue
            item = dict(node=edge['to_node'], url=dependency['url'], state=state,
                        attempts=dependency['attempts'], error=dependency['last_error'])
            if edge['requirement'] == REQUIRED:
                if state == FAILED:
                    failed_required.append(item)
                else:
                    missing_required.append(item)
            elif state == FAILED:
                failed_optional.append(item)
            else:
                missing_optional.append(item)
        if node['state'] == CANCELED:
            status = ST_CANCELED
        elif node['state'] == 'superseded':
            status = ST_SUPERSEDED
        elif missing_required or failed_required:
            status = ST_COLLECTING if not failed_required else ST_CORE_INCOMPLETE
        elif missing_optional or failed_optional:
            status = ST_AVAILABLE_PARTIAL
        else:
            status = ST_COMPLETE
        return dict(work_rev=work_rev, state=node['state'], status=status,
                    delivered=bool(node.get('state') == SUCCEEDED),
                    missing_required=missing_required, failed_required=failed_required,
                    missing_optional=missing_optional, failed_optional=failed_optional)

    def list_works(self):
        return [self.work_view(row['node_id'])
                for row in self.connection.execute(
                    "SELECT node_id FROM nodes WHERE kind='work' ORDER BY node_id")]

    def close(self):
        self.connection.close()
