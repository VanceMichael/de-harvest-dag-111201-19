from collections import deque


def topological(nodes, dependencies):
    incoming = {node: set(dependencies.get(node, [])) for node in nodes}
    if any(parent not in incoming for values in incoming.values() for parent in values):
        raise ValueError('依赖节点不存在')
    ready = deque(sorted(node for node, parents in incoming.items() if not parents))
    result = []
    while ready:
        node = ready.popleft()
        result.append(node)
        for target in sorted(incoming):
            if node in incoming[target]:
                incoming[target].remove(node)
                if not incoming[target]:
                    ready.append(target)
    if len(result) != len(incoming):
        raise ValueError('依赖图存在环')
    return result


def collect(payload):
    order = topological(payload['nodes'], payload['dependencies'])
    return [dict(key=name, position=position, resource=payload['nodes'][name]) for position, name in enumerate(order)]
