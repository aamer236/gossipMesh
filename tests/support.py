"""Test orchestration: global knowledge is allowed only in this oracle."""

from chord import ChordNode, IdentifierSpace, InMemoryTransport, Peer


def make_ring(ids, m=4, *, sequential=False, list_size=4):
    space = IdentifierSpace(m)
    transport = InMemoryTransport(space)
    nodes = []
    for identifier in ids:
        node = ChordNode(
            Peer(identifier, f"node-{identifier}"),
            space,
            transport,
            successor_list_size=list_size,
        )
        transport.register(node)
        if nodes:
            node.join(nodes[0].peer)
        else:
            node.create()
        nodes.append(node)
        if sequential:
            converge(nodes)
    return nodes, transport


def expected_owner(nodes, key):
    ordered = sorted((node.peer for node in nodes), key=lambda p: p.id)
    return next((peer for peer in ordered if peer.id >= key), ordered[0])


def consistent(nodes, *, fingers=True, lists=True):
    ordered = sorted(nodes, key=lambda n: n.peer.id)
    for i, node in enumerate(ordered):
        state = node.state
        if state.successor != ordered[(i + 1) % len(nodes)].peer:
            return False
        if state.predecessor != ordered[(i - 1) % len(nodes)].peer:
            return False
        if fingers and any(
            finger.peer != expected_owner(nodes, finger.start)
            for finger in state.fingers
        ):
            return False
        if lists:
            count = min(4, max(1, len(nodes) - 1))
            expected = tuple(
                ordered[(i + offset) % len(nodes)].peer
                for offset in range(1, count + 1)
            )
            if state.successors != expected:
                return False
    return True


def converge(nodes, rounds=200, *, lists=True):
    for tick in range(rounds):
        for node in nodes:
            node.maintain()
        if consistent(nodes, lists=lists):
            return tick + 1
    raise AssertionError(
        f"ring did not converge in {rounds} rounds: {[n.state for n in nodes]}"
    )
