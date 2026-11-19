# FEWER FACES, THE SAME MAN: MakeHuman's body brought down to as many points
# as the court can draw quickly, by collapsing one edge at a time - always
# the one that changes his shape least (Garland and Heckbert's quadric
# error), keeping one end of it where it was (so every point left is one of
# MakeHuman's, its weights its own), never folding a face over, and keeping
# the edges of his mesh (round his eyes, his mouth) where they are.
import heapq
import numpy as np


def decimate(P, F, target, guard=None):
    """P: (n, 3) points; F: list of (a, b, c) faces; target: points to keep.
    guard(a, b): False where a may not be collapsed into b. Returns the
    faces left (indices into P - points no face uses are simply unused)."""
    n = len(P)
    F = np.array(F, np.int64)
    Q = np.zeros((n, 4, 4))
    a, b, c = P[F[:, 0]], P[F[:, 1]], P[F[:, 2]]
    nrm = np.cross(b - a, c - a)
    area = np.linalg.norm(nrm, axis=1)
    nn = nrm / np.maximum(area, 1e-12)[:, None]
    d = -np.einsum("ij,ij->i", nn, a)
    pl = np.concatenate([nn, d[:, None]], axis=1)
    K = np.einsum("fi,fj->fij", pl, pl) * area[:, None, None]
    for k in range(3):
        np.add.at(Q, F[:, k], K)
    faces = [list(t) for t in F.tolist()]
    alive = [True] * len(faces)
    vf = [set() for _ in range(n)]
    for fi, t in enumerate(faces):
        for v in t:
            vf[v].add(fi)
    # The edges of his mesh: held hard where they are.
    ecount = {}
    for t in faces:
        for i in range(3):
            e = (min(t[i], t[(i + 1) % 3]), max(t[i], t[(i + 1) % 3]))
            ecount[e] = ecount.get(e, 0) + 1
    big = np.mean(area) * 50
    edge = np.zeros(n, bool)
    for (u, v), k in ecount.items():
        if k == 1:
            edge[u] = edge[v] = True
            fi = next(iter(vf[u] & vf[v]))
            e = P[v] - P[u]
            m = np.cross(e, nn[fi])
            m /= max(np.linalg.norm(m), 1e-12)
            dd = -m.dot(P[u])
            pp = np.append(m, dd)
            KK = np.outer(pp, pp) * big
            Q[u] += KK
            Q[v] += KK
    ver = [0] * n
    gone = [False] * n

    def cost(u, v):
        # u into v.
        if guard is not None and not guard(u, v):
            return np.inf
        if edge[u] and not edge[v]:
            return np.inf
        h = np.append(P[v], 1.0)
        return float(h @ (Q[u] + Q[v]) @ h)

    heap = []

    def push(u, v):
        cu = cost(u, v)
        if cu < np.inf:
            heapq.heappush(heap, (cu, ver[u], ver[v], u, v))

    for (u, v) in ecount:
        push(u, v)
        push(v, u)
    left = len({v for t in faces for v in t})

    def flips(u, v):
        for fi in vf[u]:
            t = faces[fi]
            if v in t:
                continue
            old = [P[x] for x in t]
            new = [P[v] if x == u else P[x] for x in t]
            n0 = np.cross(old[1] - old[0], old[2] - old[0])
            n1 = np.cross(new[1] - new[0], new[2] - new[0])
            if n0.dot(n1) <= 1e-14 * (np.linalg.norm(n0) + 1e-30):
                return True
        return False

    while left > target and heap:
        cu, vu, vv, u, v = heapq.heappop(heap)
        if gone[u] or gone[v] or vu != ver[u] or vv != ver[v]:
            continue
        # (No two faces left sharing the same three points, nor a fold.)
        nu = {x for fi in vf[u] for x in faces[fi]} - {u}
        nv = {x for fi in vf[v] for x in faces[fi]} - {v}
        shared = vf[u] & vf[v]
        if len(nu & nv) != len(shared) or flips(u, v):
            continue
        for fi in shared:
            alive[fi] = False
            for x in faces[fi]:
                vf[x].discard(fi)
        for fi in list(vf[u]):
            faces[fi] = [v if x == u else x for x in faces[fi]]
            vf[v].add(fi)
        vf[u] = set()
        gone[u] = True
        Q[v] += Q[u]
        edge[v] = edge[v] or edge[u]
        left -= 1
        ver[v] += 1
        for x in {x for fi in vf[v] for x in faces[fi]} - {v}:
            push(v, x)
            push(x, v)
    return [tuple(faces[i]) for i in range(len(faces)) if alive[i]]
