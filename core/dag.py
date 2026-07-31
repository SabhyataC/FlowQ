from schemas.job import JobCreate


class DAGValidationError(Exception):
    """Raised when a submitted batch's dependency graph is invalid
    (missing batch_ref, unknown reference, or a cycle)."""


def build_dependency_graph(jobs: list[JobCreate]) -> dict[str, set[str]]:

    refs = [job.batch_ref for job in jobs]

    missing = [i for i, ref in enumerate(refs) if not ref]
    if missing:
        raise DAGValidationError(
            f"every job in a DAG batch needs a batch_ref; "
            f"missing at batch index(es) {missing}"
        )

    seen = set()
    duplicates = set()
    for ref in refs:
        if ref in seen:
            duplicates.add(ref)
        seen.add(ref)
    if duplicates:
        raise DAGValidationError(f"duplicate batch_ref(s) in batch: {sorted(duplicates)}")

    valid_refs = seen
    graph: dict[str, set[str]] = {ref: set() for ref in valid_refs}

    for job in jobs:
        for dep in job.depends_on:
            if dep not in valid_refs:
                raise DAGValidationError(
                    f"job '{job.batch_ref}' depends_on unknown batch_ref '{dep}' "
                    f"(not present in this batch)"
                )
            if dep == job.batch_ref:
                raise DAGValidationError(f"job '{job.batch_ref}' cannot depend on itself")
            graph[job.batch_ref].add(dep)

    return graph


def detect_cycle(graph: dict[str, set[str]]) -> list[str] | None:
    """
    DFS with a recursion stack. Returns the cycle as a list of
    batch_refs (in cycle order, first node repeated at the end) if
    one exists, else None.
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {node: WHITE for node in graph}
    path: list[str] = []

    def visit(node: str) -> list[str] | None:
        color[node] = GRAY
        path.append(node)

        for dep in graph[node]:
            if color[dep] == GRAY:
                # found a back-edge into the current path — extract the cycle
                cycle_start = path.index(dep)
                return path[cycle_start:] + [dep]
            if color[dep] == WHITE:
                result = visit(dep)
                if result is not None:
                    return result

        path.pop()
        color[node] = BLACK
        return None

    for node in graph:
        if color[node] == WHITE:
            cycle = visit(node)
            if cycle is not None:
                return cycle

    return None


def topological_sort(graph: dict[str, set[str]]) -> list[str]:
    cycle = detect_cycle(graph)
    if cycle is not None:
        raise DAGValidationError(f"dependency cycle detected: {' -> '.join(cycle)}")

    # remaining[node] counts *unresolved dependencies*, not incoming
    # edges in the traditional graph-theory sense — same thing here,
    # since graph[node] already IS "the things node depends on".
    remaining = {node: set(deps) for node, deps in graph.items()}
    order: list[str] = []

    ready = sorted(node for node, deps in remaining.items() if not deps)

    while ready:
        node = ready.pop(0)
        order.append(node)
        del remaining[node]

        newly_ready = []
        for other, deps in remaining.items():
            if node in deps:
                deps.discard(node)
                if not deps:
                    newly_ready.append(other)
        ready.extend(sorted(newly_ready))

    if remaining:
        raise DAGValidationError(
            f"topological sort could not resolve: {sorted(remaining)} "
            f"(unexpected — cycle check passed but sort stalled)"
        )

    return order