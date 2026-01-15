def _extract_name_from_semaphore_key(key: str) -> str:
    return key.split(":")[-1]


def _get_semaphore_key(name: str, namespace: str) -> str:
    return f"{namespace}:semaphore_main:{name}"


def _get_semaphore_ttl_key(name: str, namespace: str) -> str:
    return f"{namespace}:semaphore_ttl:{name}"


def _get_max_key(name: str, namespace: str) -> str:
    return f"{namespace}:semaphore_max:{name}"
