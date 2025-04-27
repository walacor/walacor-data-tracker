from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True, slots=True)
class Snapshot:
    id: UUID
    timestamp: str
    operation: str
    shape: tuple[int, int]
    args: tuple
    kwargs: dict
    dataframe: str

class History:
    def __init__(self, max_len:int | None)-> None:
        pass

class Tracker:
    pass