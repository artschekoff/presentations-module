from typing import TypedDict


class ProgressPayloadBase(TypedDict):
    stage: str
    step: int
    total_steps: int
    percent: int


class ProgressPayload(ProgressPayloadBase, total=False):
    files: list[str]
