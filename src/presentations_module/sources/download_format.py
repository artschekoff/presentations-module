from enum import Enum


class DownloadFormat(str, Enum):
    POWERPOINT = "PowerPoint"
    PDF = "PDF"
    TEXT = "text"
