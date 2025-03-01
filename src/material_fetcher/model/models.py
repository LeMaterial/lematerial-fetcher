# Copyright 2025 Entalpic
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass
class RawStructure:
    id: str
    type: str
    attributes: dict[str, Any]
    last_modified: Optional[datetime] = None

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "type": self.type, "attributes": self.attributes}


@dataclass
class APIResponse:
    data: list[RawStructure]
    links: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {"data": [item.to_dict() for item in self.data], "links": self.links}
