# Copyright 2025 Entalpic
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class Structure:
    id: str
    type: str
    attributes: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {"id": self.id, "type": self.type, "attributes": self.attributes}


@dataclass
class APIResponse:
    data: List[Structure]
    links: Dict[str, str]

    def to_dict(self) -> Dict[str, Any]:
        return {"data": [item.to_dict() for item in self.data], "links": self.links}
