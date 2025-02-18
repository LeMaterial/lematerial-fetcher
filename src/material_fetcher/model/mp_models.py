from dataclasses import dataclass
from typing import Any, Dict

# TODO(ramlaoui): Need to convert those to Structure


@dataclass
class StructureInfo:
    class_name: str
    module: str
    version: str

    def __init__(self, class_name: str = "", module: str = "", version: str = ""):
        self.class_name = class_name
        self.module = module
        self.version = version

    def to_dict(self) -> Dict:
        return {
            "@class": self.class_name,
            "@module": self.module,
            "@version": self.version,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "StructureInfo":
        return cls(
            class_name=data.get("@class", ""),
            module=data.get("@module", ""),
            version=data.get("@version", ""),
        )


class MPStructure:
    def __init__(self, material_id: str = "", structure: StructureInfo = None):
        self.material_id = material_id
        self.structure = structure or StructureInfo()
        self.attributes: Dict[str, Any] = {}

    def to_dict(self) -> Dict:
        return {
            "material_id": self.material_id,
            "structure": self.structure.to_dict(),
            **self.attributes,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "MPStructure":
        instance = cls(
            material_id=data.get("material_id", ""),
            structure=StructureInfo.from_dict(data.get("structure", {})),
        )
        # Store all other attributes that aren't material_id or structure
        instance.attributes = {
            k: v for k, v in data.items() if k not in ["material_id", "structure"]
        }
        return instance
