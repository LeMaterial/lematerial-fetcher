# Copyright 2025 Entalpic
from enum import Enum


class Functional(str, Enum):
    PBE = "pbe"
    PBESOL = "pbesol"
    SCAN = "scan"
    r2SCAN = "r2scan"


class Source(str, Enum):
    ALEXANDRIA = "alexandria"
    MP = "mp"
    OQMD = "oqmd"
