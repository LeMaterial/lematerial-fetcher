import numpy as np
from pydantic import Field, model_validator

from lematerial_fetcher.models.optimade import OptimadeStructure


class Trajectory(OptimadeStructure):
    relaxation_steps: list[int] = Field(
        ..., description="List of relaxation steps of the consecutive trajectories"
    )
    relaxation_number: list[int] = Field(
        ..., description="List associating every step with a relaxation trajectory"
    )

    @model_validator(mode="after")
    def validate_relaxation_trajectories(self):
        if len(self.relaxation_number) != len(self.relaxation_steps):
            raise ValueError(
                "relaxation_number and relaxation_steps must have the same length"
            )

        if self.relaxation_number != sorted(self.relaxation_number):
            raise ValueError("relaxation_number must be sorted")

        if self.relaxation_steps != sorted(self.relaxation_steps):
            raise ValueError("relaxation_steps must be sorted")

        if self.relaxation_steps != list(np.arange(1, len(self.relaxation_steps) + 1)):
            raise ValueError(
                "relaxation_steps must be a list of consecutive integers starting from 1"
            )

        if np.any(np.diff(self.relaxation_number) != 1):
            raise ValueError("relaxation_number must be a list of consecutive integers")

        return self
