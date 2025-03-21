import numpy as np
from pydantic import Field, model_validator

from lematerial_fetcher.models.optimade import OptimadeStructure


class Trajectory(OptimadeStructure):
    relaxation_step: int = Field(..., description="Relaxation step of the trajectory")
    trajectory_step: list[int] = Field(
        ..., description="List of relaxation steps of the consecutive trajectories"
    )
    trajectory_number: list[int] = Field(
        ..., description="List associating every step with a relaxation trajectory"
    )

    @model_validator(mode="after")
    def validate_relaxation_trajectories(self):
        trajectory_step = np.array(self.trajectory_step)
        trajectory_number = np.array(self.trajectory_number)

        if self.relaxation_step not in trajectory_step:
            raise ValueError("relaxation_step must be in trajectory_step")

        if self.relaxation_step < 0:
            raise ValueError("relaxation_step must be a non-negative integer")

        if not (all(trajectory_step >= 0)):
            raise ValueError("trajectory_step must be a list of non-negative integers")

        if not (all(trajectory_number >= 0)):
            raise ValueError(
                "trajectory_number must be a list of non-negative integers"
            )

        if len(trajectory_number) != len(trajectory_step):
            raise ValueError(
                "trajectory_number and trajectory_step must have the same length"
            )

        if not (np.all(trajectory_number == sorted(trajectory_number))):
            raise ValueError("trajectory_number must be sorted")

        if not (np.all(trajectory_step == sorted(trajectory_step))):
            raise ValueError("trajectory_step must be sorted")

        if not (np.all(trajectory_step == list(np.arange(len(trajectory_step))))):
            raise ValueError(
                "trajectory_step must be a list of consecutive integers starting from 0"
            )

        if not (np.all(np.diff(trajectory_number) <= 1)):
            raise ValueError(
                "trajectory_number must be a list that increases by at most 1 between consecutive steps"
            )

        return self
