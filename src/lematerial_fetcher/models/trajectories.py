import numpy as np
from pydantic import Field, model_validator

from lematerial_fetcher.models.optimade import OptimadeStructure


class Trajectory(OptimadeStructure):
    relaxation_step: int = Field(..., description="Relaxation step of the trajectory")
    relaxation_number: int = Field(
        ..., description="Relaxation number of the trajectory"
    )

    @model_validator(mode="after")
    def validate_relaxation_trajectories(self):
        relaxation_number = np.array(self.relaxation_number)

        if self.relaxation_step < 0:
            raise ValueError("relaxation_step must be a non-negative integer")

        if relaxation_number < 0:
            raise ValueError("relaxation_number must be a non-negative integer")

        return self
