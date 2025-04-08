# Copyright 2025 Entalpic
import numpy as np
from pydantic import Field, model_validator

from lematerial_fetcher.models.optimade import OptimadeStructure
from lematerial_fetcher.utils.logging import logger

ENERGY_CONVERGENCE_THRESHOLD = 2e-2  # MPtrj default
FORCE_CONVERGENCE_THRESHOLD = 0.2


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


def has_trajectory_converged(
    trajectories: list[Trajectory],
    energy_threshold: float | None = ENERGY_CONVERGENCE_THRESHOLD,
    force_threshold: float | None = FORCE_CONVERGENCE_THRESHOLD,
) -> bool:
    """
    Check if the full trajectory has converged.

    This also excludes trajectories where no last step has no forces
    or energy.

    Parameters
    ----------
    trajectories : list[Trajectory]
        The trajectories to check.

    Returns
    -------
    bool
        True if the trajectory has converged, False otherwise.
    """
    # If the last step has no energy or forces, we cannot check for convergence
    # and we don't want the trajectory to be pushed
    if trajectories[-1].energy is None or trajectories[-1].forces is None:
        logger.warning(
            f"Trajectory {trajectories[-1].id} has no energy or forces, skipping"
        )
        return False

    if energy_threshold is not None and len(trajectories) > 1:
        if np.abs(trajectories[-1].energy - trajectories[-2].energy) > energy_threshold:
            logger.warning(
                f"Trajectory {trajectories[-1].id} has not converged, energy difference: {np.abs(trajectories[-1].energy - trajectories[-2].energy):.2f} eV"
            )
            return False

    if force_threshold is not None:
        if (
            np.linalg.norm(np.array(trajectories[-1].forces), axis=1).max()
            > force_threshold
        ):
            logger.warning(
                f"Trajectory {trajectories[-1].id} has not converged, max force norm: {np.linalg.norm(np.array(trajectories[-1].forces), axis=1).max():.2f} eV/A"
            )
            return False

    return True
