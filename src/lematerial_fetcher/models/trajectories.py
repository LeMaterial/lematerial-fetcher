# Copyright 2025 Entalpic
import numpy as np
from pydantic import Field, field_validator, model_validator

from lematerial_fetcher.models.optimade import OptimadeStructure
from lematerial_fetcher.utils.logging import logger

# MPtrj defaults
ENERGY_CONVERGENCE_THRESHOLD = 2e-2  # Difference with the primary MP task
MAX_ENERGY_DIFF = 1  # 1 eV


class Trajectory(OptimadeStructure):
    relaxation_step: int = Field(..., description="Relaxation step of the trajectory")
    relaxation_number: int = Field(
        ..., description="Relaxation number of the trajectory"
    )

    @field_validator("forces")
    @classmethod
    def validate_forces_too_high(
        cls, v: list[list[float]] | None
    ) -> list[list[float]] | None:
        """Override the parent class validator to avoid checking forces."""
        return v

    @model_validator(mode="after")
    def validate_relaxation_trajectories(self):
        relaxation_number = np.array(self.relaxation_number)

        if self.relaxation_step < 0:
            raise ValueError("relaxation_step must be a non-negative integer")

        if relaxation_number < 0:
            raise ValueError("relaxation_number must be a non-negative integer")

        return self


def close_to_primary_task(
    primary_trajectories: list[Trajectory], trajectories: list[Trajectory]
) -> bool:
    """
    This guarantees that the final structure's energy is close to the primary
    trajectory's final structure's energy.

    This is used for MP to only keep the most appropriate trajectories for
    a given material.

    Parameters
    ----------
    primary_trajectories : list[Trajectory]
        The primary trajectories.
    trajectories : list[Trajectory]
        The trajectories to check.

    Returns
    -------
    bool
        True if the trajectory is close to the primary trajectory, False otherwise.
    """
    if len(trajectories) == 0 or len(primary_trajectories) == 0:
        return False

    if trajectories[-1].energy is None or primary_trajectories[-1].energy is None:
        return False

    energy_diff = np.abs(
        trajectories[-1].energy / trajectories[-1].nsites
        - primary_trajectories[-1].energy / primary_trajectories[-1].nsites
    )
    if energy_diff < ENERGY_CONVERGENCE_THRESHOLD:
        return True


def has_trajectory_converged(
    trajectories: list[Trajectory],
    max_energy_diff: float | None = MAX_ENERGY_DIFF,
) -> bool:
    """
    Check if the full trajectory has converged.

    This also excludes trajectories where forces or energy are not available.

    Parameters
    ----------
    trajectories : list[Trajectory]
        The trajectories to check.
    max_energy_diff : float | None
        The maximum energy difference between a structure and the last structure
        in the trajectory.

    Returns
    -------
    bool
        True if the trajectory has converged, False otherwise.
    """
    filtered_trajectories = []

    for i, trajectory in enumerate(trajectories):
        if trajectory.energy is None or trajectory.forces is None:
            logger.debug(
                f"Trajectory {trajectory.id} has no energy or forces, skipping"
            )
            continue
        filtered_trajectories.append(trajectory)

    trajectories = filtered_trajectories

    if len(trajectories) == 0:
        return []

    final_trajectory = trajectories[-1]

    filtered_trajectories = []

    for i, trajectory in enumerate(trajectories):
        if i != len(trajectories) - 1:
            energy_diff = np.abs(
                trajectory.energy / trajectory.nsites
                - final_trajectory.energy / final_trajectory.nsites
            )
            if energy_diff <= max_energy_diff:
                filtered_trajectories.append(trajectory)
            else:
                logger.debug(
                    f"Trajectory {trajectory.id} has not converged, energy difference: {np.abs(trajectory.energy - final_trajectory.energy):.4f} eV"
                )

    return filtered_trajectories
