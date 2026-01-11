"""Training callbacks for PyTorch integration."""

from typing import Any, Dict, Optional
import logging

from dtrainer.checkpoint import CheckpointManager

logger = logging.getLogger(__name__)


class CheckpointCallback:
    """Training callback for automatic checkpointing.
    
    Integrates with PyTorch training loops to automatically
    trigger checkpoints at the configured interval.
    
    Example:
        >>> callback = CheckpointCallback("my-job", interval=1000)
        >>> for step, batch in enumerate(loader):
        ...     loss = train_step(model, batch)
        ...     callback.on_step_end(step, model, optimizer)
    """
    
    def __init__(
        self,
        job_id: str,
        interval: int = 1000,
        async_write: bool = True,
    ):
        """Initialize callback.
        
        Args:
            job_id: Unique job identifier
            interval: Steps between checkpoints
            async_write: Use non-blocking writes
        """
        self._manager = CheckpointManager(
            job_id=job_id,
            checkpoint_interval=interval,
            async_write=async_write,
        )
        self._loader_state: Optional[Dict[str, Any]] = None
        
    def set_loader_state(self, state: Dict[str, Any]) -> None:
        """Set loader state to include in checkpoints.
        
        Args:
            state: Loader state dict
        """
        self._loader_state = state
    
    def on_step_end(
        self,
        step: int,
        model: Any,
        optimizer: Any,
        extra: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Called at the end of each training step.
        
        Args:
            step: Current step number
            model: Model to checkpoint
            optimizer: Optimizer to checkpoint
            extra: Optional extra state
            
        Returns:
            True if checkpoint was triggered
        """
        return self._manager.maybe_checkpoint(
            step=step,
            model=model,
            optimizer=optimizer,
            loader_state=self._loader_state,
            extra=extra,
        )
    
    def on_epoch_end(
        self,
        epoch: int,
        step: int,
        model: Any,
        optimizer: Any,
    ) -> None:
        """Called at the end of each epoch.
        
        Forces a checkpoint at epoch boundaries.
        
        Args:
            epoch: Current epoch number
            step: Current step number
            model: Model to checkpoint
            optimizer: Optimizer to checkpoint
        """
        self._manager.checkpoint(
            step=step,
            model=model,
            optimizer=optimizer,
            loader_state=self._loader_state,
            force=True,
        )
        logger.info(f"Epoch {epoch} checkpoint complete")
    
    def on_training_end(self, model: Any, optimizer: Any) -> None:
        """Called at the end of training.
        
        Waits for any pending checkpoints and writes final state.
        
        Args:
            model: Final model
            optimizer: Final optimizer
        """
        self._manager.wait_for_completion()
        self._manager.checkpoint(
            step="final",
            model=model,
            optimizer=optimizer,
            loader_state=self._loader_state,
            force=True,
        )
        logger.info("Final checkpoint complete")
