"""Runtime initialization and management."""

from typing import Optional
import logging
import os

from vortex.config import RuntimeConfig
from vortex.coordinator import WorkerHandle, WorkerPhase, CoordinatorConnection

logger = logging.getLogger(__name__)

# Global runtime state
_runtime: Optional["VortexRuntime"] = None


class VortexRuntime:
    """Main runtime manager.
    
    Coordinates all components including:
    - Coordinator connection
    - Data loading
    - Checkpointing
    - Worker state
    """
    
    def __init__(self, config: RuntimeConfig):
        self.config = config
        self.worker_handle: Optional[WorkerHandle] = None
        self._coordinator: Optional[CoordinatorConnection] = None
        self._initialized = False
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeats."""
        import asyncio
        while self._coordinator and self._coordinator.is_connected:
            try:
                step = self.worker_handle.step if self.worker_handle else 0
                await self._coordinator.heartbeat(step)
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
            await asyncio.sleep(1.0)

    async def initialize(self) -> WorkerHandle:
        """Initialize the runtime and register with coordinator.
        
        Returns:
            WorkerHandle for this worker
        """
        logger.info(f"Initializing Vortex runtime for worker {self.config.worker_id}")
        
        # Connect to coordinator
        self._coordinator = CoordinatorConnection(self.config.coordinator_addr)
        await self._coordinator.connect()
        
        # Register
        hostname = os.uname().nodename if hasattr(os, 'uname') else "localhost"
        worker_id, epoch, step, shards = await self._coordinator.register(
            worker_uuid=f"worker-{self.config.worker_id}",
            hostname=hostname,
            port=50052 + self.config.worker_id,
        )
        
        self.worker_handle = WorkerHandle(
            worker_id=worker_id,
            phase=WorkerPhase.REGISTERED,
            epoch=epoch,
            step=step,
            fencing_token=0,
        )
        
        self._initialized = True
        logger.info(f"Runtime initialized, worker_id={worker_id}")
        
        # Start heartbeat
        import asyncio
        asyncio.create_task(self._heartbeat_loop())
        
        return self.worker_handle
    
    def sync_epoch(self, epoch: int) -> None:
        """Synchronize at epoch boundary.
        
        Args:
            epoch: Current epoch number
        """
        if self.worker_handle:
            self.worker_handle.sync_epoch(epoch)
    
    async def shutdown(self) -> None:
        """Shutdown the runtime gracefully."""
        logger.info("Shutting down Vortex runtime")
        
        if self.worker_handle:
            self.worker_handle.phase = WorkerPhase.SHUTTING_DOWN
        
        if self._coordinator:
            await self._coordinator.deregister()
        
        self._initialized = False


def init(config: RuntimeConfig) -> "VortexRuntime":
    """Initialize the Vortex runtime.
    
    This is the main entry point for starting the runtime.
    Must be called before any other DTrainer operations.
    
    Args:
        config: Runtime configuration
        
    Returns:
        Initialized runtime instance
    """
    global _runtime
    
    if _runtime is not None:
        logger.warning("Runtime already initialized, returning existing")
        return _runtime
    
    _runtime = VortexRuntime(config)
    
    # Auto-start async runtime in background thread for synchronous clients
    import threading
    import asyncio
    
    def run_async_loop(rt):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async def runner():
            await rt.initialize()
            # Keep alive
            while True:
                await asyncio.sleep(1)
                
        loop.run_until_complete(runner())

    t = threading.Thread(target=run_async_loop, args=(_runtime,), daemon=True)
    t.start()
    
    logger.info("Runtime initialized in background thread")
    
    return _runtime


def shutdown() -> None:
    """Shutdown the global runtime."""
    global _runtime
    
    if _runtime is not None:
        # Would need to run async shutdown
        import asyncio
        asyncio.get_event_loop().run_until_complete(_runtime.shutdown())
        _runtime = None


def get_runtime() -> Optional["VortexRuntime"]:
    """Get the global runtime instance."""
    return _runtime
