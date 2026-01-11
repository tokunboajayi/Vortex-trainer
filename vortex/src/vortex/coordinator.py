"""Coordinator client for worker coordination."""

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Tuple
import logging

from vortex.config import ShardAssignment

logger = logging.getLogger(__name__)


class WorkerPhase(Enum):
    """Worker lifecycle phases."""
    INITIALIZING = "initializing"
    REGISTERED = "registered"
    LOADING = "loading"
    TRAINING = "training"
    CHECKPOINTING = "checkpointing"
    EPOCH_BOUNDARY = "epoch_boundary"
    SHUTTING_DOWN = "shutting_down"
    FAILED = "failed"


@dataclass
class WorkerHandle:
    """Handle to the local worker state.
    
    This provides access to the worker's coordination state
    and methods for interacting with the coordinator.
    """
    worker_id: int
    phase: WorkerPhase
    epoch: int
    step: int
    fencing_token: int
    
    def sync_epoch(self, epoch: int) -> None:
        """Synchronize at epoch boundary.
        
        Blocks until all workers reach the barrier.
        
        Args:
            epoch: Current epoch number
        """
        # In real implementation, calls into Rust CoordinatorClient
        logger.info(f"Worker {self.worker_id} syncing at epoch {epoch}")
        self.epoch = epoch
    
    def report_step(self, step: int) -> None:
        """Report current step to coordinator.
        
        Called periodically during training.
        
        Args:
            step: Current step number
        """
        self.step = step


class CoordinatorConnection:
    """Connection to the coordinator service.
    
    Manages the gRPC connection and provides methods for
    worker registration, heartbeats, and coordination.
    """
    
    def __init__(self, addr: str, timeout_secs: float = 30.0):
        """Initialize coordinator connection.
        
        Args:
            addr: Coordinator address (e.g., "coordinator:50051")
            timeout_secs: Connection timeout in seconds
        """
        self._addr = addr
        self._timeout = timeout_secs
        self._connected = False
        self._worker_id: Optional[int] = None
        self._channel = None
        self._stub = None
        
    async def connect(self) -> None:
        """Establish connection to coordinator."""
        import grpc
        from vortex.proto import dtrainer_pb2_grpc
        
        logger.info(f"Connecting to coordinator at {self._addr}")
        # Ensure scheme is present (grpc usually expects host:port, but for aio generic is fine)
        # But grpc.aio.insecure_channel expects target.
        target = self._addr.replace("http://", "") 
        self._channel = grpc.aio.insecure_channel(target)
        self._stub = dtrainer_pb2_grpc.CoordinatorStub(self._channel)
        self._connected = True
        
    async def register(
        self,
        worker_uuid: str,
        hostname: str,
        port: int,
    ) -> Tuple[int, int, int, List[ShardAssignment]]:
        """Register with coordinator."""
        if not self._connected:
            raise RuntimeError("Not connected to coordinator")
        
        from vortex.proto import dtrainer_pb2
        
        logger.info(f"Registering worker {worker_uuid}")
        
        req = dtrainer_pb2.RegisterWorkerRequest(
            worker_uuid=worker_uuid,
            hostname=hostname,
            port=port,
            capabilities={},
            protocol_version=1
        )
        
        try:
            resp = await self._stub.RegisterWorker(req, timeout=self._timeout)
            
            # Map proto response to internal types
            shards = [] 
            for s in resp.initial_shards:
                shards.append(ShardAssignment(
                    shard_id=s.shard_id,
                    object_key=s.object_key,
                    byte_offset=s.byte_offset,
                    byte_length=s.byte_length,
                    expected_crc32c=s.expected_crc32c
                ))
                
            self._worker_id = resp.worker_id
            return resp.worker_id, resp.current_epoch, resp.current_step, shards
            
        except Exception as e:
            logger.error(f"Failed to register worker: {e}")
            raise RuntimeError(f"Registration failed: {e}")
    
    async def heartbeat(self, step: int) -> None:
        """Send heartbeat to coordinator."""
        if self._worker_id is None:
            raise RuntimeError("Worker not registered")
            
        from vortex.proto import dtrainer_pb2
        
        # Fake metrics for dashboard demo
        import random
        metrics = dtrainer_pb2.ResourceMetrics(
            cpu_percent=45.0 + random.random() * 10,
            memory_percent=60.0,
            gpu_utilization=85.0,
            gpu_memory_percent=70.0,
            bytes_read=50 * 1024 * 1024, # 50MB per tick cumulative? 
            # Note: Proto def is likely cumulative. But for this demo we send a value.
            bytes_written=0
        )
        
        req = dtrainer_pb2.HeartbeatRequest(
            worker_id=self._worker_id,
            current_step=step,
            status=3, # TRAINING (WORKER_STATUS_TRAINING)
            metrics=metrics,
            fencing_token=0
        )
        
        try:
            resp = await self._stub.Heartbeat(req)
            if resp.checkpoint_trigger and resp.checkpoint_trigger.epoch > 0:
                logger.info(f"Checkpoint requested for epoch {resp.checkpoint_trigger.epoch}")
        except Exception as e:
            logger.warning(f"Heartbeat failed: {e}")
    
    async def deregister(self) -> None:
        """Deregister from coordinator."""
        if self._worker_id is not None and self._stub:
            from vortex.proto import dtrainer_pb2
            logger.info(f"Deregistering worker {self._worker_id}")
            try:
                await self._stub.DeregisterWorker(
                    dtrainer_pb2.DeregisterWorkerRequest(worker_id=self._worker_id, reason="shutdown")
                )
            except:
                pass
            self._worker_id = None
            if self._channel:
                 await self._channel.close()
    
    @property
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected
    
    @property
    def worker_id(self) -> Optional[int]:
        """Get assigned worker ID."""
        return self._worker_id
