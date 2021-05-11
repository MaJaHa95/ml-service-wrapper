
from .contexts import ProcessContext, ServiceContext

__all__ = ["Service"]

class Service:
    async def load(self, ctx: ServiceContext):
        """Initialize variables and load models."""

    async def process(self, ctx: ProcessContext):
        """Run a prediction or processing service."""
        """Implementations may make in-place modifications to any data they receive."""

        raise NotImplementedError()

    def dispose(self):
        """Clean up any resources (file handles, temporary files, etc.) to gracefully shut down."""

        pass

