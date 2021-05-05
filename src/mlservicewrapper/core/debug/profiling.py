
class BaseProfilerWrapper:
    def enable(self) -> None:
        raise NotImplementedError()
    def disable(self) -> None:
        raise NotImplementedError()
    def save(self) -> None:
        raise NotImplementedError()

class _ProfilerWrapper(BaseProfilerWrapper):
    def __init__(self, to_path: str) -> None:
        super().__init__()
        
        self._to_path = to_path
        
        import cProfile
        self._profiler = cProfile.Profile()
    
    def enable(self):
        return self._profiler.enable()

    def disable(self):
        return self._profiler.disable()

    def save(self):
        return self._profiler.dump_stats(self._to_path)

class _EmptyProfilerWrapper(BaseProfilerWrapper):
    def enable(self):
        return None

    def disable(self):
        return None

    def save(self):
        return None

def get_profiler(profile_processing_to_file: str) -> BaseProfilerWrapper:
    
    if profile_processing_to_file is None:
        return _EmptyProfilerWrapper()
    
    return _ProfilerWrapper(profile_processing_to_file)
