from functools import lru_cache
from sdv.utils import load_synthesizer

@lru_cache(maxsize=1)
def get_synth(path: str):
    return load_synthesizer(filepath=path)
