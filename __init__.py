if __package__:
    from .adapter import register
else:  # Direct checkout import (for pytest and local validation).
    from adapter import register

__all__ = ["register"]
