"""Type stubs for dftracer_utils_ext module."""

from typing import Optional, List, Any

# ========== INDEXER ==========

class IndexerCheckpoint:
    """Information about a checkpoint in the index."""
    checkpoint_idx: int
    uc_offset: int
    uc_size: int
    c_offset: int
    c_size: int
    bits: int
    num_lines: int

class Indexer:
    """Indexer for creating and managing gzip file indices."""
    
    def __init__(
        self, 
        gz_path: str, 
        idx_path: Optional[str] = None,
        checkpoint_size: int = 1048576,
        force_rebuild: bool = False
    ) -> None:
        """Create an indexer for a gzip file."""
        ...
    
    def build(self) -> None:
        """Build the index."""
        ...
    
    def need_rebuild(self) -> bool:
        """Check if index needs rebuilding."""
        ...
    
    def exists(self) -> bool:
        """Check if the index file exists."""
        ...
    
    def get_max_bytes(self) -> int:
        """Get maximum byte position."""
        ...
    
    def get_num_lines(self) -> int:
        """Get number of lines."""
        ...

    def get_checkpoints(self) -> List[IndexerCheckpoint]:
        """Get all checkpoints."""
        ...

    def find_checkpoint(self, target_offset: int) -> Optional[IndexerCheckpoint]:
        """Find checkpoint for target offset."""
        ...
    
    @property
    def gz_path(self) -> str:
        """Get gzip path."""
        ...
    
    @property
    def idx_path(self) -> str:
        """Get index path."""
        ...
    
    @property
    def checkpoint_size(self) -> int:
        """Get checkpoint size."""
        ...
    
    def __enter__(self) -> 'Indexer':
        """Enter the runtime context for the with statement."""
        ...
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the runtime context for the with statement."""
        ... 

# ========== READER ==========

class Reader:
    """Reader for reading from gzip files"""
    
    def __init__(
        self, 
        gz_path: str,
        idx_path: Optional[str] = None,
        checkpoint_size: int = 1048576,
        indexer: Optional[Indexer] = None
    ) -> None:
        """Create a  reader."""
        ...
    
    def get_max_bytes(self) -> int:
        """Get the maximum byte position available in the file."""
        ...
    
    def get_num_lines(self) -> int:
        """Get the number of lines in the file."""
        ...
    
    def reset(self) -> None:
        """Reset the reader to initial state."""
        ...
    
    def read(self, start_bytes: int, end_bytes: int) -> bytes:
        """Read raw bytes and return as bytes."""
        ...
        
    def read_lines(self, start_line: int, end_line: int) -> List[str]:
        """Zero-copy read lines and return as list[str]."""
        ...
        
    def read_line_bytes(self, start_bytes: int, end_bytes: int) -> List[str]:
        """Read line bytes and return as list[str]."""
        ...
        
    def read_lines_json(self, start_line: int, end_line: int) -> List[JSON]:
        """Read lines and parse as JSON, return as list[JSON]."""
        ...
        
    def read_line_bytes_json(self, start_bytes: int, end_bytes: int) -> List[JSON]:
        """Read line bytes and parse as JSON, return as list[JSON]."""
        ...
    
    @property
    def gz_path(self) -> str:
        """Path to the gzip file."""
        ...
    
    @property
    def idx_path(self) -> str:
        """Path to the index file."""
        ...
    
    @property
    def checkpoint_size(self) -> int:
        """Checkpoint size in bytes."""
        ...
        
    @property
    def buffer_size(self) -> int:
        """Internal buffer size for read operations."""
        ...
    
    @buffer_size.setter
    def buffer_size(self, size: int) -> None:
        """Set internal buffer size for read operations."""
        ...
    
    def __enter__(self) -> 'Reader':
        """Enter the runtime context for the with statement."""
        ...
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the runtime context for the with statement."""
        ... 

# ========== JSON ==========

class JSON:
    """Lazy JSON object that parses on demand using yyjson."""
    
    def __init__(self, json_str: str) -> None:
        """Create a JSON object from a JSON string."""
        ...
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists in JSON object."""
        ...
    
    def __getitem__(self, key: str) -> Any:
        """Get value by key, raises KeyError if not found."""
        ...
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get value by key with optional default."""
        ...
    
    def keys(self) -> List[str]:
        """Get all keys from JSON object."""
        ...
    
    def __str__(self) -> str:
        """Return the original JSON string."""
        ...
    
    def __repr__(self) -> str:
        """Return string representation of the object."""
        ...
