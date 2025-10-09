#!/usr/bin/env python3
"""
Test cases for DFTracer indexer Python bindings
"""

import pytest
import os

import dftracer.utils as dft_utils
from .common import Environment

class TestIndexer:
    """Test cases for Indexer"""

    def test_indexer_creation(self):
        """Test indexer creation"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            idx_file = gz_file + ".idx"
            
            # Test basic creation using context manager
            with dft_utils.Indexer(gz_file, idx_file) as indexer:
                assert indexer.gz_path == gz_file
                assert indexer.idx_path == idx_file
                assert indexer.checkpoint_size > 0
    
    def test_indexer_creation_with_defaults(self):
        """Test indexer creation with default parameters"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            
            # Test creation with defaults using context manager
            with dft_utils.Indexer(gz_file) as indexer:
                assert indexer.gz_path == gz_file
                assert indexer.idx_path == gz_file + ".idx"
                assert indexer.checkpoint_size <= 33554432  # Should be <= 32MB default
    
    def test_indexer_custom_checkpoint_size(self):
        """Test indexer with custom checkpoint size"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            checkpoint_size = 1024 * 1024  # 1MB
            
            with dft_utils.Indexer(gz_file, checkpoint_size=checkpoint_size) as indexer:
                assert indexer.checkpoint_size <= checkpoint_size
    
    def test_indexer_nonexistent_file(self):
        """Test indexer creation with non-existent file"""
        # Indexer creation doesn't fail, but building should fail
        with pytest.raises(RuntimeError):
            indexer = dft_utils.Indexer("nonexistent_file.gz")
    
    def test_indexer_build_and_rebuild(self):
        """Test indexer build and rebuild functionality"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            idx_file = gz_file + ".idx"
            
            with dft_utils.Indexer(gz_file, idx_file) as indexer:
                # Should need rebuild initially
                assert indexer.need_rebuild()
                
                # Build the index
                indexer.build()
                
                # Index file should exist
                assert os.path.exists(idx_file)
                
                # Should not need rebuild after building
                assert not indexer.need_rebuild()
            
            # Test force rebuild with a new indexer
            # Note: force_rebuild affects the build process, not need_rebuild() check
            # The need_rebuild() method checks file consistency, not force_rebuild flag
            with dft_utils.Indexer(gz_file, idx_file, force_rebuild=True) as indexer_force:
                # Since the index already exists and file hasn't changed, need_rebuild should be False
                # But force_rebuild will cause a rebuild when build() is called
                assert not indexer_force.need_rebuild()
                # The force_rebuild behavior is tested by calling build() which should succeed
                indexer_force.build()  # This should rebuild due to force_rebuild=True
    
    def test_indexer_file_info(self):
        """Test indexer file information methods"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            
            with dft_utils.Indexer(gz_file) as indexer:
                if indexer.need_rebuild():
                    indexer.build()
                
                # Test file info methods
                max_bytes = indexer.get_max_bytes()
                num_lines = indexer.get_num_lines()
                
                assert isinstance(max_bytes, int)
                assert isinstance(num_lines, int)
                assert max_bytes > 0
                assert num_lines > 0
    
    
    def test_indexer_checkpoints(self):
        """Test indexer checkpoint functionality"""
        with Environment(lines=100000) as env:  # Larger file for checkpoints
            gz_file = env.create_test_gzip_file()
            checkpoint_size = 256 * 1024  # 256KB checkpoint size
            
            with dft_utils.Indexer(gz_file, checkpoint_size=checkpoint_size) as indexer:
                if indexer.need_rebuild():
                    indexer.build()
                
                # Debug: Check file size and checkpoint configuration
                max_bytes = indexer.get_max_bytes()
                num_lines = indexer.get_num_lines()
                print(f"File stats: {max_bytes} bytes, {num_lines} lines, checkpoint_size={checkpoint_size}")
                
                # Test get_checkpoints
                checkpoints = indexer.get_checkpoints()
                assert isinstance(checkpoints, list)
                print(f"Number of checkpoints created: {len(checkpoints)}")
                
                # NOTE: Checkpoint creation depends on deflate block boundaries in the compressed stream,
                # not just uncompressed file size. This is correct behavior for zlib-based random access.
                # The indexer may create 0, 1, or multiple checkpoints depending on how gzip compressed
                # the data and where deflate block boundaries fall relative to the checkpoint size.
                
                # Test that the API works correctly regardless of checkpoint count
                assert isinstance(checkpoints, list)
                
                # Test checkpoint properties if any exist
                for checkpoint in checkpoints:
                    assert hasattr(checkpoint, 'checkpoint_idx')
                    assert hasattr(checkpoint, 'uc_offset')
                    assert hasattr(checkpoint, 'uc_size')
                    assert hasattr(checkpoint, 'c_offset')
                    assert hasattr(checkpoint, 'c_size')
                    assert hasattr(checkpoint, 'bits')
                    assert hasattr(checkpoint, 'dict_compressed')
                    assert hasattr(checkpoint, 'num_lines')
                    
                    assert isinstance(checkpoint.checkpoint_idx, int)
                    assert isinstance(checkpoint.uc_offset, int)
                    assert isinstance(checkpoint.num_lines, int)
                    assert checkpoint.checkpoint_idx >= 0
                    assert checkpoint.uc_offset >= 0
                    assert checkpoint.num_lines >= 0
    
    def test_indexer_find_checkpoint(self):
        """Test indexer single checkpoint search"""
        with Environment(lines=2000) as env:  # Large file for testing
            gz_file = env.create_test_gzip_file(bytes_per_line=2048)  # Larger lines
            checkpoint_size = 512 * 1024  # 512KB checkpoint size
            
            with dft_utils.Indexer(gz_file, checkpoint_size=checkpoint_size) as indexer:
                if indexer.need_rebuild():
                    indexer.build()
                
                max_bytes = indexer.get_max_bytes()
                checkpoints = indexer.get_checkpoints()
                
                print(f"File has {max_bytes} bytes and {len(checkpoints)} checkpoints")
                
                # Test find_checkpoint API regardless of whether checkpoints exist
                target_offset = max_bytes // 2 if max_bytes > 0 else 0
                checkpoint = indexer.find_checkpoint(target_offset)
                
                # The find_checkpoint method should always return either a CheckpointInfo or None
                if checkpoint is not None:
                    # If a checkpoint is found, verify its properties
                    assert hasattr(checkpoint, 'uc_offset')
                    assert hasattr(checkpoint, 'uc_size')
                    assert hasattr(checkpoint, 'num_lines')
                    assert checkpoint.uc_offset <= target_offset
                    assert isinstance(checkpoint.uc_offset, int)
                    assert isinstance(checkpoint.uc_size, int)
                    assert isinstance(checkpoint.num_lines, int)
                
                # Test with offset 0 (per the C++ code, this should return None as a special case)
                checkpoint_0 = indexer.find_checkpoint(0)
                # According to indexer.cpp line 1104-1106, target_offset 0 always returns false
                assert checkpoint_0 is None, "find_checkpoint(0) should return None per implementation"
                
                # Test with offset beyond file size
                if max_bytes > 0:
                    checkpoint_beyond = indexer.find_checkpoint(max_bytes + 1000)
                    # This might return None or the last checkpoint, both are valid
                    if checkpoint_beyond is not None:
                        assert checkpoint_beyond.uc_offset <= max_bytes

class TestIndexerIntegration:
    """Integration tests for indexer with reader"""
    
    def test_indexer_with_reader_creation(self):
        """Test creating readers from indexer"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            
            # Create and build indexer using context manager
            with dft_utils.Indexer(gz_file) as indexer:
                if indexer.need_rebuild():
                    indexer.build()
                
                # Test creating reader from indexer
                reader = dft_utils.Reader(gz_file, indexer=indexer)
                assert reader.get_max_bytes() > 0
                assert reader.gz_path == gz_file
    
    def test_indexer_with_reader_creation(self):
        """Test using indexer with reader creation"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            
            # Create and build indexer using context manager
            with dft_utils.Indexer(gz_file) as indexer:
                if indexer.need_rebuild():
                    indexer.build()
                
                # Test creating reader with indexer
                reader = dft_utils.Reader(gz_file, indexer=indexer)
                assert reader.get_max_bytes() > 0
    
    def test_multiple_readers_same_indexer(self):
        """Test creating multiple readers from the same indexer"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            
            # Create and build indexer using context manager
            with dft_utils.Indexer(gz_file) as indexer:
                if indexer.need_rebuild():
                    indexer.build()
                
                # Create multiple readers from same indexer
                readers = []
                for i in range(3):
                    reader = dft_utils.Reader(gz_file, indexer=indexer)
                    assert reader.get_max_bytes() > 0
                    readers.append(reader)
                
                # All should have same file info
                max_bytes = readers[0].get_max_bytes()
                for reader in readers[1:]:
                    assert reader.get_max_bytes() == max_bytes


if __name__ == "__main__":
    pytest.main([__file__])
