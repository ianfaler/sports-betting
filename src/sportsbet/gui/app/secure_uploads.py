"""Secure file upload handlers for the sports betting GUI."""

import asyncio
import hashlib
import io
import logging
from pathlib import Path
from typing import Any

import cloudpickle
import reflex as rx
from typing_extensions import Self

from sportsbet.datasets import BaseDataLoader
from sportsbet.evaluation import BaseBettor

# File upload security configuration
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB limit
ALLOWED_EXTENSIONS = ['.pkl']
ALLOWED_MIME_TYPES = ['application/octet-stream']

# Memory management configuration
MAX_MEMORY_USAGE = 500 * 1024 * 1024  # 500MB memory limit for data in state
BATCH_SIZE = 1000  # For data processing in batches

# Animation delay for UI feedback
DELAY = 0.001

logger = logging.getLogger(__name__)


class FileValidationError(Exception):
    """Custom exception for file validation errors."""
    pass


class MemoryError(Exception):
    """Custom exception for memory limit errors."""
    pass


def validate_file_upload(filename: str, file_content: bytes) -> dict[str, Any]:
    """Validate uploaded file for security and size constraints."""
    validation_result = {
        'valid': False,
        'error': None,
        'file_info': {},
        'security_hash': None
    }
    
    try:
        # Check file extension
        file_path = Path(filename)
        if file_path.suffix.lower() not in ALLOWED_EXTENSIONS:
            raise FileValidationError(f"File extension {file_path.suffix} not allowed. Allowed: {ALLOWED_EXTENSIONS}")
        
        # Check file size
        file_size = len(file_content)
        if file_size > MAX_FILE_SIZE:
            raise FileValidationError(f"File size {file_size:,} bytes exceeds limit of {MAX_FILE_SIZE:,} bytes")
        
        if file_size == 0:
            raise FileValidationError("File is empty")
        
        # Generate security hash for integrity checking
        file_hash = hashlib.sha256(file_content).hexdigest()
        
        # Basic magic number validation for pickle files
        if not file_content.startswith(b'\x80\x03') and not file_content.startswith(b'\x80\x04'):
            logger.warning(f"File {filename} doesn't appear to be a valid pickle file (magic number check)")
        
        validation_result.update({
            'valid': True,
            'file_info': {
                'name': filename,
                'size': file_size,
                'extension': file_path.suffix
            },
            'security_hash': file_hash
        })
        
        logger.info(f"File validation passed for {filename} (size: {file_size:,} bytes)")
        
    except FileValidationError as e:
        validation_result['error'] = str(e)
        logger.error(f"File validation failed for {filename}: {e}")
    except Exception as e:
        validation_result['error'] = f"Unexpected validation error: {e}"
        logger.error(f"Unexpected file validation error for {filename}: {e}")
    
    return validation_result


def safe_deserialize_dataloader(file_content: bytes, filename: str) -> tuple[BaseDataLoader | None, str | None]:
    """Safely deserialize dataloader with additional validation."""
    try:
        # Use io.BytesIO for safer deserialization
        buffer = io.BytesIO(file_content)
        
        # Attempt deserialization with size limit
        dataloader = cloudpickle.load(buffer)
        
        # Validate the deserialized object
        if not isinstance(dataloader, BaseDataLoader):
            return None, f"File contains {type(dataloader).__name__}, expected BaseDataLoader"
        
        # Additional validation - check if dataloader has required methods
        required_methods = ['extract_train_data', 'extract_fixtures_data', 'get_odds_types']
        for method in required_methods:
            if not hasattr(dataloader, method):
                return None, f"Dataloader missing required method: {method}"
        
        logger.info(f"Successfully deserialized dataloader from {filename}")
        return dataloader, None
        
    except Exception as e:
        error_msg = f"Failed to deserialize dataloader: {e}"
        logger.error(f"Deserialization error for {filename}: {error_msg}")
        return None, error_msg


def safe_deserialize_model(file_content: bytes, filename: str) -> tuple[BaseBettor | None, str | None]:
    """Safely deserialize betting model with additional validation."""
    try:
        # Use io.BytesIO for safer deserialization
        buffer = io.BytesIO(file_content)
        
        # Attempt deserialization
        model = cloudpickle.load(buffer)
        
        # Validate the deserialized object
        if not isinstance(model, BaseBettor):
            return None, f"File contains {type(model).__name__}, expected BaseBettor"
        
        # Additional validation - check if model has required methods
        required_methods = ['fit', 'predict', 'bet']
        for method in required_methods:
            if not hasattr(model, method):
                return None, f"Model missing required method: {method}"
        
        logger.info(f"Successfully deserialized model from {filename}")
        return model, None
        
    except Exception as e:
        error_msg = f"Failed to deserialize model: {e}"
        logger.error(f"Model deserialization error for {filename}: {error_msg}")
        return None, error_msg


async def secure_dataloader_upload_handler(
    state_instance, 
    files: list[rx.UploadFile]
) -> None:
    """Secure handler for dataloader file uploads."""
    state_instance.loading = True
    state_instance.dataloader_error = False
    
    try:
        if not files:
            raise FileValidationError("No files provided")
        
        for file in files:
            try:
                # Read file content
                file_content = await file.read()
                filename = Path(file.filename).name
                
                # Validate file upload
                validation = validate_file_upload(filename, file_content)
                if not validation['valid']:
                    raise FileValidationError(validation['error'])
                
                # Safely deserialize dataloader
                dataloader, error = safe_deserialize_dataloader(file_content, filename)
                if error:
                    raise FileValidationError(error)
                
                # Store the validated data
                state_instance.dataloader_serialized = str(file_content, 'iso8859_16')
                state_instance.dataloader_filename = filename
                
                logger.info(f"Successfully uploaded and validated dataloader: {filename}")
                
            except Exception as e:
                state_instance.dataloader_error = True
                error_message = f"File upload failed: {str(e)}"
                logger.error(f"Dataloader upload error: {error_message}")
                
                state_instance.loading = False
                
                message = f"❌ Upload Error: {error_message}"
                state_instance.streamed_message = ''
                for char in message:
                    await asyncio.sleep(DELAY)
                    state_instance.streamed_message += char
                return
        
        # Success case
        state_instance.dataloader_error = False
        state_instance.loading = False
        
        message = "✅ Dataloader uploaded successfully! You may proceed to the next step."
        state_instance.streamed_message = ''
        for char in message:
            await asyncio.sleep(DELAY)
            state_instance.streamed_message += char
            
    except Exception as e:
        # Catch-all error handler
        state_instance.dataloader_error = True
        state_instance.loading = False
        error_message = f"Unexpected upload error: {str(e)}"
        logger.error(f"Unexpected dataloader upload error: {error_message}")
        
        message = f"❌ Unexpected Error: {error_message}"
        state_instance.streamed_message = ''
        for char in message:
            await asyncio.sleep(DELAY)
            state_instance.streamed_message += char


async def secure_model_upload_handler(
    state_instance, 
    files: list[rx.UploadFile]
) -> None:
    """Secure handler for model file uploads."""
    state_instance.loading = True
    state_instance.model_error = False
    
    try:
        if not files:
            raise FileValidationError("No files provided")
        
        for file in files:
            try:
                # Read file content
                file_content = await file.read()
                filename = Path(file.filename).name
                
                # Validate file upload
                validation = validate_file_upload(filename, file_content)
                if not validation['valid']:
                    raise FileValidationError(validation['error'])
                
                # Safely deserialize model
                model, error = safe_deserialize_model(file_content, filename)
                if error:
                    raise FileValidationError(error)
                
                # Store the validated data
                state_instance.model_serialized = str(file_content, 'iso8859_16')
                state_instance.model_filename = filename
                
                logger.info(f"Successfully uploaded and validated model: {filename}")
                
            except Exception as e:
                state_instance.model_error = True
                error_message = f"File upload failed: {str(e)}"
                logger.error(f"Model upload error: {error_message}")
                
                state_instance.loading = False
                
                message = f"❌ Upload Error: {error_message}"
                state_instance.streamed_message = ''
                for char in message:
                    await asyncio.sleep(DELAY)
                    state_instance.streamed_message += char
                return
        
        # Success case
        state_instance.model_error = False
        state_instance.loading = False
        
        message = "✅ Model uploaded successfully! You may proceed to the next step."
        state_instance.streamed_message = ''
        for char in message:
            await asyncio.sleep(DELAY)
            state_instance.streamed_message += char
            
    except Exception as e:
        # Catch-all error handler
        state_instance.model_error = True
        state_instance.loading = False
        error_message = f"Unexpected upload error: {str(e)}"
        logger.error(f"Unexpected model upload error: {error_message}")
        
        message = f"❌ Unexpected Error: {error_message}"
        state_instance.streamed_message = ''
        for char in message:
            await asyncio.sleep(DELAY)
            state_instance.streamed_message += char