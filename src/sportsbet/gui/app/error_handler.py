"""Enhanced error handling and user messaging system."""

import logging
import traceback
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
import asyncio

logger = logging.getLogger(__name__)


class ErrorCategory(Enum):
    """Categories of errors for better user messaging."""
    NETWORK = "network"
    FILE_UPLOAD = "file_upload"
    DATA_PROCESSING = "data_processing"
    MEMORY = "memory"
    VALIDATION = "validation"
    AUTHENTICATION = "authentication"
    UNKNOWN = "unknown"


class ErrorSeverity(Enum):
    """Severity levels for errors."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class UserError:
    """Represents a user-friendly error with recovery suggestions."""
    
    def __init__(
        self,
        title: str,
        message: str,
        category: ErrorCategory,
        severity: ErrorSeverity,
        recovery_suggestions: List[str] = None,
        technical_details: str = None
    ):
        self.title = title
        self.message = message
        self.category = category
        self.severity = severity
        self.recovery_suggestions = recovery_suggestions or []
        self.technical_details = technical_details
        self.timestamp = asyncio.get_event_loop().time()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for API/UI consumption."""
        return {
            'title': self.title,
            'message': self.message,
            'category': self.category.value,
            'severity': self.severity.value,
            'recovery_suggestions': self.recovery_suggestions,
            'technical_details': self.technical_details,
            'timestamp': self.timestamp
        }
    
    def format_for_ui(self) -> str:
        """Format error message for UI display."""
        emoji = self._get_emoji()
        return f"{emoji} {self.title}\n\n{self.message}\n\n{self._format_suggestions()}"
    
    def _get_emoji(self) -> str:
        """Get appropriate emoji for error severity."""
        emoji_map = {
            ErrorSeverity.INFO: "ℹ️",
            ErrorSeverity.WARNING: "⚠️",
            ErrorSeverity.ERROR: "❌",
            ErrorSeverity.CRITICAL: "🚨"
        }
        return emoji_map.get(self.severity, "❓")
    
    def _format_suggestions(self) -> str:
        """Format recovery suggestions for display."""
        if not self.recovery_suggestions:
            return ""
        
        suggestions = "\n".join(f"• {suggestion}" for suggestion in self.recovery_suggestions)
        return f"💡 Try these solutions:\n{suggestions}"


class ErrorHandler:
    """Central error handling and user messaging system."""
    
    def __init__(self):
        self.error_patterns = self._initialize_error_patterns()
        self.recent_errors = []
        self.max_recent_errors = 50
    
    def _initialize_error_patterns(self) -> Dict[str, Dict[str, Any]]:
        """Initialize common error patterns and their user-friendly messages."""
        return {
            # Network errors
            'connection_timeout': {
                'category': ErrorCategory.NETWORK,
                'severity': ErrorSeverity.WARNING,
                'title': 'Connection Timeout',
                'message': 'The server took too long to respond. This might be due to a slow internet connection or server issues.',
                'suggestions': [
                    'Check your internet connection',
                    'Try again in a few moments',
                    'Contact support if the problem persists'
                ]
            },
            'connection_refused': {
                'category': ErrorCategory.NETWORK,
                'severity': ErrorSeverity.ERROR,
                'title': 'Connection Failed',
                'message': 'Unable to connect to the data server. The service might be temporarily unavailable.',
                'suggestions': [
                    'Check your internet connection',
                    'Wait a few minutes and try again',
                    'Check if there are any announced service outages'
                ]
            },
            'dns_resolution_failed': {
                'category': ErrorCategory.NETWORK,
                'severity': ErrorSeverity.ERROR,
                'title': 'Server Not Found',
                'message': 'Could not find the data server. This might be a temporary network issue.',
                'suggestions': [
                    'Check your internet connection',
                    'Try using a different DNS server',
                    'Contact your network administrator'
                ]
            },
            
            # File upload errors
            'file_too_large': {
                'category': ErrorCategory.FILE_UPLOAD,
                'severity': ErrorSeverity.ERROR,
                'title': 'File Too Large',
                'message': 'The uploaded file exceeds the maximum allowed size of 50MB.',
                'suggestions': [
                    'Try compressing the file',
                    'Use a smaller dataset',
                    'Contact support for larger file limits'
                ]
            },
            'invalid_file_type': {
                'category': ErrorCategory.FILE_UPLOAD,
                'severity': ErrorSeverity.ERROR,
                'title': 'Invalid File Type',
                'message': 'Only .pkl (pickle) files are supported for upload.',
                'suggestions': [
                    'Ensure your file has a .pkl extension',
                    'Save your data in pickle format',
                    'Check the file format documentation'
                ]
            },
            'corrupted_file': {
                'category': ErrorCategory.FILE_UPLOAD,
                'severity': ErrorSeverity.ERROR,
                'title': 'Corrupted File',
                'message': 'The uploaded file appears to be corrupted or not a valid pickle file.',
                'suggestions': [
                    'Try re-saving the file',
                    'Verify the file was created correctly',
                    'Upload a different file'
                ]
            },
            
            # Data processing errors
            'invalid_data_format': {
                'category': ErrorCategory.DATA_PROCESSING,
                'severity': ErrorSeverity.ERROR,
                'title': 'Invalid Data Format',
                'message': 'The data format is not recognized or supported by the application.',
                'suggestions': [
                    'Check that your data follows the expected format',
                    'Verify column names and data types',
                    'Refer to the data format documentation'
                ]
            },
            'missing_required_columns': {
                'category': ErrorCategory.DATA_PROCESSING,
                'severity': ErrorSeverity.ERROR,
                'title': 'Missing Required Data',
                'message': 'Some required columns or data fields are missing from your dataset.',
                'suggestions': [
                    'Check the required data format specification',
                    'Ensure all mandatory columns are present',
                    'Verify your data source is complete'
                ]
            },
            
            # Memory errors
            'out_of_memory': {
                'category': ErrorCategory.MEMORY,
                'severity': ErrorSeverity.CRITICAL,
                'title': 'Out of Memory',
                'message': 'The application has run out of available memory. This usually happens with very large datasets.',
                'suggestions': [
                    'Try using a smaller dataset',
                    'Close other applications to free memory',
                    'Restart the application',
                    'Consider upgrading your system memory'
                ]
            },
            'memory_limit_exceeded': {
                'category': ErrorCategory.MEMORY,
                'severity': ErrorSeverity.WARNING,
                'title': 'Memory Limit Reached',
                'message': 'The application is approaching its memory limit. Some features may be limited.',
                'suggestions': [
                    'Clear cached data',
                    'Use smaller datasets',
                    'Restart the application to free memory'
                ]
            },
            
            # Validation errors
            'invalid_parameters': {
                'category': ErrorCategory.VALIDATION,
                'severity': ErrorSeverity.ERROR,
                'title': 'Invalid Parameters',
                'message': 'Some of the provided parameters are invalid or out of acceptable range.',
                'suggestions': [
                    'Check parameter values and ranges',
                    'Refer to the parameter documentation',
                    'Use default values if unsure'
                ]
            }
        }
    
    def handle_exception(self, exception: Exception, context: str = None) -> UserError:
        """Convert an exception to a user-friendly error."""
        try:
            # Get technical details
            technical_details = f"{type(exception).__name__}: {str(exception)}"
            if context:
                technical_details = f"{context} - {technical_details}"
            
            # Determine error pattern
            error_pattern = self._match_error_pattern(exception, technical_details)
            
            if error_pattern:
                user_error = UserError(
                    title=error_pattern['title'],
                    message=error_pattern['message'],
                    category=error_pattern['category'],
                    severity=error_pattern['severity'],
                    recovery_suggestions=error_pattern['suggestions'],
                    technical_details=technical_details
                )
            else:
                # Generic error for unmatched patterns
                user_error = UserError(
                    title='Unexpected Error',
                    message=f'An unexpected error occurred: {str(exception)}',
                    category=ErrorCategory.UNKNOWN,
                    severity=ErrorSeverity.ERROR,
                    recovery_suggestions=[
                        'Try the operation again',
                        'Restart the application if the problem persists',
                        'Contact support with the error details'
                    ],
                    technical_details=technical_details
                )
            
            # Log the error
            self._log_error(user_error, exception)
            
            # Store in recent errors
            self._store_recent_error(user_error)
            
            return user_error
            
        except Exception as e:
            # Fallback error handling
            logger.error(f"Error in error handler: {e}")
            return UserError(
                title='System Error',
                message='A system error occurred while processing your request.',
                category=ErrorCategory.UNKNOWN,
                severity=ErrorSeverity.CRITICAL,
                recovery_suggestions=['Restart the application', 'Contact technical support'],
                technical_details=str(exception)
            )
    
    def _match_error_pattern(self, exception: Exception, technical_details: str) -> Optional[Dict[str, Any]]:
        """Match exception to known error patterns."""
        exception_type = type(exception).__name__.lower()
        error_message = str(exception).lower()
        
        # Network errors
        if 'timeout' in error_message or 'timeouterror' in exception_type:
            return self.error_patterns['connection_timeout']
        elif 'connection' in error_message and ('refused' in error_message or 'failed' in error_message):
            return self.error_patterns['connection_refused']
        elif 'dns' in error_message or 'name resolution' in error_message:
            return self.error_patterns['dns_resolution_failed']
        
        # File upload errors
        elif 'file size' in error_message and 'exceeds' in error_message:
            return self.error_patterns['file_too_large']
        elif 'extension' in error_message and 'not allowed' in error_message:
            return self.error_patterns['invalid_file_type']
        elif 'pickle' in error_message or 'deserializ' in error_message:
            return self.error_patterns['corrupted_file']
        
        # Memory errors
        elif 'memory' in error_message or 'memoryerror' in exception_type:
            if 'limit' in error_message:
                return self.error_patterns['memory_limit_exceeded']
            else:
                return self.error_patterns['out_of_memory']
        
        # Data processing errors
        elif 'column' in error_message and ('missing' in error_message or 'not found' in error_message):
            return self.error_patterns['missing_required_columns']
        elif 'format' in error_message or 'schema' in error_message:
            return self.error_patterns['invalid_data_format']
        
        # Validation errors
        elif 'parameter' in error_message or 'value' in error_message:
            return self.error_patterns['invalid_parameters']
        
        return None
    
    def _log_error(self, user_error: UserError, original_exception: Exception):
        """Log error with appropriate level."""
        log_message = f"{user_error.title}: {user_error.message}"
        
        if user_error.severity == ErrorSeverity.CRITICAL:
            logger.critical(log_message, exc_info=original_exception)
        elif user_error.severity == ErrorSeverity.ERROR:
            logger.error(log_message, exc_info=original_exception)
        elif user_error.severity == ErrorSeverity.WARNING:
            logger.warning(log_message)
        else:
            logger.info(log_message)
    
    def _store_recent_error(self, user_error: UserError):
        """Store error in recent errors list."""
        self.recent_errors.append(user_error)
        
        # Keep only the most recent errors
        if len(self.recent_errors) > self.max_recent_errors:
            self.recent_errors = self.recent_errors[-self.max_recent_errors:]
    
    def get_recent_errors(self, category: ErrorCategory = None, severity: ErrorSeverity = None) -> List[UserError]:
        """Get recent errors with optional filtering."""
        errors = self.recent_errors
        
        if category:
            errors = [e for e in errors if e.category == category]
        
        if severity:
            errors = [e for e in errors if e.severity == severity]
        
        return errors
    
    def clear_recent_errors(self):
        """Clear the recent errors list."""
        self.recent_errors.clear()
        logger.info("Cleared recent errors list")


# Global error handler instance
error_handler = ErrorHandler()


async def display_error_message(state_instance, user_error: UserError, delay: float = 0.001):
    """Display error message to user with typing animation."""
    try:
        message = user_error.format_for_ui()
        state_instance.streamed_message = ''
        
        for char in message:
            await asyncio.sleep(delay)
            state_instance.streamed_message += char
            
    except Exception as e:
        logger.error(f"Failed to display error message: {e}")
        # Fallback to simple message
        state_instance.streamed_message = f"❌ Error: {user_error.title}"


def handle_state_error(exception: Exception, context: str = None) -> UserError:
    """Convenience function for handling errors in state classes."""
    return error_handler.handle_exception(exception, context)


class ErrorRecovery:
    """Provides automated error recovery suggestions and actions."""
    
    @staticmethod
    def suggest_network_recovery() -> List[str]:
        """Suggest network-related recovery actions."""
        return [
            "Check your internet connection",
            "Try refreshing the page",
            "Wait a moment and try again",
            "Clear browser cache and cookies"
        ]
    
    @staticmethod
    def suggest_memory_recovery() -> List[str]:
        """Suggest memory-related recovery actions."""
        return [
            "Close unnecessary browser tabs",
            "Restart the application",
            "Use a smaller dataset",
            "Clear cached data"
        ]
    
    @staticmethod
    def suggest_file_recovery() -> List[str]:
        """Suggest file-related recovery actions."""
        return [
            "Check file format and size",
            "Try re-saving the file",
            "Verify file is not corrupted",
            "Use a different file"
        ]