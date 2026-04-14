from rich.console import Console, RenderableType
from rich.logging import RichHandler
from rich.abc import RichRenderable
from rich.table import Table
from rich.text import Text

from typing import Optional
from io import StringIO

import logging

def create_rich_table(title: str, columns: list[str], rows: list[list[str]]) -> Table:
    """Create a Rich table with given title, columns, and rows
    """
    table = Table(title=title)
    for col in columns:
        table.add_column(col)
    for row in rows:
        table.add_row(*row)
    return table

def log_table(title: str, columns: list[str], rows: list[list[str]]):
    return create_rich_table(title, columns, rows)

class RenderableHandler(RichHandler):
    def render_message(self, record, message):
        if isinstance(message, RichRenderable):
            return message
        else:
            return super().render_message(record, message)

class RenderableFormatter(logging.Formatter):
    def format(self, record):
        if isinstance(record.msg, RichRenderable):
            return record.msg
        else:
            return super().format(record)

class RichFileFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='%', validate=True, **kwargs):
        super().__init__(fmt, datefmt, style, validate)
        # Initialize a rich Console for rendering to a string buffer.
        # - file=StringIO() redirects output to an in-memory string.
        # - force_terminal=False and no_color=True ensure plain text output without ANSI codes.
        # - width can be adjusted to control how wide tables/panels are rendered in the log file.
        self._console = Console(file=StringIO(), force_terminal=False, no_color=True, width=120)

    def format(self, record):
        # Store the original message to restore it later, preventing side effects
        original_msg = record.msg
        try:
            # If the message is a rich renderable object
            if isinstance(original_msg, RenderableType):
                # Capture the rich object's rendered string
                with self._console.capture() as capture:
                    # Print the rich object to our internal console, soft_wrap helps with long lines
                    self._console.print(original_msg, soft_wrap=True)
                rendered_string = capture.get()
                # Replace the record's message with the rendered string, removing trailing newlines
                record.msg = rendered_string.strip()

            # Now, let the standard formatter handle the (potentially modified) record
            return super().format(record)
        finally:
            # Always restore the original message
            record.msg = original_msg

def get_rich_logger(
    name: Optional[str] = None, 
    level: int = logging.INFO,
    log_file_name: str = "app_log",
    logger_format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
) -> logging.Logger:
    """Get a logger that can handle Rich renderables in both console and file outputs.
    """
    logger = logging.getLogger(name)
    
    
    rich_file_formatter = RichFileFormatter(logger_format)
    renderable_formatter = RenderableFormatter(logger_format)

    file_handler = logging.FileHandler(log_file_name)
    file_handler.setFormatter(rich_file_formatter)
    logger.addHandler(file_handler)

    handler = RenderableHandler(show_path=False)
    handler.setFormatter(renderable_formatter)
    logger.addHandler(handler)

    logger.setLevel(level)

    return logger