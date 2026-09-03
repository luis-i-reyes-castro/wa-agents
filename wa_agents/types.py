"""
Base Types
"""

from pydantic import Field
from typing import Annotated


type NE_str      = Annotated[ str, Field( min_length = 2)]
""" Non-empty string (at least 2 chars) """

type NE_var_name = Annotated[ str, Field( pattern = r"^[A-Za-z\_]\w+$")]
""" Non-empty variable name (at least 2 chars) """
