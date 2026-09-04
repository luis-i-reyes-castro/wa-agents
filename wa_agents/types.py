"""
Base Types
"""

from pydantic import Field
from typing import Annotated


type HexHash     = Annotated[ str, Field( pattern = r"^[A-Fa-f0-9]+$")]
""" Hexadecimal hash """

type NE_str      = Annotated[ str, Field( pattern = r"^[^\s].+")]
""" Non-empty string (at least 2 chars, first char cannot be whitespace) """

type NE_var_name = Annotated[ str, Field( pattern = r"^[A-Za-z\_]\w+$")]
""" Non-empty variable name (at least 2 chars) """

type NumericID   = Annotated[ str, Field( pattern = r"^[0-9]+$")]
""" Numeric ID """

type UnixTS      = Annotated[ str, Field( pattern = r"^[1-9][0-9]*$")]
""" Unix timestamp """
