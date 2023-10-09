"""Modulo con recursos compartidos."""
import os
import great_expectations as gx
from azure_datalake_utils import Datalake


datalake = Datalake.from_account_key(os.environ.get("datalake"), os.environ.get("datalake_key"))
context = gx.get_context()
