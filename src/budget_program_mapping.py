from __future__ import annotations

import pandas as pd

KEYWORD_TO_PROGRAM = {
    "program=cloud_migration": "Cloud Migration Program",
    "program=cybersecurity": "Cybersecurity Program",
    "program=data_platform": "Data Platform Program",
    "program=airport_automation": "Airport Automation Program",
    "program=network_modernization": "Network Modernization Program",
}


def derive_budget_program(details: pd.Series) -> pd.Series:
    def match_one(text: str) -> str:
        value = str(text).lower()
        for keyword, program in KEYWORD_TO_PROGRAM.items():
            if keyword in value:
                return program
        return "other"

    return details.astype(str).map(match_one)
