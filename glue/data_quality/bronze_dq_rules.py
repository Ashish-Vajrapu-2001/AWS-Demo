# To be included in Glue Jobs or imported
dq_ruleset_template = """
Rules = [
    RowCount > 0,
    IsComplete "{pk_col}",
    Uniqueness "{pk_col}"
]
"""
# This logic is integrated into the orchestration flow via DLQ and Glue evaluation results