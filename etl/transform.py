import pandas as pd

def transform_indicators(data):
    df = pd.DataFrame(data)

    df_main = df.drop(columns=['qualityBySpace'], errors="ignore")

    cols_int = [
        "done", "notApply", "notDone",
        "notEvaluatedOnTime", "notEvaluatedOffTime",
        "code"
    ]

    for col in cols_int:
        df_main[col] = pd.to_numeric(df_main[col], errors="coerce")

    df_main["executionDate"] = pd.to_datetime(
        df_main["executionDate"],
        dayfirst=True,
        errors="coerce"
    )

    cols_float = ["onTime", "quality", "rework"]

    for col in cols_float:
        df_main[col] = pd.to_numeric(df_main[col], errors="coerce")
    
    return df_main

def transform_quality(data):

    df = pd.DataFrame(data)
    rows = []

    for _, row in df.iterrows():
        qspaces = row.get("qualityBySpace")

        if not qspaces:
            continue

        if isinstance(qspaces, dict):
            for space_id, space_data in qspaces.items():
                rows.append({
                    "store_id": row["store_id"],
                    "space_id": space_id,
                    **space_data
                })

        elif isinstance(qspaces, list):
            for space_data in qspaces:
                rows.append({
                    "store_id": row["store_id"],
                    **space_data
                })
    

    df_quality = pd.DataFrame(rows)

    cols_float = ["star", "quality", "notApply"]

    for col in cols_float:
        df_quality[col] = pd.to_numeric(df_quality[col], errors="coerce")

    df_quality = df_quality.replace("", None)

    return df_quality
