import polars as pl

TAG_SCHEMA = {
    "hash": pl.UInt64,
    "category1": pl.String,
    "category2": pl.String,
}


def write_tags(historical_data, tags_file):
    existing_tags = pl.read_csv(tags_file, schema=TAG_SCHEMA)
    untagged_indices = historical_data["category1"].is_null().arg_true()
    new_hashes = []
    new_category1 = []
    new_category2 = []
    for index in untagged_indices:
        row = historical_data.row(index, named=True)
        row_hash = row["hash"]
        description = row["description"][:30]
        date = row["date"]
        amount = row["balance"]
        print(f"[{description:<30}] {date}  {amount:<10}")
        category1 = input(f"Category 1: ")
        if category1 == "":
            break
        category2 = input(f"Category 2: ")
        new_hashes.append(row_hash)
        new_category1.append(category1)
        new_category2.append(category2)

    new_tag_data = {
        "hash": new_hashes,
        "category1": new_category1,
        "category2": new_category2,
    }
    new_tags = pl.DataFrame(new_tag_data, schema=TAG_SCHEMA)
    all_tags = pl.concat([existing_tags, new_tags])
    all_tags = all_tags.unique(subset="hash", keep="last")
    all_tags.write_csv(tags_file)


def tag_transactions(historical_data, tags_file, user_tagging):
    hash_column = pl.concat_str(("date", "balance")).hash()
    historical_data = historical_data.with_columns(hash_column.alias("hash"))
    tagged_data = identify_transactions(historical_data, tags_file)
    if user_tagging:
        write_tags(tagged_data, tags_file)
        tagged_data = identify_transactions(historical_data, tags_file)
    return tagged_data


def identify_transactions(data, tags_file):
    if not tags_file.is_file():
        tags_file.write_text(",".join(TAG_SCHEMA.keys()))
    tags = pl.read_csv(tags_file, schema=TAG_SCHEMA)
    return data.join(tags, on="hash", how="left")
