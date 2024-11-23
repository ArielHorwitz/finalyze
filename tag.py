import polars as pl

TAG_SCHEMA = {
    "tag1": pl.String,
    "tag2": pl.String,
    "hash": pl.UInt64,
}


def write_tags(historical_data, tags_file, auto_cache: bool = False):
    existing_tags = pl.read_csv(tags_file, schema=TAG_SCHEMA)
    untagged_indices = historical_data["tag1"].is_null().arg_true()
    new_hashes = []
    new_tag1 = []
    new_tag2 = []
    cached_map = {}
    for row in historical_data.iter_rows(named=True):
        if row["tag1"]:
            cached_map[row["description"]] = (row["tag1"], row["tag2"])
    cached1, cached2 = None, None
    cached_repr = ""
    for index in untagged_indices:
        row = historical_data.row(index, named=True)
        row_hash = row["hash"]
        description = row["description"]
        date = row["date"]
        amount = row["balance"]
        cached_values = cached_map.get(description)
        if cached_values is not None:
            cached1, cached2 = cached_values
            cached_repr = f"[{cached1} / {cached2}]"
        else:
            cached1, cached2 = None, None
            cached_repr = ""
        print(f"[{description[:30]:<30}] {date}  {amount:<10} {cached_repr}")
        if cached_values is not None and auto_cache:
            tag1 = cached1
            tag2 = cached2
            print(f"Using cached values: {cached_repr}")
        else:
            tags = input(f"Tags (comma-separated, 'quit' to finish): ")
            cache_shoot_and_miss = tags == "" and cached_values is None
            if tags == "quit" or cache_shoot_and_miss:
                break
            elif tags == "":
                tag1 = cached1
                tag2 = cached2
            else:
                if ',' in tags:
                    tag1, tag2 = tags.split(',', 1)
                    tag1 = tag1.strip()
                    tag2 = tag2.strip()
                else:
                    tag1 = tags
                    tag2 = ""
        cached_map[description] = (tag1, tag2)
        new_hashes.append(row_hash)
        new_tag1.append(tag1)
        new_tag2.append(tag2)

    new_tag_data = {
        "hash": new_hashes,
        "tag1": new_tag1,
        "tag2": new_tag2,
    }
    new_tags = pl.DataFrame(new_tag_data, schema=TAG_SCHEMA)
    all_tags = pl.concat([existing_tags, new_tags])
    all_tags = all_tags.unique(subset="hash", keep="last")
    all_tags.sort("tag1", "tag2", "hash").write_csv(tags_file)


def tag_transactions(historical_data, tags_file, auto_cache: bool = False, tag_missing: bool = False):
    hash_column = pl.concat_str(("date", "balance")).hash()
    historical_data = historical_data.with_columns(hash_column.alias("hash"))
    tagged_data = identify_transactions(historical_data, tags_file)
    if tag_missing:
        write_tags(tagged_data, tags_file, auto_cache)
        tagged_data = identify_transactions(historical_data, tags_file)
    return tagged_data


def identify_transactions(data, tags_file):
    if not tags_file.is_file():
        tags_file.write_text(",".join(TAG_SCHEMA.keys()))
    tags = pl.read_csv(tags_file, schema=TAG_SCHEMA)
    return data.join(tags, on="hash", how="left")
