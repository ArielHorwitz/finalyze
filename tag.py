import polars as pl

import utils
from source import get_source_data

TAG_SCHEMA = {
    "tag1": pl.String,
    "tag2": pl.String,
    "hash": pl.UInt64,
}


def add_subparser(subparsers):
    parser = subparsers.add_parser("tag", help="Tag source data")
    parser.set_defaults(func=run)
    parser.add_argument(
        "--auto-cache",
        action="store_true",
        help="Automatically use cached values for tags",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear saved tags and quit",
    )
    parser.add_argument(
        "--path",
        action="store_true",
        help="Print path to tags file and quit",
    )


def run(args):
    tags_file = args.tags_file
    auto_cache = args.auto_cache
    clear = args.clear
    flip_rtl = args.flip_rtl
    print_path = args.path
    if print_path:
        print(tags_file)
        return
    if clear:
        if tags_file.is_file():
            tags_file.replace(f"{tags_file}.bak")
        return
    if not tags_file.is_file():
        tags_file.write_text(",".join(TAG_SCHEMA.keys()))
    source_data = get_source_data(args)
    write_tags(
        source_data,
        tags_file=tags_file,
        auto_cache=auto_cache,
    )


def apply_tags(data, tags_file):
    hash_column = pl.concat_str(("date", "amount")).hash()
    data = data.with_columns(hash_column.alias("hash"))
    tags = pl.read_csv(tags_file, schema=TAG_SCHEMA)
    return data.join(tags, on="hash", how="left")


def write_tags(source_data, tags_file, auto_cache: bool = False):
    source_data = apply_tags(source_data, tags_file)
    existing_tags = pl.read_csv(tags_file, schema=TAG_SCHEMA)
    untagged_indices = source_data["tag1"].is_null().arg_true()
    new_hashes = []
    new_tag1 = []
    new_tag2 = []
    cached_map = {}
    for row in source_data.iter_rows(named=True):
        if row["tag1"]:
            cached_map[row["description"]] = (row["tag1"], row["tag2"])
    cached1, cached2 = None, None
    cached_repr = ""
    for index in untagged_indices:
        row = source_data.row(index, named=True)
        row_hash = row["hash"]
        description = row["description"]
        date = row["date"]
        amount = row["amount"]
        cached_values = cached_map.get(description)
        skip_caching = False
        if cached_values is not None:
            cached1, cached2 = cached_values
            cached_repr = f"[{cached1} / {cached2}]"
        else:
            cached1, cached2 = None, None
            cached_repr = ""
        print("Start with '-' to skip caching this entry.")
        print("Up to 2 tags (comma-separated).")
        print("Enter 'quit' to finish.")
        print()
        print(f"Applying tags for:")
        print(f"    Date: {date}")
        print(f"  Amount: {amount}")
        print(f"    Desc: {description}")
        print()
        if cached_values is not None and auto_cache:
            tag1 = cached1
            tag2 = cached2
            print(f"Using cached values: {cached_repr}")
        else:
            if cached_values is not None:
                print(f"Cached as: {cached_repr}")
            tags = input(f"Tags: ")
            cache_shoot_and_miss = tags == "" and cached_values is None
            if tags == "quit" or cache_shoot_and_miss:
                break
            elif tags == "":
                tag1 = cached1
                tag2 = cached2
            else:
                if tags.startswith("-"):
                    tags = tags[1:]
                    skip_caching = True
                if "," in tags:
                    tag1, tag2 = tags.split(",", 1)
                    tag1 = tag1.strip()
                    tag2 = tag2.strip()
                else:
                    tag1 = tags
                    tag2 = ""
        if not skip_caching:
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
