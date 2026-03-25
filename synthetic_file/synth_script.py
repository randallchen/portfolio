"""
Synthetic e-commerce customer dataset generator
Produces 10,000 records → customers_raw.parquet

Install dependencies first:
    pip install faker pandas pyarrow
"""

import random
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from faker import Faker

fake = Faker("en_US")
Faker.seed(42)
random.seed(42)

NUM_RECORDS = 10_000
NOW = datetime.now(timezone.utc)


def random_past_datetime(start_year: int = 2018) -> datetime:
    return fake.date_time_between(
        start_date=datetime(start_year, 1, 1),
        end_date=NOW,
        tzinfo=timezone.utc,
    )


def maybe_child_birthday() -> Optional[str]:
    """~40% of customers have a child birthday on file."""
    if random.random() < 0.40:
        return fake.date_of_birth(minimum_age=0, maximum_age=17).isoformat()
    return None


def build_record() -> dict:
    created_at = random_past_datetime(start_year=2018)
    # updated_at is always >= created_at
    updated_at = fake.date_time_between(
        start_date=created_at,
        end_date=NOW,
        tzinfo=timezone.utc,
    )

    return {
        # --- identifiers ---
        "customer_id": fake.uuid4(),
        "external_id": fake.bothify(text="CUST-########"),

        # --- demographics ---
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.unique.email(),
        "phone": fake.phone_number(),

        # --- location ---
        "city": fake.city(),
        "state": fake.state_abbr(),
        "country": "US",
        "postal_code": fake.postcode(),

        # --- child birthday (nullable) ---
        "child_birthday": maybe_child_birthday(),

        # --- timestamps ---
        "created_at": created_at,
        "updated_at": updated_at,
    }


def main() -> None:
    print(f"Generating {NUM_RECORDS:,} records...")
    records = [build_record() for _ in range(NUM_RECORDS)]

    df = pd.DataFrame(records)

    # Enforce clean dtypes before writing
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    df["child_birthday"] = pd.to_datetime(df["child_birthday"], errors="coerce")

    output_path = "customers_raw.parquet"
    df.to_parquet(output_path, index=False, engine="pyarrow")

    print(f"Saved → {output_path}")
    print(f"Shape : {df.shape[0]:,} rows × {df.shape[1]} columns")
    print("\nColumn dtypes:")
    print(df.dtypes.to_string())
    print("\nSample (3 rows):")
    print(df.head(3).to_string())
    print(f"\nChild birthday coverage: {df['child_birthday'].notna().mean():.1%} of records")


if __name__ == "__main__":
    main()