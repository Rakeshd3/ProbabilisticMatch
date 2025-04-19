# query_logic.py

import os
import pandas as pd
import numpy as np
import recordlinkage as rl
from recordlinkage.preprocessing import clean
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry

registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')


def run_matching_query(credentials, logger):
    """
    Load SQL, fetch data, perform matching, and return result set.
    """
    query_file = "cts_query_for_probmatch.txt"
    if not os.path.exists(query_file):
        logger.error(f"Query file not found: {query_file}")
        raise FileNotFoundError(query_file)

    with open(query_file, "r") as f:
        query = f.read()

    logger.info("Loaded SQL query")
    logger.debug(f"Query Preview:\n{query[:300]}...")

    # Build Snowflake connection string
    url = (
        f"snowflake://{credentials['user']}:{credentials['password']}@{credentials['account']}/"
        f"MY_DB/MY_SCHEMA?warehouse=MY_WH"
    )

    engine = create_engine(url)
    logger.info("Connecting to Snowflake...")

    # Simulate two queries
    df1 = pd.read_sql(query, con=engine)
    df2 = pd.read_sql(query, con=engine)

    logger.info(f"Fetched {len(df1)} and {len(df2)} records")

    df1.fillna("none", inplace=True)
    df2.fillna("none", inplace=True)

    indexer = rl.Index()
    indexer.block("NAME")
    candidate_links = indexer.index(df1, df2)

    compare = rl.Compare()
    compare.string("NAME", "NAME", method="levenshtein", label="name_sim")
    compare.exact("DOB", "DOB", label="dob_exact")

    features = compare.compute(candidate_links, df1, df2)
    logger.info("Matching completed")

    results = features[features.sum(axis=1) > 1]
    return results.reset_index().to_dict(orient="records")
