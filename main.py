# importing required library
import logging
from logging.config import dictConfig
from logging_handlers import *
from credentials import *

dictConfig(logging_config)

logging.basicConfig(filename='logfile.log', level=logging.DEBUG, filemode='w',
                    format=("[%(asctime)s] %(levelname)s "
                            "[%(name)s:%(lineno)s] %(message)s"))
logger = logging.getLogger('program_logger')
handler = logging.FileHandler('logfile.log')
logger.addHandler(handler)

try:
    import time
    from sqlalchemy import create_engine
    import pandas as pd
    import numpy as np
    import spacy
    from sqlalchemy.dialects import registry
    from queries import *
    import recordlinkage as rl
    from recordlinkage.preprocessing import clean
    from sqlalchemy.sql import text as sa_text
    from tqdm import tqdm as tdm
    import csv
    import os
    import datetime

except Exception as e:
    logging.exception(e)


else:
    registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
    logger.info('libraries imported successfully')

# print(url)
params = dict(threshold=0.95, method_str='levenshtein', method_num='step', scale=5, offset=5)


def creation_of_dataframe(queries, url):
    engine = create_engine(url, echo=False)
    keys = [k for k in queries.keys()]
    dfs = []
    for key in keys:
        dfs.append(pd.read_sql(queries[key], con=engine))
    return dfs[0], dfs[1]


def cleaning_df(df):
    df.fillna('none', inplace=True)

    date_cols = [x for x in df if 'date' in x]
    cat_cols = [x for x in df.columns if ('name' in x) or (x.endswith("city"))]
    numerical_cols = df.columns[~df.columns.isin(cat_cols)]

    try:
        for col in df.columns:
            if col not in date_cols:
                df[col] = df[col].apply(
                    lambda x: x.replace('"', '') if ((x != 'none') & (not isinstance(x, int)) & (x != None)) else x)
            else:
                df[col] = df[col].apply(lambda x: pd.to_datetime(x))
    except Exception as error:
        logging.exception(error)

    try:
        for col in numerical_cols:
            if (col not in cat_cols) and (col not in date_cols):
                df[col] = df[col].apply(lambda x: int(x) if isinstance(x, float) else x)
            else:
                if 'zip' and 'store' in col:
                    df[col] = df[col].apply(lambda x: int(x) if x.isnumeric() else x.replace("none", 99))
                    df[col] = df[col].astype("int")
                else:
                    if col not in date_cols:
                        df[col] = df[col].apply(lambda x: x.replace('"', '') if x.isalpha() else 'none')
                    else:
                        df[col] = df[col].apply(
                            lambda x: pd.to_datetime(x) if isinstance(x, datetime.datetime) else x.replace('none',
                                                                                                           pd.to_datetime(
                                                                                                               np.nan)))
    except Exception as error:
        print(error)
    # clean left and right spaces in categorical columns
    for col in cat_cols:
        df[col] = df[col].str.lstrip(" ").str.rstrip(" ").str.strip(" ")
        df[col] = df[col].apply(lambda x: ' '.join(str(x).split()))
        df[col] = df[col].replace('', 'none')
        df[col] = df[col].apply(lambda x: x.lower())
    try:
        for col in df.columns:
            if 'user_name' in col:
                df.msrname = np.where(df.msrname == 'none', df.user_name, df.msrname)
    except AttributeError as e:
        logging.exception(e)
        # resetting user name as it is to avoid failure
    df = df[df.msrname != 'none']
    # final cleaning for categorical columns
    try:
        for col in cat_cols:
            df[col] = df[col].str.replace(r'[^\w\s-]+', '', regex=True).str.replace(
                r'[!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~]+', ' ', regex=True)  # eliminate back slash,underscore    symbols
            mask = df.loc[:, col].str.contains(r"^[a-zA-Z\s-]*$", np.nan, regex=True, na=False)
            df[col] = df.loc[mask, col]
    except Exception as error:
        logging.exception(error)

        # nlp implementation to remove bad names
    try:
        nlp = spacy.load("en_core_web_sm")
    except Exception as error:
        spacy.cli.download("en")
        nlp = spacy.load("en_core_web_sm")
        # identify columns that contain person names
    name_cols = []
    for col in cat_cols:
        # get 10 values in column
        el = df[df[col].notnull()][col]  # pick values that are notNa
        el = np.random.permutation(el)  # shuffle the values
        el = el[:10].tolist()  # pick 10 first elements and convert to list
        # create a string with sequence of 10 first values
        string = ''
        for word in el:
            try:  # try method to avoid error for numerical columns
                string += word + ' '
            except Exception as e:
                logging.exception(e)
                # check how many string elements are person names
            if len(string) > 0:
                isname = nlp(string)
                names_list = [token for token in isname if token.ent_type_ == 'PERSON']
                if len(names_list) > 3:  # add column if at least 5 names (we're checking 10) were found
                    name_cols.append(col)
    name_cols = [x for x in name_cols if
                 'user' not in x.lower() and 'email' not in x.lower() and 'city' not in x.lower()]
    if len(name_cols) == 0:
        name_cols = [col for col in df.columns if 'name' in col.lower()]
        name_cols = [x for x in name_cols if
                     'user' not in x.lower() and 'email' not in x.lower() and 'city' not in x.lower()]
    wildcard = set(
        ['credit', 'debit', 'card', 'debito', 'credito', 'mastercard', 'visa', 'customer', 'value', 'cardmember',
         'valuelink', 'gift', 'you', 'a', 'instant', 'issue', 'branded', 'biolife', 'cls', 'happy', 'holidays', 'thank',
         'gc', 'tanger', 'outlet', 'malls',
         'plasma', 'donor', 'vl', 'christmastreeshops', 'preferred', 'custom', 'valued', 'client', 'name', 'sort',
         'our', 'neighbor'])
    for col in name_cols:
        df[col].fillna('none', inplace=True)  # input -99 to nan values
        df.loc[:, 'words_found'] = [','.join(sorted(wildcard.intersection(l)))
                                    for l in df[col].str.lower().str.split()]
        df.loc[:, 'found'] = df.loc[:, 'words_found'].astype(bool).astype(int)
        df = df[(df['found'] == 0)]
        # drop additional columns
        df.drop(['words_found', 'found'], axis=1, inplace=True)

    df = df[df.msrname != 'none']
    # model friendly cleaning
    try:
        for col in df.columns:
            if col in cat_cols:
                df[col] = clean(df[col])

    except Exception as e:
        logging.exception(e)

    for col in df.columns:
        if 'store_zip' in col:
            df[col] = df[col].replace("none", 99)
            df[col] = df[col].fillna(99)
            df[col] = df[col].astype(int)

    return df


def prob_match(pd_dfs, url, threshold=None, method_str=None, method_num=None, scale=None, offset=None):
    df1, df2 = creation_of_dataframe(pd_dfs, url)
    # cleaning object cols for model redeability
    df1 = cleaning_df(df1)
    df2 = cleaning_df(df2)

    logger.info(f"Matched Datasets Shape After Cleaning:{df1.shape[0]}")
    logger.info(f"UnMatched Datasets Shape After Cleaning:{df2.shape[0]}")
    # resetiing index to core customerids of respective datasets

    df_v1 = df1.set_index('CUSTOMER_ID'.lower())

    df_v2 = df2.set_index('CUSTOMER_ID'.lower())

    ## creating mathced indexes using SoretdNeighbourHood Approach
    clx = rl.index.SortedNeighbourhood('MSRNAME'.lower(), window=5)
    clx = clx.index(df_v1, df_v2)
    try:
        if (method_str == None) or (method_num == None) or (threshold == None) or (scale == None) or (offset == None):
            ## comparing
            cr = rl.Compare()
            cr.string('MSRNAME'.lower(), 'MSRNAME'.lower(), method='jaro', threshold=0.85, label='MSRNAME'.lower())
            cr.string('STORE_CITY'.lower(), 'STORE_CITY'.lower(), method='jaro_winkler', threshold=0.85,
                      label='STORE_CITY'.lower())
            cr.numeric('STORE_ZIP'.lower(), 'STORE_ZIP'.lower(), scale=1, offset=10, label='STORE_ZIP'.lower())
            cr.numeric('STORE'.lower(), 'STORE'.lower(), method='linear', scale=1,
                       offset=10, label='Store'.lower())
            feature_vectors = cr.compute(clx, df_v1, df_v2)
            pred = feature_vectors[feature_vectors.sum(axis=1) > 3.8]
            logging.info('Completed Algorithm Prediction With Default Parameters.')
            return feature_vectors, pred, df1, df2
        else:
            ## comparing
            try:
                cr = rl.Compare()
                cr.string('MSRNAME'.lower(), 'MSRNAME'.lower(), method=method_str, threshold=threshold,
                          label='MSRNAME'.lower())
                cr.string('STORE_CITY'.lower(), 'STORE_CITY'.lower(), method=method_str, threshold=threshold,
                          label='STORE_CITY'.lower())
                cr.numeric('STORE_ZIP'.lower(), 'STORE_ZIP'.lower(), scale=scale, offset=offset,
                           label='STORE_ZIP'.lower())
                cr.numeric('STORE'.lower(), 'STORE'.lower(), method=method_num, scale=scale,
                           offset=offset, label='Store')
                feature_vectors = cr.compute(clx, df_v1, df_v2)
            except Exception as error:
                logging.exception(error)
            else:
                if threshold <= 0.6:
                    pred = feature_vectors[feature_vectors.sum(axis=1) > round(threshold * 6, 1)]
                else:
                    pred = feature_vectors[feature_vectors.sum(axis=1) > round(threshold * 4, 1)]
                    logging.info('Completed Algorithm Prediction With Given Parameters.')
                return feature_vectors, pred, df1, df2
    except Exception as error:
        logging.exception(error)


def Col_Name_Change(df):
    df.columns = ['core_customer_id	transaction_date	transaction_number'.split('\t')]
    return df


def getting_shape(query, url):
    engine = create_engine(url, echo=False)
    shape_unmtch = pd.read_sql(query, con=engine)
    shape = shape_unmtch.values[0][0]
    try:
        ls = []
        for i in range(1, 100):

            if shape % i == 0:
                ls.append(i)
    except Exception as e:
        logger.error(e)

    return shape, max(ls)


def creating_match_table(pred, df1, df2, query, query2, url_sor_prod):
    data_indexes = pred.reset_index()

    df_v1 = data_indexes.merge(df1, left_on="CUSTOMER_ID_1".lower(), right_on="CUSTOMER_ID".lower())
    df_v2 = data_indexes.merge(df2, left_on="CUSTOMER_ID_2".lower(), right_on="CUSTOMER_ID".lower())

    df_v3 = df_v1[['customer_id_1', 'customer_id_2', 'core_customer_id']]
    df_v4 = df_v2[['customer_id_1', 'customer_id_2', 'txn_date', 'transaction_number']]

    ## final data frame creation
    df_final = df_v3.merge(df_v4, on=["CUSTOMER_ID_1".lower(), "CUSTOMER_ID_2".lower()], how="left")
    logger.info(f"probmatch  dataset shape :{df_final.shape[0]}")
    # matching with original unmatched population to get all transactions associated with matched people "SOR_PROD"."CTS"."CTS_EVENTS_ANALYTICS" where unmatched_flag='Y'
    shape, max_div = getting_shape(query2, url_sor_prod)

    engine = create_engine(url_sor_prod, echo=False)

    chunksize = (shape // max_div)
    df_unmatch_raw = pd.read_sql(query, con=engine, chunksize=chunksize)
    # print(df_final.shape[0])
    # Data pull from unmatch view from sor_prod
    df_all_unmatch_transactions = pd.concat(df_unmatch_raw)
    # filtering transaction_number mapped with customer_id that is found after probabilistic matching
    df_all_unmatch_transactions_with_prob_match_users = df_all_unmatch_transactions[
        df_all_unmatch_transactions.customer_id.isin(df_final.customer_id_2)]

    # final merging to get required results
    df_final_v1 = df_final.merge(df_all_unmatch_transactions_with_prob_match_users, left_on='customer_id_2',
                                 right_on='customer_id')
    # filtering only required columns
    df_final_v2 = df_final_v1[['core_customer_id_x', 'trasaction_date', 'transaction_number_y']]
    # removing duplicate transaction_number row level mapped with core_customer_id
    df_final_v2.drop_duplicates(subset=['transaction_number_y'], keep='first', inplace=True)

    logger.info(
        f"probmatch dataset shape after mapping transaction_number with original unmatch dataset :{df_final_v2.shape[0]}")
    # column name change as per discussed requirement
    df_final = Col_Name_Change(df_final_v2)
    # Adding a system generated column with todays date
    df_final['date_updated'] = datetime.datetime.today()

    return df_final


def creating_csv_gz(df, output_name):
    # converting to .gz
    df.to_csv(output_name, sep=',',
              index=False, quoting=csv.QUOTE_NONNUMERIC, compression="gzip")


def running_queries(url, queries):
    engine = create_engine(url, echo=True)
    keys = [k for k in queries.keys()]
    for idx in range(len(queries)):
        try:
            with engine.connect() as conn:
                if keys[idx] == 'truncate' or keys[idx] == 'remove':
                    print(keys[idx])
                    conn.execute(
                        sa_text(queries[keys[idx]]).execution_options(autocommit=True, supports_statement_cache=False))

                else:
                    conn.execute(queries[keys[idx]])
        except Exception as e:

            logger.exception(e)

        else:

            logger.info(f'completed running query for {keys[idx]}'.title())


class Main:
    @staticmethod
    def run():
        try:
            start = time.time()
            logger.info("*******Debuging Program*********")

            _, prediction, df1, df2 = prob_match(queries, url_hubuser, **params)

            df_final = creating_match_table(prediction, df1, df2, query, count_of_unmatch, url_sor_prod)
            logger.info(
                f"No of records to be inserted into {TABLE_NAME_HUBUSER} & {TABLE_NAME_SOR} are :{df_final.shape[0]}")

            creating_csv_gz(df_final, FILE_NAME)
            # creating a backup in hubuser

            running_queries(url_hubuser, table_creation_queries_hubuser)

            # truncating existing table sor.cts.cts_prob_match_results and inserting new data into the table in sor.cts.cts_prob_match_results
            # running_queries(url_sor,table_copying_queries_sor_final)

            # Validation Check
            hub_user_df = pd.read_sql(hub_user_new_table, con=url_hubuser)
            sor_df = pd.read_sql(sor_user_new_table, con=url_sor)

            if hub_user_df.shape[0] == sor_df.shape[0]:
                logger.info(f"counts are matching sor,hub_user {sor_df.shape[0]}={hub_user_df.shape[0]}")
            else:
                logger.error(f"counts are not matching sor,hub_user:{sor_df.shape[0]}!= {hub_user_df.shape[0]}")
            # removing created zip file
            os.remove(os.path.abspath(FILE_NAME))
            logger.info(f"created gunzip file for table creation deleted")

            end = time.time()
            elapsed = (end - start)
            format_time = time.strftime("%H:%M:%S", time.gmtime(elapsed))

        except Exception as e:
            logger.exception(e)
        else:
            logger.info(f"Execution time: {format_time}")
            logger.info("*******Debuging Completed*********")


Main.run()



