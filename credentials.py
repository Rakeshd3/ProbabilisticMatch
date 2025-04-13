# for pandas
import datetime

# date time string
today = datetime.datetime.today()
str_formatted_date = today.strftime("%m_%d_%Y")

# user credential
SNOW_SQL_USER = """probmatchuser"""
SNOW_SQL_PASSWORD = """Wtn1a$29"""

# View Schema created from SOR production account
DATABASE_SOR_PROD = """SOR_PROD"""
SCHEMA_SOR_PROD = """CTS"""

# Table creation
STAGE_NAME = """cts"""
FILE_NAME = f'''probabilistic_match_result_{str_formatted_date}.csv.gz'''
TABLE_NAME_HUBUSER = f'''cts_prob_match_results_{str_formatted_date}'''
TABLE_NAME_SOR = f'''cts_prob_match_results'''
FORMAT_NAME = '''csv'''

# Hubuser Account
SNOW_SQL_ACCOUNT_HUBREADER = """zeta_hub_reader.us-east-1"""
DATABASE_HUBUSER = '''HUBUSERS'''
SCHEMA_HUBUSER = """DATA_SCIENCE"""
USER_ROLE_HUBUSER = """DATA_SCIENCE_ROLE"""
SNOW_SQL_WAREHOUSE = """DATA_SCIENCE_WH"""

# Prod Account
SNOW_SQL_ACCOUNT_PROD = """zetaglobal.us-east-1"""
DATABASE_SOR = """SOR"""
SCHEMA_SOR = """CTS"""
USER_ROLE_SOR = """SOR_CTS_ROLE"""
SNOW_SQL_WAREHOUSE_SOR = """SOR_WH"""

# Connection String for creating back up table at hubusers.data_science
url_hubuser = 'snowflake://{user}:{password}@{account_identifier}/{database}/{schema}?warehouse={warehouse}&role={role}'.format(
    user=SNOW_SQL_USER,
    password=SNOW_SQL_PASSWORD,
    account_identifier=SNOW_SQL_ACCOUNT_HUBREADER,
    database=DATABASE_HUBUSER,
    schema=SCHEMA_HUBUSER,
    warehouse=SNOW_SQL_WAREHOUSE,
    role=USER_ROLE_HUBUSER)

# Connection String for creating final table in SOR.CTS.prob_match_results in production account
url_sor = 'snowflake://{user}:{password}@{account_identifier}/{database}/{schema}?warehouse={warehouse}&role={role}'.format(
    user=SNOW_SQL_USER,
    password=SNOW_SQL_PASSWORD,
    account_identifier=SNOW_SQL_ACCOUNT_PROD,
    database=DATABASE_SOR,
    schema=SCHEMA_SOR,
    warehouse=SNOW_SQL_WAREHOUSE_SOR,
    role=USER_ROLE_SOR)

# for validation Check
hub_user_new_table = f"select * from {TABLE_NAME_HUBUSER}"
sor_user_new_table = f"select * from {TABLE_NAME_SOR}"

# Connection String for pulling data from original unmatched dataset

url_sor_prod = 'snowflake://{user}:{password}@{account_identifier}/{database}/{schema}?warehouse={warehouse}&role={role}'.format(
    user=SNOW_SQL_USER,
    password=SNOW_SQL_PASSWORD,
    account_identifier=SNOW_SQL_ACCOUNT_HUBREADER,
    database=DATABASE_SOR_PROD,
    schema=SCHEMA_SOR_PROD,
    warehouse=SNOW_SQL_WAREHOUSE,
    role=USER_ROLE_HUBUSER)
