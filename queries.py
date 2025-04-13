import datetime
from credentials import *

today = datetime.datetime.today()
str_formatted_date = today.strftime("%m_%d_%Y")

# Queries Use for Creating Match and Unmatch Data pull
queries = dict(
    # query for match extract
    cts_match_extract=f"""    
        select 
            b.customer_id,
            b.core_customer_id,
            store,
            max(to_date(event_date)) as txn_date,
            max(transaction_number) as transaction_number,
            max(address_city) as store_city,
            max(address_postal_code) as store_zip,
            max(num_shop) num_shop,
            max(msrname) msrname,
            max(user_name) as user_name,
            max(preferred_store) user_preferred_store,
            max(closest_store) user_closest_store,
            max(postal_code) user_postal
        from
        (select  core_customer_id ,
                customer_id,
                store,
                msrname,
                num_shop,
                event_date as event_date,
                transaction_number as transaction_number,
                row_number() over (PARTITION BY customer_id,core_customer_id ORDER BY num_shop DESC) as row_num 
        from
        (select 
            core_customer_id ,
            customer_id,
            store,
            max(event_date) as event_date,
            max(transaction_number) as transaction_number,
            count(distinct event_date) as num_shop,
            max(value:msrname::string) as msrname
            from (select * from "SOR_PROD"."CTS"."CTS_EVENTS_ANALYTICS" where unmatched_flag='N')m, lateral flatten(input => tender_array) 
            group by customer_id,core_customer_id,store 
            order by customer_id,core_customer_id,num_shop desc)
            )b
            left join (select entity_id,address_city,address_postal_code from "SOR_PROD"."CTS"."CTS_STORE")s on (b.store=s.entity_id)
            left join (select properties:customer_id as customer_id,
                            properties:name as user_name,
                            properties:preferred_store as preferred_store,
                            properties:closest_store as closest_store,
                            properties:postal_code as postal_code
                from "CRM"."USER_PROPS"."USER_PROPS_CHRISTMAS_TREE_SHOPS")u on (b.customer_id=u.customer_id)
                where row_num=1
                group by b.customer_id,b.core_customer_id,store  
 """,
    cts_unmatch_extract=f"""
     select 
     b.customer_id,
        b.core_customer_id,
        store,
        max(to_date(event_date)) as txn_date,
        max(transaction_number) as transaction_number,
        max(address_city) as store_city,
        max(address_postal_code) as store_zip,
        max(num_shop) as num_shop,
        max(msrname) msrname
    from (select 
            core_customer_id ,
            customer_id,
            store,
            event_date as event_date,
            transaction_number as transaction_number,
            msrname,
            num_shop,
            row_number() over (PARTITION BY customer_id,core_customer_id ORDER BY num_shop DESC) as row_num 
            from
        (select core_customer_id,
                customer_id,
                store,
                max(event_date) as event_date,
                max(transaction_number) as transaction_number,
                count(distinct event_date) as num_shop,
                max(value:msrname::string) as msrname
            from (select * 
    from "SOR_PROD"."CTS"."CTS_EVENTS_ANALYTICS" 
    where unmatched_flag='Y'),lateral flatten(input => tender_array)
            group by customer_id,core_customer_id,
                    store 
                    order by customer_id,core_customer_id,
                    num_shop desc))b
    left join (select 
    entity_id,
    address_city,
    address_postal_code 
    from "SOR_PROD"."CTS"."CTS_STORE"
    )s on (b.store=s.entity_id)
    where b.row_num=1
    group by b.customer_id,b.core_customer_id,
    b.store  
 """
)

# Queries used for creating table in Hubuser.data_science back up table
table_creation_queries_hubuser = dict(
    data_base=f'''use database {DATABASE_HUBUSER};''',
    schema=f'''use schema {SCHEMA_HUBUSER}''',
    remove=f'''remove @cts ;''',
    stage=f'''CREATE OR REPLACE STAGE {STAGE_NAME} ;''',
    upload_file=f'''put file://{FILE_NAME} @CTS AUTO_COMPRESS=FALSE OVERWRITE = TRUE;''',
    set_up_format=f"""create or replace file format {FORMAT_NAME}
    type=csv
    field_delimiter=','
    skip_header=1
    null_if=(' ')
    empty_field_as_null=true
    compression=gzip
    file_extension='.csv'
    field_optionally_enclosed_by='0x22';
    """,
    creating_table=f"""create or replace table {DATABASE_HUBUSER}.{SCHEMA_HUBUSER}.{TABLE_NAME_HUBUSER} as
    select 
     a.$1::bigint as core_customer_id,
     a.$2::date   as transaction_date, 
     a.$3::bigint  as transaction_number,
     a.$4::date as date_updated
    from @{STAGE_NAME}/{FILE_NAME} (file_format => {FORMAT_NAME}) a ;""",
    copy_data=f"""COPY INTO {TABLE_NAME_HUBUSER} from '@{STAGE_NAME}/{FILE_NAME}'
    file_format=(format_name=csv COMPRESSION = gzip error_on_column_count_mismatch=false),   
    on_error = continue """,
    remove_duplicates=f"""create or replace table {DATABASE_HUBUSER}.{SCHEMA_HUBUSER}.{TABLE_NAME_HUBUSER} as
    select 
    CORE_CUSTOMER_ID,
    TRANSACTION_DATE,
    TRANSACTION_NUMBER,
    DATE_UPDATED
    from (
    select *,row_number()over(partition by core_customer_id,transaction_number order by core_customer_id,transaction_number) as rank from {DATABASE_HUBUSER}.{SCHEMA_HUBUSER}.{TABLE_NAME_HUBUSER})
    where rank=1"""

)

# Queries used for creating table in sor.cts final table copying data after probabilistic matching

table_copying_queries_sor_final = dict(
    data_base=f'''use database {DATABASE_SOR};''',
    schema=f'''use schema {SCHEMA_SOR} ;''',
    ware_house=f'''use warehouse {SNOW_SQL_WAREHOUSE_SOR} ;''',
    remove=f'''remove @cts ;''',
    upload_file=f'''put file://{FILE_NAME} @cts AUTO_COMPRESS=FALSE OVERWRITE = TRUE;''',
    truncate=f'''truncate table SOR.CTS.{TABLE_NAME_SOR} ;''',
    copy_data=f"""COPY INTO {TABLE_NAME_SOR} from '@{STAGE_NAME}/{FILE_NAME}'
    file_format=(format_name=csv COMPRESSION = gzip error_on_column_count_mismatch=false),   
    on_error = continue ;""",
)

# Queries for final mapping after probabilistic matching process is being done
query = f"""select customer_id,core_customer_id,transaction_number,event_date as Trasaction_Date
    from "SOR_PROD"."CTS"."CTS_EVENTS_ANALYTICS" 
    where unmatched_flag='Y' """
# Queries for final mapping after probabilistic matching process to get a count for a faster reading of process
count_of_unmatch = f"""select count(*) as shape from (select customer_id,core_customer_id,transaction_number,event_date as Trasaction_Date
    from "SOR_PROD"."CTS"."CTS_EVENTS_ANALYTICS" 
    where unmatched_flag='Y') """

check_for_overlap = f"""select distinct CORE_CUSTOMER_ID as CORE_CUSTOMER_ID from SOR.CTS.{TABLE_NAME_SOR} 
where core_customer_id  not in ( select distinct (core_customer_id) from sor_prod.cts.cts_events_analytics where unmatched_flag = 'N') """





