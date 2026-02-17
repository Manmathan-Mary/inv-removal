import argparse
import uuid
import os
import sys
import polars as pl
import json
import requests

from sqlalchemy import create_engine, text
from dataclasses import dataclass, asdict
from typing import List, Optional
from datetime import timezone, datetime
from uuid import uuid4
from more_itertools import chunked
from pathlib import Path
from logging_config import setup_logger 
from datetime import datetime

AUTH_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJqdGkiOiI3MjA2YWU1YS1kYTkzLTRkMDktOWEzMS0wNmVkYjk4NTU3NzUiLCJzdWIiOiJtYW5tYXRoYW5tcyIsInR2Y1VzZXIiOnsidGVuYW50SWQiOiJjNThkYTgyYS05MDJiLTQzZmYtODkwOS0zYmJiYjU3Y2RhYTkiLCJ1c2VySW5mb0lkIjoiMDkzYWVhNGYtODJkNC00OTk4LWJlMTctMmJlZDdkOWIwYWYyIiwiYnVzaW5lc3NVbml0SWQiOiIwYTI3YjFlZC1hOGY0LTQ4MjEtOTNmNS03NDIxNTczMTAzMzYiLCJncmFudHMiOlt7InJvbGVJZCI6ImUyZjZiYmM4LWNkODMtNDU3ZS04MmI0LWZlMWI5MzE3MzNlYiIsImZlYXR1cmVzIjpbIk1TUl9JTlZFTlRPUlkiLCJTSElQTUVOVF9TVEFUVVNfTUFOQUdFTUVOVCIsIlRBU0tfTElTVCIsIlBBU1NXT1JEX1JFQ09WRVJZIiwiSU5WRU5UT1JZX1JFUE9SVCIsIk5PVElGSUNBVElPTlNfUFVCTElTSCIsIldSSVRFT0ZGX1JFUE9SVCIsIkNPTlRBQ1RfTUFOQUdFTUVOVCIsIklURU1fTEFCRUxfVklFVyIsIlJFQ0lQRV9SRVBPUlQiLCJQSUNLTElTVF9TRVNTSU9OIiwiU0lURV9WSUVXIiwiQ09ORklHVVJBVElPTl9NQU5BR0VNRU5UIiwiUkVQTEVOSVNITUVOVF9ERVRBSUxTIiwiQ1lDTEVfQ09VTlRfVVNFUiIsIlJFQ0lQRV9MSVNUSU5HIiwiSU1BR0VfTE9PS1VQIiwiTVNSX1NJVEVfVklFVyIsIk9SREVSX01BTkFHRU1FTlQiLCJNT0JJTEVfTE9HSU4iLCJNU1JfU1lOQyIsIkNZQ0xFX0NPVU5UX0RBU0hCT0FSRCIsIlBST0RVQ1RfQVNTT0NJQVRJT05fTElTVCIsIldSSVRFX1RBRyIsIkVSUF9QUk9DRVNTIiwiR0VUX0VQQ19BU1NPQ0lBVElPTiIsIkNZQ0xFX0NPVU5UX0NSRUFURSIsIlJFUExFTklTSE1FTlRfQ09ORklHVVJBVElPTiIsIkNZQ0xFX0NPVU5UX1JFUE9SVCIsIkRFVklDRV9SRUdJU1RSQVRJT04iLCJJTlZFTlRPUllfREVUQUlMUyIsIkJJTl9MT0NBVElPTiIsIkNZQ0xFX0NPVU5UX1ZBUklBTkNFX0RBU0hCT0FSRCIsIklOX1NUT1JFX01PVkVNRU5UIiwiSVRFTV9BVFRSSUJVVEVfTU9CSUxFIiwiVEFTS19NQU5BR0UiLCJPUkRFUl9GVUxGSUxMTUVOVCIsIlNZTkMiLCJNT0JJTEVfU0VUVElOR1MiLCJXUklURU9GRl9TVUJNSVQiLCJXRUJfTE9HSU4iLCJXUklURV9UQUdfUkVBU09OX1NZTkMiLCJSRVBMRU5JU0hNRU5UX01PVkUiLCJTSElQTUVOVF9NQU5BR0VNRU5UIiwiVVNFUl9NQU5BR0VNRU5UIiwiUElDS0lOR19PUkRFUlNfQ1JFQVRFIiwiVE9LRU5fUkVWT0NBVElPTiIsIkNZQ0xFX0NPVU5UX01BTkFHRU1FTlQiLCJQSUNLSU5HX09SREVSU19NQU5BR0VNRU5UIiwiUFJPRFVDVF9MSVNUU19WSUVXIiwiUFJPRFVDVF9BVFRSSUJVVEVfVkFMVUVfTUFOQUdFTUVOVCIsIk1PQklMRV9TWU5DIiwiUFJPRFVDVF9WSUVXU19WSUVXIiwiQ1VTVE9NRVJfQ0FMTEJBQ0tfTU9EVUxFIiwiSU5WRU5UT1JZX1BPU0lUSU9OX0ZJTEUiLCJOT1RJRklDQVRJT05TX1JFVklFVyIsIldSSVRFX1RBR19SRUFTT05fU1RBVFVTIiwiU0lURV9MRVZFTF9GSUxFX1VQTE9BRCIsIlVTRVJfQ09ORklHX1ZJRVciLCJQUk9EVUNUX1JFUE9SVCIsIlVTRVJfUEFTU1dPUkRfTUFOQUdFTUVOVCIsIkZBQ0lMSVRZX1JFUE9SVCIsIkNZQ0xFX0NPVU5UX1JFVklFVyIsIklOVkVOVE9SWV9EQVNIQk9BUkQiLCJJVEVNX0xBQkVMX01BTkFHRU1FTlQiLCJUQVNLX0VYRUNVVEUiLCJFUENfTU9WRU1FTlQiLCJQSUNLTElTVF9WSUVXIiwiREVWSUNFX0xJU1QiLCJTSVRFX01BTkFHRU1FTlQiLCJTSElQTUVOVF9SRVBPUlQiLCJTSVRFX0xBWU9VVF9WSUVXIl0sImNvbnRleHRMZXZlbCI6IlNJVEUiLCJjb250ZXh0cyI6W3sidGVuYW50SWQiOiJjNThkYTgyYS05MDJiLTQzZmYtODkwOS0zYmJiYjU3Y2RhYTkiLCJidXNpbmVzc1VuaXRzIjpbeyJidXNpbmVzc1VuaXRJZCI6IjBhMjdiMWVkLWE4ZjQtNDgyMS05M2Y1LTc0MjE1NzMxMDMzNiIsInNpdGVzIjpbXX1dfV0sInJvbGVOYW1lIjoiU3RvcmUgTWFuYWdlciJ9XSwiYmVhcmVyVG9rZW5WYWx1ZSI6bnVsbCwiZW1haWwiOiJtYW5tYXRoYW5AZ21haWwuY29tIn0sImV4cCI6MTc3MjE4NjY5MX0.AhhWwuzyk7DmgPzzP9cetMy3-yIU0SvDtXYP-dbMGQpv3ClmsmrBnxSQ2k9Io_o33l1hT68KDmvsbp5UxrwG2g"
BATCH_SIZE = 500

PROJECT_ID = "tvc-dev-187621"
INSTANCE_NAME = "dev2-spanner"
MR_INSTANCE_NAME = "dev2-spanner-mr"
INVENTORY_DATABASE = "inventory"
FACILITY_DATABASE = "facility"
CACHE_FOLDER_NAME = "cache"
SITES_FILES = "sites_{buid}.csv"
COMPLETED_SITES_FILE = "{cacheFolderName}/completed_sites_{buid}.csv"
OUTPUT_DATA_FOLDER = "data"

GET_EPC_LOCATION_QUERY = """
    select to_base64(epc) as epc, 
           business_unit_id, 
           date_updated, 
           event_time, 
           product_code, 
           site_id, 
           zone_id
    from epc_location 
    where business_unit_id = :business_unit_id 
    and site_id = :site_id 
    and product_code in unnest(:product_codes)
"""

GET_ALL_ACTIVE_SITES = """
    select * from site 
    where business_unit_id = :business_unit_id 
    and is_active = true
    order by site_id asc
"""

logger = setup_logger()
execution_time = datetime.now().strftime("%Y%m%d_%H%M%S")

@dataclass 
class EpcReadEvent:
    epc:str
    eventTime: str

@dataclass 
class RecipeInfoResource: 
    denormalizedRecipeId: uuid4
    applyTime: str
    
@dataclass 
class EpcRemovedRequest: 
    zoneId: str 
    workflow: str 
    disposition: str
    removedEvents: List[EpcReadEvent]
    readPointId: uuid4
    recipeInfo: Optional[RecipeInfoResource]=None


def get_engine(project_id, instance_name, database_name):
    return create_engine(
        f"spanner+spanner:///projects/{project_id}/instances/{instance_name}/databases/{database_name}"
    )


def execute_query_return_dataframe(query: str, engine,params: dict):
    with engine.connect().execution_options(read_only=True) as connection:
        result = connection.execute(text(query), parameters=params)
        return pl.DataFrame(result.fetchall(), schema=result.keys())

def build_inventory_removal_request(zone_id:str, epcs: List[str], denormalized_recipe_id: str, workflow:str="UNKNOWN", disposition:str="UNKNOWN", read_point_id:str="ECRT_REMOVAL") -> EpcRemovedRequest:
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="milliseconds") + "Z"

    epc_events = [
        EpcReadEvent(
            epc=epc,
            eventTime=now
        )
        for epc in epcs
    ]

    recipe_info = None
    if denormalized_recipe_id is not None:
        denormalized_recipe_id = str(denormalized_recipe_id)
        recipe_info = RecipeInfoResource(
            denormalizedRecipeId=denormalized_recipe_id,
            applyTime=now
        )

    epcRemovedRequest = None
    if recipe_info == None:
        epcRemovedRequest = EpcRemovedRequest(
            zoneId=zone_id, 
            workflow=workflow, 
            disposition=disposition, 
            removedEvents=epc_events,
            readPointId=read_point_id
        )
    else : 
        epcRemovedRequest = EpcRemovedRequest(
            zoneId=zone_id, 
            workflow=workflow, 
            disposition=disposition, 
            removedEvents=epc_events,
            recipeInfo=recipe_info, 
            readPointId=read_point_id
        )

    return epcRemovedRequest

def valid_uuid(value: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid UUID: {value}")


def valid_input_file(path: str) -> str:
    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"File not found: {path}")

    ext = os.path.splitext(path)[1].lower()
    if ext not in (".csv", ".xls", ".xlsx"):
        raise argparse.ArgumentTypeError(
            "Input file must be a CSV or Excel file (.csv, .xls, .xlsx)"
        )
    return path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Process recipe input file"
    )

    parser.add_argument(
        "-e", "--env",
        required=True,
        help="Environment name (e.g. dev, qa, prod)"
    )

    parser.add_argument(
        "-b", "--buid",
        type=valid_uuid,
        required=True,
        help="Business Unit ID (UUID)"
    )

    parser.add_argument(
        "-s", "--site-ids",
        type=valid_uuid,
        nargs="+",
        required=False,
        help="Space separated site ids"
    )

    parser.add_argument(
        "-sl", "--site-limit", 
        type=int, 
        required=False, 
        help="Specify number of sites to remove the products from the inventory if site id list not provided"
    )

    parser.add_argument(
        "-d", "--denormalized-recipe-id",
        type=valid_uuid,
        required=False,
        help="Denormalized Recipe ID (UUID)"
    )

    parser.add_argument(
        "-i", "--input-file",
        type=valid_input_file,
        required=True,
        help="Input CSV or Excel file"
    )

    return parser.parse_args()

def send_inventory_remove_request(envr:str, business_unit_id: str,inventory_epcs_removal_payload: str) -> requests.Response:
    INVENTORY_REMOVE_URL = f"https://inventory-{envr}.truevue.shoppertrak.com//epcEvent/reads/removed"
    url =f"{INVENTORY_REMOVE_URL}?businessUnitId={business_unit_id}"
    logger.info(f"calling url {url}")

    headers = {
            'Authorization': f'Bearer {AUTH_TOKEN}',
            'Content-Type': 'application/json', 
            'Accept' : '*/*'
    }

    try:
        response = requests.post(url, headers=headers, data=inventory_epcs_removal_payload)
        logger.info(f"Response code {response.status_code}")
        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError as e:
        logger.info(f"Error occured while sending inventory removal message {e}")
        raise


def persist_summary(summary_rows):

    SCHEMA = {
        "site_id": pl.Utf8,
        "total_count": pl.Int64,
        "removed_count": pl.Int64
    }

    completed_sites_path = Path(COMPLETED_SITES_FILE)

    new_rows_df = pl.DataFrame(summary_rows, schema=SCHEMA)

    if completed_sites_path.exists() and completed_sites_path.stat().st_size > 0:

        existing_df = pl.read_csv(COMPLETED_SITES_FILE)
        completed_sites = existing_df.vstack(new_rows_df)

    else:
        completed_sites = new_rows_df

    completed_sites.write_csv(COMPLETED_SITES_FILE)



if __name__ == "__main__":
    args = parse_args()

    logger.info(f"env                    = {args.env}")
    logger.info(f"buid                   = {args.buid}")
    logger.info(f"siteId                 = {args.site_ids}")
    logger.info(f"site limit             = {args.site_limit}")
    logger.info(f"denormalized_recipe_id = {args.denormalized_recipe_id}")
    logger.info(f"input_file             = {args.input_file}")

    ext = os.path.splitext(args.input_file)[1].lower()

    if (ext == ".csv") : 
        product_df = pl.read_csv(args.input_file)
    elif (ext == ".xlsx" or ext == ".xls"):
        product_df = pl.read_excel(args.input_file)
    else: 
        logger.info(f"file format is not supported {ext}")
        system.exit(1)


    if not product_df.is_empty():
        os.makedirs(OUTPUT_DATA_FOLDER, exist_ok=True)
        product_list = product_df.select(pl.col("product_code")).unique().to_series().to_list()
        params = { "business_unit_id": str(args.buid)}

        SITES_FILES = SITES_FILES.format(buid=args.buid)
        COMPLETED_SITES_FILE = COMPLETED_SITES_FILE.format(cacheFolderName=CACHE_FOLDER_NAME, buid=args.buid)
        
        try:
            sites_df = pl.read_csv(f"{CACHE_FOLDER_NAME}/{SITES_FILES}")
        except FileNotFoundError: 
            logger.info("site cache file not found getting it from spanner")
            facility_engine = get_engine(PROJECT_ID, INSTANCE_NAME, FACILITY_DATABASE)
            sites_df = execute_query_return_dataframe(GET_ALL_ACTIVE_SITES, facility_engine, params)

            if not sites_df.is_empty():
                os.makedirs(CACHE_FOLDER_NAME,exist_ok=True)
                sites_df.write_csv(f"{CACHE_FOLDER_NAME}/{SITES_FILES}")
            
            else : 
                logger.info(f"No active sites identified, exiting")
                sys.exit(1)
                
        # site selections for inventory epc removal. 

        sites_for_removal = []
        try: 
            completed_sites_df = pl.read_csv(COMPLETED_SITES_FILE)

            if args.site_ids :
                sites_for_removal = args.site_ids   
            else: 
                completed_sites = completed_sites_df.select(pl.col("site_id")).to_series().to_list()
                uncompleted_sites_df = sites_df.filter(
                                        ~pl.col("site_id").is_in(completed_sites)
                                    ).sort("site_id")
                uncompleted_sites = uncompleted_sites_df.head(args.site_limit).select(pl.col("site_id")).to_series().to_list()
                sites_for_removal.extend(uncompleted_sites)

        except FileNotFoundError :
            logger.info("completed sites file not found")
            if args.site_ids :
                sites_for_removal = args.site_ids   
            else: 
                sites_for_removal.extend(sites_df.head(args.site_limit).select(pl.col("site_id")).to_series().to_list())

        logger.info(f"sites selected for inventory epc removal for the requested products {sites_for_removal}")
        
        row_summary = []
        
        for site_id in sites_for_removal: 
            params.update({
                "site_id" : str(site_id), 
                "product_codes": product_list
            })
    
    
            inventory_engine = get_engine(PROJECT_ID, MR_INSTANCE_NAME, INVENTORY_DATABASE)
            inventory_df = execute_query_return_dataframe(GET_EPC_LOCATION_QUERY, engine=inventory_engine, params=params)
            epc_removed_count = 0
            total_epc_count = inventory_df.shape[0]
            
            if not inventory_df.is_empty():
                inventory_df.write_csv(f"{OUTPUT_DATA_FOLDER}/inventory_epc_removal_{site_id}_{execution_time}.csv") 
                zone_epcs_df = inventory_df.group_by(
                    pl.col("site_id"), pl.col("zone_id")
                ).agg(pl.col("epc").alias("epc_list"))
            
            else: 
                new_row = {
                        "site_id": str(site_id),
                        "total_count": int(total_epc_count),
                        "removed_count": epc_removed_count
                    }
                persist_summary(new_row)
                continue

            try:
                for row in zone_epcs_df.iter_rows(named=True):
                    zone_epcs = row['epc_list']
                    logger.info(f"EPC count for zone id {row['zone_id']}: {len(zone_epcs)}")
                    chunked_epcs = list(chunked(zone_epcs, BATCH_SIZE))
                    for ind, elist in enumerate(chunked_epcs): 
                        inventory_remove_payload_dict = asdict(build_inventory_removal_request(row["zone_id"], elist, args.denormalized_recipe_id))
                        inventory_remove_payload_json = json.dumps(inventory_remove_payload_dict, indent=4)
                        logger.info(f"sending batch {ind+1} of {len(chunked_epcs)} inventory removal request for zone id {row['zone_id']}")
                        logger.info(f"payload : {inventory_remove_payload_json}")
                        send_inventory_remove_request(args.env, args.buid, inventory_epcs_removal_payload=inventory_remove_payload_json)
                        epc_removed_count += len(elist)    
                        logger.info(f"batch {ind+1} sent")
            except Exception as ex: 
                raise 
            finally:
                new_row = {
                        "site_id": str(site_id),
                        "total_count": int(total_epc_count),
                        "removed_count": epc_removed_count
                    }
                persist_summary(new_row)
                       
    else: 
        logger.info("No products found in the provided product list file")