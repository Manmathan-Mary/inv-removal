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

AUTH_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJqdGkiOiI0NTVmMDE4My04ZGI1LTQ5ZjgtYjAzZi04MTJhY2U3NWViYWIiLCJzdWIiOiJtYW5tYXRoYW5tcyIsInR2Y1VzZXIiOnsidGVuYW50SWQiOiJjZWNjZWEwYi0xZTRkLTQxMmYtYWUyZi03OTQzZDcyZTkyNGIiLCJ1c2VySW5mb0lkIjoiMTY3YzA0N2YtNWNmMS00MDY5LWE3ZmQtOTUwOGUwMzAwMmFkIiwiYnVzaW5lc3NVbml0SWQiOiIwZGYwOGQ1Ni03OTM5LTRhNGMtODRhOC03MTU4ZGM2MTBlNTciLCJncmFudHMiOlt7InJvbGVJZCI6ImUyZjZiYmM4LWNkODMtNDU3ZS04MmI0LWZlMWI5MzE3MzNlYiIsImZlYXR1cmVzIjpbIk1TUl9JTlZFTlRPUlkiLCJTSElQTUVOVF9TVEFUVVNfTUFOQUdFTUVOVCIsIlRBU0tfTElTVCIsIlBBU1NXT1JEX1JFQ09WRVJZIiwiSU5WRU5UT1JZX1JFUE9SVCIsIk5PVElGSUNBVElPTlNfUFVCTElTSCIsIldSSVRFT0ZGX1JFUE9SVCIsIkNPTlRBQ1RfTUFOQUdFTUVOVCIsIklURU1fTEFCRUxfVklFVyIsIlJFQ0lQRV9SRVBPUlQiLCJQSUNLTElTVF9TRVNTSU9OIiwiU0lURV9WSUVXIiwiQ09ORklHVVJBVElPTl9NQU5BR0VNRU5UIiwiUkVQTEVOSVNITUVOVF9ERVRBSUxTIiwiQ1lDTEVfQ09VTlRfVVNFUiIsIlJFQ0lQRV9MSVNUSU5HIiwiSU1BR0VfTE9PS1VQIiwiTVNSX1NJVEVfVklFVyIsIk9SREVSX01BTkFHRU1FTlQiLCJNT0JJTEVfTE9HSU4iLCJNU1JfU1lOQyIsIkNZQ0xFX0NPVU5UX0RBU0hCT0FSRCIsIlBST0RVQ1RfQVNTT0NJQVRJT05fTElTVCIsIldSSVRFX1RBRyIsIkVSUF9QUk9DRVNTIiwiR0VUX0VQQ19BU1NPQ0lBVElPTiIsIkNZQ0xFX0NPVU5UX0NSRUFURSIsIlJFUExFTklTSE1FTlRfQ09ORklHVVJBVElPTiIsIkNZQ0xFX0NPVU5UX1JFUE9SVCIsIkRFVklDRV9SRUdJU1RSQVRJT04iLCJJTlZFTlRPUllfREVUQUlMUyIsIkJJTl9MT0NBVElPTiIsIkNZQ0xFX0NPVU5UX1ZBUklBTkNFX0RBU0hCT0FSRCIsIklOX1NUT1JFX01PVkVNRU5UIiwiSVRFTV9BVFRSSUJVVEVfTU9CSUxFIiwiVEFTS19NQU5BR0UiLCJPUkRFUl9GVUxGSUxMTUVOVCIsIlNZTkMiLCJNT0JJTEVfU0VUVElOR1MiLCJXUklURU9GRl9TVUJNSVQiLCJXRUJfTE9HSU4iLCJXUklURV9UQUdfUkVBU09OX1NZTkMiLCJSRVBMRU5JU0hNRU5UX01PVkUiLCJTSElQTUVOVF9NQU5BR0VNRU5UIiwiVVNFUl9NQU5BR0VNRU5UIiwiUElDS0lOR19PUkRFUlNfQ1JFQVRFIiwiVE9LRU5fUkVWT0NBVElPTiIsIkNZQ0xFX0NPVU5UX01BTkFHRU1FTlQiLCJQSUNLSU5HX09SREVSU19NQU5BR0VNRU5UIiwiUFJPRFVDVF9MSVNUU19WSUVXIiwiUFJPRFVDVF9BVFRSSUJVVEVfVkFMVUVfTUFOQUdFTUVOVCIsIk1PQklMRV9TWU5DIiwiUFJPRFVDVF9WSUVXU19WSUVXIiwiQ1VTVE9NRVJfQ0FMTEJBQ0tfTU9EVUxFIiwiSU5WRU5UT1JZX1BPU0lUSU9OX0ZJTEUiLCJOT1RJRklDQVRJT05TX1JFVklFVyIsIldSSVRFX1RBR19SRUFTT05fU1RBVFVTIiwiU0lURV9MRVZFTF9GSUxFX1VQTE9BRCIsIlVTRVJfQ09ORklHX1ZJRVciLCJQUk9EVUNUX1JFUE9SVCIsIlVTRVJfUEFTU1dPUkRfTUFOQUdFTUVOVCIsIkZBQ0lMSVRZX1JFUE9SVCIsIkNZQ0xFX0NPVU5UX1JFVklFVyIsIklOVkVOVE9SWV9EQVNIQk9BUkQiLCJJVEVNX0xBQkVMX01BTkFHRU1FTlQiLCJUQVNLX0VYRUNVVEUiLCJFUENfTU9WRU1FTlQiLCJQSUNLTElTVF9WSUVXIiwiREVWSUNFX0xJU1QiLCJTSVRFX01BTkFHRU1FTlQiLCJTSElQTUVOVF9SRVBPUlQiLCJTSVRFX0xBWU9VVF9WSUVXIl0sImNvbnRleHRMZXZlbCI6IlNJVEUiLCJjb250ZXh0cyI6W3sidGVuYW50SWQiOiJjZWNjZWEwYi0xZTRkLTQxMmYtYWUyZi03OTQzZDcyZTkyNGIiLCJidXNpbmVzc1VuaXRzIjpbeyJidXNpbmVzc1VuaXRJZCI6IjBkZjA4ZDU2LTc5MzktNGE0Yy04NGE4LTcxNThkYzYxMGU1NyIsInNpdGVzIjpbXX1dfV0sInJvbGVOYW1lIjoiU3RvcmUgTWFuYWdlciJ9XSwiYmVhcmVyVG9rZW5WYWx1ZSI6bnVsbCwiZW1haWwiOm51bGx9LCJleHAiOjE3NjY3NDY0Mjl9.RzUhCOerTh5vZQPKuvy0H9xjgJbp88iJdn3qbdhc47-t7paXGADYvTYKdikcdSoyRWlbxtwYSBb0zvW1HF-8Ag"
BATCH_SIZE = 10

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
    limit 8
"""

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
    engine = create_engine(
        f"spanner+spanner:///projects/{project_id}/instances/{instance_name}/databases/{database_name}"
    )

    return engine;

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
        "-s", "--site-id",
        type=valid_uuid,
        required=True,
        help="Site ID (UUID)"
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
    print(f"calling url {url}")

    headers = {
            'Authorization': f'Bearer {AUTH_TOKEN}',
            'Content-Type': 'application/json', 
            'Accept' : '*/*'
    }

    try:
        response = requests.post(url, headers=headers, data=inventory_epcs_removal_payload)
        print(f"Response code {response.status_code}")
        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError as e:
        print(f"Error occured while sending inventory removal message {e}")
        raise



if __name__ == "__main__":
    args = parse_args()

    print(f"env                    = {args.env}")
    print(f"buid                   = {args.buid}")
    print(f"siteId                 = {args.site_id}")
    print(f"denormalized_recipe_id = {args.denormalized_recipe_id}")
    print(f"input_file             = {args.input_file}")

    ext = os.path.splitext(args.input_file)[1].lower()

    if (ext == ".csv") : 
        product_df = pl.read_csv(args.input_file)
    elif (ext == ".xlsx" or ext == ".xls"):
        product_df = pl.read_excel(args.input_file)
    else: 
        print(f"file format is not supported {ext}")

    product_list = product_df.select(
        pl.col("UPC-Number")
    ).unique().to_series().to_list()

    print(GET_EPC_LOCATION_QUERY)

    params = {
        "business_unit_id": str(args.buid),
        "site_id" : str(args.site_id), 
        "product_codes": product_list
    }

    if product_list:
        engine = get_engine("tvc-dev-187621", "dev2-spanner-mr", "inventory")
        inventory_df = execute_query_return_dataframe(GET_EPC_LOCATION_QUERY, engine=engine, params=params)
        print(inventory_df)
        
        zone_epcs_df = inventory_df.group_by(
            pl.col("site_id"), pl.col("zone_id")
        ).agg(pl.col("epc").alias("epc_list"))
    
        for row in zone_epcs_df.iter_rows(named=True):
            zone_epcs = row['epc_list']
            print(f"EPC count for zone id {row['zone_id']}: {len(zone_epcs)}")
            chunked_epcs = list(chunked(zone_epcs, BATCH_SIZE))

            for ind, elist in enumerate(chunked_epcs): 
                inventory_remove_payload_dict = asdict(build_inventory_removal_request(row["zone_id"], elist, args.denormalized_recipe_id))
                inventory_remove_payload_json = json.dumps(inventory_remove_payload_dict, indent=4)
                print(f"sending batch {ind+1} of {len(chunked_epcs)} inventory removal request for zone id {row['zone_id']}")
                print(f"payload : {inventory_remove_payload_json}")
                send_inventory_remove_request(args.env, args.buid, inventory_epcs_removal_payload=inventory_remove_payload_json)
                print(f"batch {ind+1} sent")