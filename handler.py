import json
import logging
import os
import sys
import tempfile
from os.path import exists

import boto3
from arcgis.features import FeatureLayerCollection
from arcgis.gis import GIS
from arcgis.mapping import WebMap
from botocore.exceptions import ClientError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()


def run(event, context):
    """
    Handler for agol_driver.
    Calls both shp2agol and agol2s3 depending on event payload
    """
    print(f"Running {event['method']}")

    # access AWS secrets manager and retreive GIS credentials
    secrets = get_secrets()

    # Login to AGOL
    agol_url = os.environ.get("AGOL_URL")
    gis = GIS(agol_url, secrets["AGOL_USER"], secrets["AGOL_PASS"])

    # Get all content from AGOL within MRDS group or under specific user
    agol_fs = gis.content.search(
        query=f"owner:{gis.properties.user.username} type:feature service"
    )

    # init s3
    s3 = boto3.resource("s3")

    # Get all content from feature service json
    fs_conf = s3.meta.client.get_object(
        Bucket=event["s3_bucket"], Key=event["fs_conf_path"]
    )
    fs_rec_json = json.loads(fs_conf["Body"].read().decode())

    # Check and compare current state of AGOL content with conf file
    if agol_fs:
        """Check if all AGOL features are listed in fs_conf file, if feature service is found in AGOL
        but it is currently absent from the conf file, update the conf file with new feature service.
        This will be constrained to a headless user's folder or a specific group in AGOL, which will be
        used in the AGOL content search method as a filter on line 27"""
        for fs in agol_fs:
            if fs.id not in [x["fs_id"] for x in fs_rec_json]:
                fs_rec_json.append({"fs_name": fs.title, "fs_id": fs.id})
                print(
                    f"New feature service found in AGOL that is absent from conf file. Added {fs.title} - {fs.id}"
                )

        # Check if fs_conf feature services match current state of content in AGOL
        # If old feature services are in conf file that have been deleted from AGOL, delete them from conf file
        for fs_rec in fs_rec_json:
            if fs_rec["fs_id"] not in [x.id for x in agol_fs]:
                print(
                    f"Feature service listed in conf file not found in AGOL, will be deleted from conf file. {fs_rec['fs_name']} - {fs_rec['fs_id']}"
                )
        fs_rec_json = [
            x for x in fs_rec_json if x["fs_id"] in [fs.id for fs in agol_fs]
        ]

        if event["method"] == "shp2agol":
            # Run checks on incoming new/update feature services in event payload
            for update_fs in event["data"]:
                # Check for existing feature services without fs_id in event payload
                if (
                    update_fs["fs_name"] in [x.title for x in agol_fs]
                    and not update_fs["fs_id"]
                ):
                    raise ValueError(
                        f"Feature service title '{update_fs['fs_name']}' exists in AGOL but no item id was provided in the event payload to overwrite"
                    )

                # Check if fs_id provided in event payload is a current feature service
                if update_fs["fs_id"] and update_fs["fs_id"] not in [
                    x.id for x in agol_fs
                ]:
                    raise ValueError(
                        f"Feature service '{update_fs['fs_name']}' has a specified id that is not currently in AGOL, check feature service id in event payload"
                    )

        elif event["method"] == "agol2s3":
            # Check if data to be exported from AGOL exists in AGOL
            for export_fs in event["data"]:
                if export_fs["fs_id"] not in [x.id for x in agol_fs]:
                    raise ValueError(
                        f"Feature service to be exported '{export_fs['fs_name']}' has a specified id that is not currently in AGOL, check feature service id in event payload"
                    )

    # Pipe updated feature service records from config file in to event payload
    fs_record = {"fs_record": fs_rec_json}
    event = event | fs_record

    print(f"{len(event['data'])} feature services to be processed")

    # call method to/from AGOL
    if event["method"] == "shp2agol":
        shp2agol(gis, s3, event)
    elif event["method"] == "agol2s3":
        agol2s3(gis, s3, event)


def shp2agol(gis: GIS, s3, event):
    """
    Load shapefile into AGOL organization for editing
    """

    # Iterate through shapfile names / feauture service
    for add_service in event["data"]:
        # Check if file to download exists in s3
        bucket = s3.Bucket(event["s3_bucket"])
        objs = list(
            bucket.objects.filter(
                Prefix=event["shp_s3_path"]
                + add_service["shp_name"]
                + add_service["format"]
            )
        )
        if not objs:
            print(
                f"Key not found in s3 bucket, skipping {event['shp_s3_path'] + add_service['shp_name'] + add_service['format']}"
            )
            continue

        # Get data from s3 bucket
        download_path = f"{tempfile.gettempdir()}/{add_service['fs_name']}"
        try:
            s3.meta.client.download_file(
                event["s3_bucket"],
                event["shp_s3_path"] + add_service["shp_name"] + add_service["format"],
                download_path,
            )
        except:
            print("Unable to download file from s3")
            raise

        # Set content props for AGOL item
        shp_props = {
            "type": "Shapefile",
            "title": add_service["fs_name"],
            "overwrite": True,
        }

        # Add shp to AGOL content
        shp_content = gis.content.add(item_properties=shp_props, data=download_path)
        print("added shpfile")

        # Delete existing feature service in AGOL if fs_id exists in event payload
        old_fs_id = None
        if "fs_id" in add_service.keys() and add_service["fs_id"]:
            try:
                old_fs = gis.content.get(add_service["fs_id"])
                old_fs_id = old_fs.id
                resp = old_fs.delete()
                print(f"Existing feature service found, deleted {old_fs.title}")
                if resp is not True:
                    raise Exception("Failed to delete existing feature service")
            except:
                sys.exit(
                    f"Incorrect feature service ID provided in event ({add_service['fs_id']}) - Feature service not found in content of user {gis.properties.user.username}"
                )
        else:
            print(
                f"{add_service['fs_name']} is a new feature service and will be added to AGOL"
            )

        # Publish feature service from shp
        try:
            fs_pub = shp_content.publish()
            print(f"published {fs_pub.title}")

            # store new feature service id, overwrite old version of existing feature service if it existed
            if old_fs_id:
                for old_fs in event["fs_record"]:
                    if old_fs_id == old_fs["fs_id"]:
                        old_fs["fs_id"] = fs_pub.id
                        old_fs["fs_name"] = fs_pub.title
            else:
                event["fs_record"].append({"fs_name": fs_pub.title, "fs_id": fs_pub.id})
                print("writing new feature service id")

            # Set service properties, if not provided use default
            capabilities = (
                event["fs_capabilities"]
                if event["fs_capabilities"]
                else "Query, Editing, Create, Update, Delete, ChangeTracking, Extract"
            )
            service_props = {
                "hasStaticData": False,
                "capabilities": capabilities,
                "editorTrackingInfo": {
                    "enableEditorTracking": True,
                    "enableOwnershipAccessControl": False,
                    "allowOthersToUpdate": True,
                    "allowOthersToDelete": True,
                    "allowOthersToQuery": True,
                    "allowAnonymousToUpdate": True,
                    "allowAnonymousToDelete": True,
                },
            }

            # Create Feature Layer Collection in order to enable editing within AGOL as Feature Service
            flc = FeatureLayerCollection.fromitem(fs_pub)

            # Update service properties on feature service
            flc.manager.update_definition(service_props)
            print("updated fs definition")

            # Add new feature service to MRDS webmap
            wm_item = gis.content.get(event["wm_id"])
            wm = WebMap(wm_item)
            resp = wm.add_layer(fs_pub)
            if resp is not True:
                raise Exception(
                    "Failed to add published feature service to existing MRDS webmap"
                )
            else:
                print(f"Added {fs_pub.title} feature service to webmap")

            # Delete uploaded shapefile
            shp_content.delete()
        except Exception as e:
            print(e)

            # delete uploaded shapefile to cleanup for next run
            shp_content.delete()

    # Overwrite feature service json config
    print(event["fs_record"])
    fs_services_dump = json.dumps(event["fs_record"])
    with tempfile.TemporaryFile("w+", delete=False) as tmpfile:
        tmpfile.write(fs_services_dump)
        print("wrote to tempfile")
        tmpfile.close()
        s3.meta.client.upload_file(
            tmpfile.name, event["s3_bucket"], event["fs_conf_path"]
        )


def agol2s3(gis, s3, event):
    """
    Export shapefile from AGOL organization and upload to s3
    """
    # get feature service id from event payload here

    for export_fs in event["data"]:
        # Get feature service object in AGOL
        feature_service = gis.content.get(export_fs["fs_id"])
        print("got feature service")

        # Export feature service from AGOL organization
        exported_shp = feature_service.export(
            export_fs["shp_name"], event["export_format"]
        )
        print("exported feature service")

        # Download exported shapefile then delete shapefile in AGOL
        shp_path = (
            f"{tempfile.gettempdir()}/{export_fs['shp_name'] + export_fs['format']}"
        )
        if exists(shp_path):
            os.remove(shp_path)
        exported_shp.download(
            tempfile.gettempdir(), export_fs["shp_name"] + export_fs["format"]
        )
        exported_shp.delete()
        print("downloaded shp and deleted shp object")

        # Upload and overwrite new shapefile to s3 bucket
        s3.meta.client.upload_file(
            shp_path,
            event["s3_bucket"],
            event["target_s3_filepath"] + export_fs["shp_name"] + export_fs["format"],
        )
        print("uploaded shp to s3 bucket")


def get_secrets():
    '''
    Retrieve username and password secret for
    AGOL authentication from AWS Secrets Manager
    '''

    secret_name = os.environ.get('AGOL_SECRET')
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    secret_json = json.loads(secret)

    return secret_json

