import boto3
from botocore import UNSIGNED
from botocore.config import Config
from tqdm import tqdm
import json
import simplekml
from concurrent.futures import ThreadPoolExecutor
import urllib.parse

s3 = boto3.client('s3', region_name='us-west-2', config=Config(signature_version=UNSIGNED))

# List objects in the bucket
bucket_name = 'umbra-open-data-catalog'
response = s3.list_objects_v2(Bucket=bucket_name)

# Get all metadata file keys
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name)

metadata_file_keys = [ (obj.get('Key'), next(obj2.get('Key','') for obj2 in page.get('Contents') if obj2.get('Key','').endswith("GEC.tif"))) for page in pages for obj in page.get('Contents') if obj.get("Key","").endswith("METADATA.json") ]

# Download and parse the metadata files from the keys
max_workers = 200

def download_and_parse_metadata(metadata_file_keys):
    key, gec_tif_key = metadata_file_keys
    file = s3.get_object(Bucket=bucket_name, Key=key)
    jsonstring = file.get("Body").read().decode('utf-8')
    metadata = json.loads(jsonstring)
    metadata["key"] = key
    metadata["gec_tif_url"] = f"{bucket_name}.s3.amazonaws.com/{gec_tif_key}"
    return metadata

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    all_metadata = list(tqdm(executor.map(download_and_parse_metadata, metadata_file_keys), total=len(metadata_file_keys)))

# Create an easy to use list of footprints and important metadata
footprints = []

for item in all_metadata:
    # Skip items without collects - there are approximately 20 of these "MONOSTATIC COLLECT_ANALYSIS" ??
    collects = item.get('collects',[])
    for collect in collects:       
        polygon_coordinates = collect.get('footprintPolygonLla',{}).get('coordinates')[0]
        center_point = collect.get('sceneCenterPointLla')
        point_coordinates = center_point.get('coordinates') if center_point is not None else polygon_coordinates[0]
        datetime_start = collect.get('startAtUTC')

        key_parts = item.get('key').split("/")
        index = 3 if key_parts[2] == "ad hoc" else 2
        name = key_parts[index]

        footprints.append({
            "point_coordinates": point_coordinates,
            "polygon_coordinates": polygon_coordinates,
            "datetime_start": datetime_start,
            "date": datetime_start.split("T",1)[0],
            "time": datetime_start.split("T")[1],
            "name": name.replace("&", "&amp;"),
            "image_url": f"""{urllib.parse.quote(item.get("gec_tif_url")).replace("&", "&amp;")}""",
            "directory_url": f"""http://umbra-open-data-catalog.s3-website.us-west-2.amazonaws.com/?prefix={urllib.parse.quote(item.get("key").rsplit("/", 1)[0]).replace("&", "&amp;")}"""
        })

# Sort the footprints by date and then name
footprints.sort(key=lambda x: (x["date"], x["name"]))

# Create a kml file with the polygons and TimeStamps
kml = simplekml.Kml()

sharedstyle = simplekml.Style()
sharedstyle.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png'
sharedstyle.iconstyle.scale = 3
sharedstyle.polystyle.color = simplekml.Color.changealphaint(128, simplekml.Color.red)
sharedstyle.balloonstyle.text =  f"""
<![CDATA[
    <h2>$[shortname]</h2>
    <p><b>Date:</b> $[date]</p>
    <p><b>Time:</b> $[time]</p>
    <p><b>Directory URL:</b> $[directory_url]</p>
    <p>Files ending "GEC.tif" are GeoTIFF files that may be loaded directly into Google Earth.</p>
    <p>In Google Earth Pro check the option at Tools → Options → General → Display → Show web results in external browser. Alternatively, you can manually copy and paste the link into an external browser.</p>
]]>
"""

for fp in footprints:
    mg = kml.newmultigeometry(name=f"""{fp['date']} - {fp['name']}""")
    mg.timestamp.when = fp['datetime_start']
    mg.style = sharedstyle
    point = mg.newpoint(coords=[(fp['point_coordinates'][0],fp['point_coordinates'][1])])
    polygon = mg.newpolygon(outerboundaryis=[(coord[0],coord[1]) for coord in fp['polygon_coordinates']])
    mg.extendeddata.newdata(
        name="shortname", 
        value=fp.get('name','')
        )
    mg.extendeddata.newdata(
        name="date", 
        value=fp.get('date','')
        )
    mg.extendeddata.newdata(
        name="time", 
        value=fp.get('time','')
        )
    mg.extendeddata.newdata(
        name="directory_url", 
        value=fp.get('directory_url','')
        )


kml.save("dist/footprints.kml")



