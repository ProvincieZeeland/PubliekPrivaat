from publicspace.downloaders import OGCFeatureApi
from datetime import datetime

api = OGCFeatureApi("https://api.pdok.nl/lv/bgt/ogc/v1")

api.download_collection(
    collection={"id": "begroeidterreindeel"},
    output_path="../data/output/provincie_zeeland/BGT",
    mask="data/input/provincie_zeeland.gpkg",  # Polygon/MultiPolygon met juiste CRS
    snapshot=None  # Laat weg of gebruik oude datum
)