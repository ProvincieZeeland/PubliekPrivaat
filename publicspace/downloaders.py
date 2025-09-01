import geopandas as gpd
import os
import requests
from requests.exceptions import HTTPError, ConnectionError
import logging
import time

logger = logging.getLogger(__name__)

class OGCFeatureApi:
    def __init__(self, url, limit=1000):
        """
        Initialization method of the OGCFeatureApi
        :param url: url of the OGC Feature API, eg https://api.pdok.nl/lv/bgt/ogc/v1
        :param limit: Maximum amount of features to request per page
        """

        self.bbox = None
        self.mask = None

        self.url = url
        self.collections_endpoint = f'{self.url}/collections'
        self.limit = limit

        # Retrieve collections
        self.collections = self.get_collections()

    def get_collections(self):
        """
        Return the different collections within the OGC api
        :return: list of dictionaries containing collection information
        """
        response = requests.get(f'{self.collections_endpoint}?f=json')
        response.raise_for_status()
        return response.json()['collections']

    @staticmethod
    def get_storage_crs(collection):
        """
        Return the crs in which the collection is stored
        :param collection: collection of which to retrieve the information from
        :return: CRS definition
        """
        return collection['storageCRS']

    def download(self, output_path, mask=None, snapshot=None):
        """
        Download all collections within the api
        :param output_path: Path where the collections will be stored
        :param mask: GeoDataFrame with masking polygons for spatial filtering
        :param snapshot: Datetime object for temporal filtering
        :return:
        """

        # determine bounding box for api requests
        self.bbox = None
        gdf_mask = None
        if mask is not None:
            gdf_mask = mask.to_crs('EPSG:4326')
            self.bbox = gdf_mask.total_bounds
            self.mask = gdf_mask.union_all()

        # iterate over all collections and download
        for collection in self.collections:
            self.download_collection(collection, output_path, gdf_mask, snapshot=snapshot)

    def download_collection(self, collection, output_path, mask=None, snapshot=None):
        """
        Download a collection to GeoPackage
        :param collection: Dictionary containing the collection information
        :param output_path: Path where the collection will be stored
        :param mask: GeoDataFrame with a mask for clipping the downloaded result
        :param snapshot: Datetime object for temporal filtering
        :return:
        """

        collection_id = collection['id']
        logger.info(f"Start downloading: {collection_id}")

        payload = {
            'limit': self.limit,
            'crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
            'bbox-crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
            'bbox': ','.join(map(str, self.bbox))
        }

        # Add snapshot to payload
        if snapshot:
            payload['datetime'] = snapshot.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Build initial url
        url = f'{self.collections_endpoint}/{collection_id}/items'
        all_features = []

        next_page_url = url

        try:
            max_retries = 5
            retry_wait = 60  # seconden

            # iterate over pages
            while next_page_url:
                for attempt in range(max_retries):
                    try:
                        if next_page_url == url:
                            response = requests.get(next_page_url, params=payload)
                        else:
                            response = requests.get(next_page_url)
                        response.raise_for_status()
                        break  # success
                    except (HTTPError, ConnectionError) as e:
                        logger.warning(f"[Attempt {attempt + 1}/{max_retries}] Error for {collection_id}: {e}")
                        if attempt < max_retries - 1:
                            time.sleep(retry_wait)
                        else:
                            logger.error(f"Failed to download {collection_id} after {max_retries} attempts")
                            return None

                if response.status_code == 200:
                    try:
                        data = response.json()
                    except ValueError as e:
                        raise RuntimeError(
                            f"Kon JSON niet decoderen uit antwoord: {e}\nResponse tekst: {response.text[:1000]}")
                else:
                    raise RuntimeError(f"Downloadfout: {response.status_code} - {response.text[:1000]}")

                features = data.get('features', [])
                all_features.extend(features)

                next_page_url = next((link.get('href') for link in data.get('links', []) if link.get('rel') == 'next'),
                                     None)

        except HTTPError as e:
            logger.error(f"HTTPError on collection {collection_id}: {e}")
            return None

        # Convert the collected features to a GeoDataFrame
        if len(all_features) > 0:
            gdf = gpd.GeoDataFrame.from_features(all_features)
            gdf = gdf.set_crs("EPSG:4326")

            # Clip
            gdf = gdf.clip(mask=mask, keep_geom_type=True)

            # Reproject to RD New
            gdf = gdf.to_crs("EPSG:28992")

            if not os.path.exists(output_path):
                os.makedirs(output_path)

            gdf.to_file(os.path.join(output_path, f'{collection_id}.gpkg'))
            return gdf

        else:
            logger.info(f"No features in {collection_id}, skipping layer")
