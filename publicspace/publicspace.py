import geopandas as gpd
import pandas as pd
import pyogrio.errors
from shapely.errors import GEOSException
from shapely.geometry import Polygon, MultiPolygon
from pathlib import Path
import logging

from datetime import datetime
import numpy as np
from tqdm import tqdm

from publicspace.downloaders import OGCFeatureApi

logger = logging.getLogger(__name__)

from publicspace.settings import TOP10NL_FUNCTIONEELGEBIED_PRIVATE, PRIVATE, PUBLIC, BGT_BEGRTERREINDEEL_PUBLIC, \
    TOP10NL_FUNCTIONEELGEBIED_HARBOUR, KUNSTWERKDEEL_PUBLIC


class PublicSpace:

    def __init__(self,
                 bgt_path: str,
                 top10nl_path: str,
                 bgt_layers: dict = None,
                 bgt_download: bool = True,
                 top10nl_layers: dict = None,
                 top10nl_download: bool = True,
                 aoi: [Polygon, MultiPolygon] = None,
                 ):

        """
        Initialize the PublicSpace class.

        :param bgt_path: Path to the BGT data.
        :param top10nl_path: Path to the TOP10NL data.
        :param bgt_layers: Dictionary of BGT layers to load.
        :param bgt_download: Boolean indicating whether to download BGT data.
        :param top10nl_layers: Dictionary of TOP10NL layers to load.
        :param top10nl_download: Boolean indicating whether to download TOP10NL data.
        :param aoi: Area of interest as a Polygon or MultiPolygon.
        """

        if bgt_download:
            logger.info("Start downloading BGT")
            bgt = OGCFeatureApi('https://api.pdok.nl/lv/bgt/ogc/v1')
            dt = datetime.now()
            bgt.download(output_path=bgt_path, mask=aoi, snapshot=dt)

        if top10nl_download:
            logger.info("Start downloading TOP10NL")
            top10nl = OGCFeatureApi('https://api.pdok.nl/brt/top10nl/ogc/v1')
            top10nl.download(output_path=top10nl_path, mask=aoi)

        self.bgt = self.load_source(bgt_path, bgt_layers)
        self.top10nl = self.load_source(top10nl_path, top10nl_layers)

        self.gdf = gpd.GeoDataFrame(columns=['source', 'layer', 'source_id', 'reason', 'source_category', 'category',
                                             'geometry'],
                                    geometry='geometry')

        self.analyze_public_private_space(self.bgt, self.top10nl)

    def load_source(self, path: str, layers: dict, filter_geom_type=None):
        """
        Load GIS data containing layers. Layers dict should contain file name, layer name and column name information.

        :param path: path containing layers
        :param layers: dictionary containing all layer names as keys and the filename as value
        :param filter_geom_type: List of geometry types used as a filter when loading data sources
        :return: Dictionary object with layer name as keys and GeoDataFrames as values
        """

        # Set default filter_geom_type
        if filter_geom_type is None:
            filter_geom_type = ['Polygon', 'MultiPolygon']

        logger.info(f"Start loading dataset in {path}")

        # create data dict
        data = {}

        for key in layers:
            logger.info(f"Loading layer {key}")

            filename = layers[key]
            gdf = None
            file_list = []
            if isinstance(filename, str):
                file_list = [filename]
            elif isinstance(filename, list):
                file_list = filename

            for file in file_list:
                try:
                    gdf_item = gpd.read_file(Path(path) / file)

                    if filter_geom_type:
                        gdf_item = gdf_item[gdf_item.geom_type.isin(filter_geom_type)]

                    if gdf is None:
                        gdf = gdf_item.copy()
                    else:
                        gdf = pd.concat([gdf, gdf_item])
                except pyogrio.errors.DataSourceError as e:
                    logger.warning(f"Unable to load {file}. If the dataset is empty for the area of interest, "
                                   f"this file won't be created.")
                    continue

            data[key] = gdf

        return data

    def analyze_public_private_space(self, bgt: dict, top10nl: dict):
        """
        Method that implements a dataflow using different data sources (BGT and TOP10NL) in order to determine what
        area is considered private and public.
        :param bgt: dictionary containing BGT layers as keys and as value the GeoDataFrame
        :param top10nl: dictionary containing TOP10NL layers as keys and as value the GeoDataFrame
        :return: GeoDataFrame containing public and private area
        """

        # Step 1 TOP10NL functioneel gebieden with specific category to private
        logger.info("Step 1: TOP10NL functioneel gebied of certain categories -> private")
        try:
            top10nl_functioneelgebied = top10nl['functioneelgebied']
            top10nl_functioneelgebied_selectie = top10nl_functioneelgebied[
                top10nl_functioneelgebied['typefunctioneelgebied'].isin(TOP10NL_FUNCTIONEELGEBIED_PRIVATE)]
            self.add_data(data=top10nl_functioneelgebied_selectie,

                          source='top10nl',
                          layer='functioneelgebied',
                          source_id_column='lokaal_id',
                          reason=f"TOP10NL functioneel gebied van niet-openbare categorie, zie kolom 'source_category'. "
                                 f"Geclassificeerd als {PRIVATE}",
                          source_category='typefunctioneelgebied',
                          category=PRIVATE
                          )
        except TypeError:
            logger.warning("Geen TOP10NL functioneel gebied aangetroffen")

        # Step 2 BGT Pand to private
        logger.info("Step 2: BGT Pand -> private")
        bgt_pand = bgt['pand']
        self.add_data(data=bgt_pand,
                      source='bgt',
                      layer='pand',
                      source_id_column='lokaal_id',
                      reason=f"BGT pand geclassificeerd als {PRIVATE}",
                      source_category=None,
                      category=PRIVATE
                      )

        # Step 3 BGT Onbegroeidterreindeel of which fysiek voorkomen == erf to private
        logger.info("Step 3: BGT Onbegroeidterreindeel of category 'erf' -> private")
        try:
            bgt_onbegroeidterreindeel = bgt['onbegroeidterreindeel']
            bgt_obtd_erf = bgt_onbegroeidterreindeel[bgt_onbegroeidterreindeel['fysiek_voorkomen'] == 'erf']
            self.add_data(data=bgt_obtd_erf,
                          source='bgt',
                          layer='onbegroeid_terreindeel',
                          source_id_column='lokaal_id',
                          reason=f"BGT onbegroeid terreindeel van categorie 'erf' geclassificeerd als {PRIVATE}",
                          source_category='fysiek_voorkomen',
                          category=PRIVATE
                          )
        except TypeError:
            logger.warning("Geen BGT onbegroeidterreindeel aangetroffen")

        # Step 4 BGT Begroeidterreindeel with specific category to public
        logger.info("Step 4: BGT Begroeidterreindeel of certain categories -> public")
        try:
            bgt_begroeidterreindeel = bgt['begroeidterreindeel']
            bgt_begroeidterreindeel_selectie = bgt_begroeidterreindeel[
                bgt_begroeidterreindeel['fysiek_voorkomen'].isin(BGT_BEGRTERREINDEEL_PUBLIC)]
            self.add_data(data=bgt_begroeidterreindeel_selectie,
                          source='bgt',
                          layer='begroeid_terreindeel',
                          source_id_column='lokaal_id',
                          reason=f"BGT begroeidterreindeel fysiek voorkomen van openbare categorie, zie kolom "
                                 f"'source_category'. Geclassificeerd als {PUBLIC}",
                          source_category='fysiek_voorkomen',
                          category=PUBLIC
                          )
        except TypeError:
            logger.warning("Geen BGT begroeidterreindeel aangetroffen")

        # Step 5 BGT Begroeidterreindeel which are not in step 4 to private
        logger.info("Step 5: BGT Begroeidterreindeel of certain categories -> private")
        try:
            bgt_begroeidterreindeel = bgt['begroeidterreindeel']
            bgt_begroeidterreindeel_overig = bgt_begroeidterreindeel[
                ~bgt_begroeidterreindeel['fysiek_voorkomen'].isin(BGT_BEGRTERREINDEEL_PUBLIC)]
            self.add_data(data=bgt_begroeidterreindeel_overig,
                          source='bgt',
                          layer='begroeid_terreindeel',
                          source_id_column='lokaal_id',
                          reason="BGT begroeidterreindeel fysiek voorkomen van niet-openbare categorie, zie kolom "
                                 f"'source_category'. Geclassificeerd als {PRIVATE}",
                          source_category='fysiek_voorkomen',
                          category=PRIVATE
                          )
        except TypeError:
            logger.warning("Geen BGT begroeidterreindeel aangetroffen")

        # Step 6 BGT Wegdeel for which functie = spoorbaan to private
        logger.info("Step 6: BGT wegdeel with category 'spoorbaan' -> private")
        try:
            bgt_wegdeel = bgt['wegdeel']
            bgt_wegdeel_spoorbaan = bgt_wegdeel[
                bgt_wegdeel['functie'] == 'spoorbaan']
            self.add_data(data=bgt_wegdeel_spoorbaan,
                          source='bgt',
                          layer='wegdeel',
                          source_id_column='lokaal_id',
                          reason=f"BGT wegdeel van categorie 'spoorbaan' geclassificeerd als {PRIVATE}",
                          source_category='functie',
                          category=PRIVATE
                          )
        except TypeError:
            logger.warning("Geen BGT wegdeel aangetroffen")

        # Step 7 All BGT Wegdeel which are not in step 6 to public
        logger.info("Step 7: BGT wegdeel with other categories -> public")
        try:
            bgt_wegdeel = bgt['wegdeel']
            bgt_wegdeel_overig = bgt_wegdeel[
                bgt_wegdeel['functie'] != 'spoorbaan']
            self.add_data(data=bgt_wegdeel_overig,
                          source='bgt',
                          layer='wegdeel',
                          source_id_column='lokaal_id',
                          reason=f"BGT wegdeel (geen spoorbaan) geclassificeerd als {PUBLIC}",
                          source_category='functie',
                          category=PUBLIC
                          )
        except TypeError:
            logger.warning("Geen BGT wegdeel aangetroffen")

        # Step 8 All BGT Waterdeel to public
        bgt_waterdeel = bgt['waterdeel']
        logger.info("Step 8: BGT waterdeel -> public")
        self.add_data(data=bgt_waterdeel,
                      source='bgt',
                      layer='waterdeel',
                      source_id_column='lokaal_id',
                      reason=f"BGT waterdeel geclassificeerd als {PUBLIC}",
                      source_category='type',
                      category=PUBLIC
                      )

        # Step 9 All BGT Ondersteunend Waterdeel to public
        logger.info("Step 9: BGT ondersteunend waterdeel -> public")
        bgt_ond_waterdeel = bgt['ondersteunend_waterdeel']
        self.add_data(data=bgt_ond_waterdeel,
                      source='bgt',
                      layer='ondersteunend_waterdeel',
                      source_id_column='lokaal_id',
                      reason=f"BGT ondersteunend waterdeel geclassificeerd als {PUBLIC}",
                      source_category='type',
                      category=PUBLIC
                      )

        # Step 10 All BGT Ondersteunend Wegdeel to public
        logger.info("Step 10: BGT ondersteunend wegdeel -> public")
        bgt_ond_wegdeel = bgt['ondersteunend_wegdeel']
        self.add_data(data=bgt_ond_wegdeel,
                      source='bgt',
                      layer='ondersteunend_wegdeel',
                      source_id_column='lokaal_id',
                      reason=f"BGT ondersteunend wegdeel geclassificeerd als {PUBLIC}",
                      source_category='fysiek_voorkomen',
                      category=PUBLIC
                      )

        # Step 11 All BGT Scheiding to private
        logger.info("Step 11: BGT scheiding -> private")
        bgt_scheiding = bgt['scheiding']
        self.add_data(data=bgt_scheiding,
                      source='bgt',
                      layer='scheiding',
                      source_id_column='lokaal_id',
                      reason=f"BGT scheiding geclassificeerd als {PRIVATE}",
                      source_category='type',
                      category=PRIVATE
                      )

        # Step 12 ALL BGT Overigbouwwerk to private
        logger.info("Step 12: BGT overigbouwwerk -> private")
        bgt_overigbouwwerk = bgt['overigbouwwerk']
        self.add_data(data=bgt_overigbouwwerk,
                      source='bgt',
                      layer='overigbouwwerk',
                      source_id_column='lokaal_id',
                      reason=f"BGT overig bouwwerk geclassificeerd als {PRIVATE}",
                      source_category='type',
                      category=PRIVATE
                      )

        # Step 13 TOP10NL functioneel gebieden with specific category to private
        logger.info("Step 13: TOP10NL functioneel gebied 'haven' -> private")
        try:
            top10nl_functioneelgebied = top10nl['functioneelgebied']
            top10nl_functioneelgebied_harbour = top10nl_functioneelgebied[
                top10nl_functioneelgebied['typefunctioneelgebied'].isin(TOP10NL_FUNCTIONEELGEBIED_HARBOUR)]
            self.add_data(data=top10nl_functioneelgebied_harbour,
                          source='top10nl',
                          layer='functioneelgebied',
                          source_id_column='lokaal_id',
                          reason=f"TOP10NL functioneel gebied van categorie 'haven', geclassificeerd als {PRIVATE}",
                          source_category='typefunctioneelgebied',
                          category=PRIVATE
                          )
        except TypeError:
            logger.warning("Geen TOP10NL functioneel gebied aangetroffen")

        # Step 14 BGT Onbegroeidterreindeel of which fysiek voorkomen =/ erf to public
        logger.info("Step 14: BGT Onbegroeidterreindeel of category other than 'erf' -> public")
        try:
            bgt_obtd_overig = bgt_onbegroeidterreindeel[bgt_onbegroeidterreindeel['fysiek_voorkomen'] != 'erf']
            self.add_data(data=bgt_obtd_overig,
                          source='bgt',
                          layer='onbegroeid_terreindeel',
                          source_id_column='lokaal_id',
                          reason="BGT onbegroeidterreindeel fysiek voorkomen van openbare categorie, zie kolom "
                                 f"'source_category'. Geclassificeerd als {PUBLIC}",
                          source_category='fysiek_voorkomen',
                          category=PUBLIC
                          )
        except TypeError:
            logger.warning("Geen BGT onbegroeidterreindeel aangetroffen")

        # Step 15 BGT kunstwerkdeel of category 'perron' to public
        logger.info("Step 15: BGT kunstwerkdeel of category 'perron' to public")
        try:
            bgt_kunstwerkdeel = bgt["kunstwerkdeel"]
            bgt_kunstwerkdeel_public = bgt_kunstwerkdeel[bgt_kunstwerkdeel['type'].isin(KUNSTWERKDEEL_PUBLIC)]
            self.add_data(data=bgt_kunstwerkdeel_public,
                          source='bgt',
                          layer='kunstwerkdeel',
                          source_id_column='lokaal_id',
                          reason="BGT kunstwerkdeel type van openbare categorie, zie kolom "
                                 f"'source_category'. Geclassificeerd als {PUBLIC}",
                          source_category='type',
                          category=PUBLIC
                          )
        except TypeError:
            logger.warning("Geen BGT kunstwerkdeel aangetroffen")

        # Step 16 Other BGT kunstwerkdeel (not of category 'perron') to private
        logger.info("Step 16: BGT kunstwerkdeel of other categories to private")
        try:
            bgt_kunstwerkdeel = bgt["kunstwerkdeel"]
            bgt_kunstwerkdeel_private = bgt_kunstwerkdeel[~bgt_kunstwerkdeel['type'].isin(KUNSTWERKDEEL_PUBLIC)]
            self.add_data(data=bgt_kunstwerkdeel_private,
                          source='bgt',
                          layer='kunstwerkdeel',
                          source_id_column='lokaal_id',
                          reason="BGT kunstwerkdeel type van niet-openbare categorie, zie kolom "
                                 f"'source_category'. Geclassificeerd als {PRIVATE}",
                          source_category='type',
                          category=PRIVATE
                          )
        except TypeError:
            logger.warning("Geen BGT kunstwerkdeel aangetroffen")

        # Step 17 BGT overbruggingsdeel to private
        logger.info("Step 17: BGT overbruggingsdeel to private")
        try:
            bgt_overbruggingsdeel = bgt["overbruggingsdeel"]
            self.add_data(data=bgt_overbruggingsdeel,
                          source='bgt',
                          layer='overbruggingsdeel',
                          source_id_column='lokaal_id',
                          reason=f"BGT overbruggingsdeel geclassificeerd als {PRIVATE}",
                          source_category='type_overbruggingsdeel',
                          category=PRIVATE
                          )
        except TypeError:
            logger.warning("Geen BGT overbruggingsdeel aangetroffen")


    def add_data(self, data: gpd.GeoDataFrame, source: str, layer, source_id_column, reason, source_category, category):
        """
        Adds data to the resulting GeoDataFrame (self.gdf). Makes sure that area that overlaps with already
        classified area cannot be added again.
        :param data: GeoDataframe with data which needs to be added
        :param source: String with the name of the source
        :param layer: String with the name of the layer
        :param source_id_column: String with the column name in which a feature identifier is stored
        :param reason: String with the reason for classification
        :param source_category: String with the column name in which a filter category was stored
        :param category: String with resulting class
        :return: Added data
        """

        logger.debug("Create buffer to fix geometries")
        self.gdf['geometry'] = self.gdf['geometry'].buffer(0)
        if data is not None:
            if not self.gdf.empty and not data.empty:
                # Define grid size
                grid_size = 1000

                # Create grid
                xmin, ymin, xmax, ymax = data.total_bounds
                x_coords = np.arange(xmin, xmax, grid_size)
                y_coords = np.arange(ymin, ymax, grid_size)
                grid = [Polygon([(x, y), (x + grid_size, y), (x + grid_size, y + grid_size), (x, y + grid_size)]) for x in
                        x_coords for y in y_coords]

                # Clean up data
                data.loc[:, 'geometry'] = data['geometry'].buffer(0)

                # Process each grid cell with a progress bar
                for cell in tqdm(grid, desc="Processing grid cells"):
                    covered_area = self.gdf[self.gdf.intersects(cell)].unary_union
                    try:
                        data_in_cell = data.clip(cell).copy()

                    except GEOSException:
                        raise


                    data_in_cell = data_in_cell[data_in_cell.geom_type.isin(['Polygon','MultiPolygon'])]
                    data_in_cell['geometry'] = data_in_cell['geometry'].difference(covered_area)

                    # Remove empty geometries
                    data_in_cell = data_in_cell[~data_in_cell.is_empty].copy()

                    # Add processed data to self.gdf
                    self._add_processed_data(data_in_cell, source, layer, source_id_column, reason, source_category,
                                             category)
            else:
                logger.debug("self.gdf is empty, adding data directly")
                self._add_processed_data(data, source, layer, source_id_column, reason, source_category, category)

            return data

    def _add_processed_data(self, data, source, layer, source_id_column, reason, source_category, category):
        """
        Helper function to add processed data to self.gdf.

        :param data: GeoDataFrame containing the data to be added.
        :param source: String with the name of the source.
        :param layer: String with the name of the layer.
        :param source_id_column: String with the column name in which a feature identifier is stored.
        :param reason: String with the reason for classification.
        :param source_category: String with the column name in which a filter category was stored.
        :param category: String with the resulting class.
        """

        data = data.copy()
        if not data.empty:
            data.loc[:, 'source_id'] = data[source_id_column]

            # Add supplied data
            data.loc[:, 'source'] = source
            data.loc[:, 'layer'] = layer
            data.loc[:, 'reason'] = reason

            if source_category:
                data.loc[:, 'source_category'] = data[source_category]
            else:
                data.loc[:, 'source_category'] = None

            data.loc[:, 'category'] = category

            # Reorganize columns to match
            data = data[list(self.gdf.columns)]

            # Concatenate to PublicSpace GeoDataFrame
            logger.debug("Add new data to result")
            self.gdf = pd.concat([self.gdf, data])

    @staticmethod
    def merge_tiled_data(tiled_data):
        """
        Merge tiled data into a single GeoDataFrame by dissolving based on all columns except 'geometry'.

        :param tiled_data: GeoDataFrame containing the tiled data to be merged.
        :return: GeoDataFrame with the merged data.
        """

        gdf = tiled_data.copy()
        dissolve_columns = gdf.columns.drop('geometry').tolist()

        gdf[dissolve_columns] = gdf[dissolve_columns].fillna('')

        # Dissolve based on the specified columns
        gdf_dissolved = gdf.dissolve(by=dissolve_columns, as_index=False)
        return gdf_dissolved

    def export(self, filename):
        """
        Export the results to a file

        :param filename: String with filename
        :return: None
        """

        self.gdf = self.merge_tiled_data(self.gdf)
        self.gdf.to_file(filename)

    def export_aggregate(self, filename, aggregate_on=None):
        """
        Export the aggregate results to a file

        :param filename: String with filename
        :param aggregate_on: List with column names on which to aggregate.
        :return: None
        """
        if aggregate_on is None:
            aggregate_on = ['category']
        aggregate = self.gdf.copy().dissolve(by=aggregate_on, as_index=False).explode()
        aggregate.to_file(filename)
