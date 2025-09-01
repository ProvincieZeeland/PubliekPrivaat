import geopandas as gpd
import pandas as pd
import logging
import os
import gc
from multiprocessing import Process
from publicspace.settings import BGT_LAYERS, TOP10NL_LAYERS
import tempfile
import subprocess
import shutil

# Subproces-functie
def run_analysis_for_polygon(idx, row, crs, base_output_path, aoi_name):
    import geopandas as gpd
    from publicspace.publicspace import PublicSpace
    from shapely.validation import make_valid
    import os

    try:
        geom = gpd.GeoDataFrame([row], crs=crs)
        geom["geometry"] = geom["geometry"].apply(make_valid)
        geom = geom[geom.geometry.notnull() & ~geom.geometry.is_empty]

        output_id = f"{aoi_name}_area{idx}"
        bgt_path = os.path.join(base_output_path, output_id, "BGT")
        top10nl_path = os.path.join(base_output_path, output_id, "TOP10NL")
        export_path = os.path.join(base_output_path, f"{output_id}.gpkg")
        export_aggregate_path = os.path.join(base_output_path, f"{output_id}_geaggregeerd.gpkg")

        if os.path.exists(export_path) and os.path.exists(export_aggregate_path):
            print(f"[{output_id}] overslaan: resultaten bestaan al.")
            return

        os.makedirs(os.path.dirname(export_path), exist_ok=True)
        os.makedirs(os.path.dirname(export_aggregate_path), exist_ok=True)

        ps = PublicSpace(
            aoi=geom,
            bgt_path=bgt_path,
            bgt_layers=BGT_LAYERS,
            bgt_download=True,
            top10nl_path=top10nl_path,
            top10nl_layers=TOP10NL_LAYERS,
            top10nl_download=True
        )

        ps.export(export_path)
        ps.export_aggregate(export_aggregate_path)

        print(f"[{output_id}] voltooid.")
        del ps, geom
        gc.collect()
    except Exception as e:
        print(f"[{aoi_name}_area{idx}] FOUT: {e}")

# Merge-functie
def merge_geopackages(input_files, output_file, dissolve_columns=None):
    merged = None
    for file in input_files:
        if os.path.exists(file):
            try:
                gdf = gpd.read_file(file)
                if merged is None:
                    merged = gdf
                else:
                    merged = pd.concat([merged, gdf], ignore_index=True)
            except Exception as e:
                print(f"Fout bij lezen van {file}: {e}")

    if merged is not None:
        merged = gpd.GeoDataFrame(merged, geometry='geometry', crs=merged.crs)

        if dissolve_columns:
            merged[dissolve_columns] = merged[dissolve_columns].fillna('')
            merged = merged.dissolve(by=dissolve_columns, as_index=False)
            merged = merged.explode(ignore_index=True)

        merged['geometry'] = merged['geometry'].buffer(0)
        merged.to_file(output_file, driver="GPKG")

# Main entry point
def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    aoi_name = 'provincie_zeeland'
    aoi_path = fr'../data/input/{aoi_name}.gpkg'
    base_output_path = fr'C:\Temp\publicspace\data\output'

    merged_result_path = os.path.join(base_output_path, f'PublicSpace_{aoi_name}.gpkg')
    merged_agg_path = os.path.join(base_output_path, f'PublicSpace_{aoi_name}_geaggregeerd.gpkg')

    # Verwijder oude merges
    for path in [merged_result_path, merged_agg_path]:
        if os.path.exists(path):
            os.remove(path)

    aoi_gdf = gpd.read_file(aoi_path)
    processes = []

    for idx, row in aoi_gdf.iterrows():
        p = Process(target=run_analysis_for_polygon, args=(idx, row, aoi_gdf.crs, base_output_path, aoi_name))
        p.start()
        p.join()  # Sequential. Verwijder voor parallelle verwerking.
        processes.append(p)

    # Als je parallel wilt draaien, verplaats join() naar hier:
    # for p in processes:
    #     p.join()

    # Verzamel alle output-bestanden
    temp_result_files = []
    temp_agg_files = []

    for idx in range(len(aoi_gdf)):
        output_id = f"{aoi_name}_area{idx}"
        export_path = os.path.join(base_output_path, f"{output_id}.gpkg")
        export_aggregate_path = os.path.join(base_output_path, f"{output_id}_geaggregeerd.gpkg")
        if os.path.exists(export_path):
            temp_result_files.append(export_path)
        if os.path.exists(export_aggregate_path):
            temp_agg_files.append(export_aggregate_path)

    merge_geopackages(
        temp_result_files,
        merged_result_path,
        dissolve_columns=['source_id', 'source', 'layer', 'reason', 'source_category', 'category']
    )

    merge_geopackages(
        temp_agg_files,
        merged_agg_path,
        dissolve_columns=['category']
    )

    logging.info("Alle AOI's voltooid en samengevoegd.")

if __name__ == '__main__':
    main()
