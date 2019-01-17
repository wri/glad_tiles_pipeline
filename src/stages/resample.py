"""
def resample(tiles):
    tiles_dir = os.path.join(region_dir, "tiles")

    # build tile-based resample jobs for zooms 10, 11 and 12
    for tile_id in os.listdir(tiles_dir):
        processing_dir = os.path.join(region_dir, "tiles", tile_id, "raster_processing")
        date_conf_dir = os.path.join(processing_dir, "date_conf", "1_prep")
        intensity_dir = os.path.join(processing_dir, "intensity", "1_prep")

        date_conf_ras = os.path.join(date_conf_dir, "date_conf_all_nd_0.tif")
        intensity_ras = os.path.join(intensity_dir, "source_intensity.tif")

        # build resample jobs to go from zoom 12 to zoom 10
        # this happens at the tile ID level - we'll then join all the z10 tiles
        # in one VRT, and resample all together to make a z9 TIF
        build_resample_job(date_conf_ras, 10, max_zoom_level, "mode", q)
        build_resample_job(intensity_ras, 10, max_zoom_level, "bilinear", q)

    # build a VRT of all z10 tifs
    date_conf_vrt = build_z10_vrt(region_dir, tiles_dir, "date_conf", q)
    intensity_vrt = build_z10_vrt(region_dir, tiles_dir, "intensity", q)

    # resample this to make z9 - z0
    build_resample_job(date_conf_vrt, 0, 9, "mode", q, False)
    build_resample_job(intensity_vrt, 0, 9, "bilinear", q, False)
"""
