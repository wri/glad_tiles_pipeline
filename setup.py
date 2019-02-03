from setuptools import setup


setup(
    name="glad_tile_pipeline",
    version="0.1.0",
    description="Tool to process GLAD tiles",
    packages=["glad"],
    author="Thomas Maschler",
    license="MIT",
    install_requires=[
        "google-cloud-storage",
        "parallelpipe",
        "gdal",
        "numba",
        "numpy",
        "xmltodict",
        "raster2points",
    ],
    scripts=[
        "glad/glad_pipeline.py",
        "glad/scripts/pixel_depth.py",
        "glad/scripts/encode_date_conf.py",
        "glad/scripts/download_glad_tiles.py",
    ],
)
