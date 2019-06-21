from setuptools import setup


setup(
    name="glad_tile_pipeline",
    version="0.3.0",
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
        "mercantile",
        "boto3",
    ],
    scripts=[
        "glad/glad_pipeline.py",
        "glad/check_glad_tiles.py",
        "glad/scripts/pixel_depth.py",
        "glad/scripts/encode_date_conf.py",
        "glad/scripts/download_glad_tiles.py",
    ],
)
