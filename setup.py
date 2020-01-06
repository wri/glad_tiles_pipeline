from setuptools import setup


setup(
    name="glad_tile_pipeline",
    version="0.3.2",
    description="Tool to process GLAD tiles",
    packages=["glad"],
    author="Thomas Maschler",
    license="MIT",
    install_requires=[
        "google-cloud-storage~=1.14.0",
        "parallelpipe~=0.2.6",
        "gdal~=2.4.0",
        "numba~=0.47.0",
        "numpy~=1.16.2",
        "xmltodict~=0.12.0",
        "raster2points~=0.1.6",
        "mercantile~=1.0.4",
        "boto3~=1.9.170",
    ],
    scripts=[
        "glad/glad_pipeline.py",
        "glad/check_glad_tiles.py",
        "glad/scripts/pixel_depth.py",
        "glad/scripts/encode_date_conf.py",
        "glad/scripts/download_glad_tiles.py",
    ],
)
