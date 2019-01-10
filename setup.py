from setuptools import setup

setup(
    name="glad_tile_pipeline",
    version="0.0.1",
    description="Tool to process GLAD tiles",
    package_dir={"": "src"},
    packages=["stages", "utils"],
    author="thomas.maschler",
    install_requires=["google-cloud-storage", "parallelpipe", "gdal"],
    # entry_points={"console_scripts": ["pixel_depth=utils.pixel_depth:main"]},
    scripts=["src/utils/pixel_depth.py"],
)
