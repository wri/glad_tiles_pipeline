from setuptools import setup  # , Extension


setup(
    name="glad_tile_pipeline",
    version="0.0.1",
    description="Tool to process GLAD tiles",
    package_dir={"": "src"},
    packages=["stages", "helpers"],
    author="thomas.maschler",
    license="MIT",
    install_requires=["google-cloud-storage", "parallelpipe", "gdal", "numba", "numpy"],
    # entry_points={"console_scripts": ["pixel_depth=utils.pixel_depth:main"]},
    scripts=["src/helpers/pixel_depth.py", "src/helpers/encode_date_conf.py"],
    # ext_modules=[Extension("add", ["src/utils/cpp/add2.exe.cpp"],)]
)
