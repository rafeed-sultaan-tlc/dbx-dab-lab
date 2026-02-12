from setuptools import setup, find_packages

setup(
    name='dbx-dab-labs',
    version='0.0.1',
    description='This contains the code in the ./src directory of the project',
    author='Rafeed Sultaan',
    packages = find_packages(where='./src'),
    package_dir={"":"./src"},
    install_requires=["setuptools"],
    entry_points={
        "packages":[
            "main=dbx_dab_labs.main:main"
        ]
    }
)