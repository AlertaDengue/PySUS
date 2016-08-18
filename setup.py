from setuptools import setup, find_packages

with open('README.md') as f:
    ld = f.read()

setup(
    name='PySUS',
    version='0.1.1',
    packages=find_packages(),
    package_data={
        '': ['*.c', '*.h', '*.o', '*.so']
    },
    zip_safe=False,
    url='',
    license='gpl-v3',
    author='Flavio Codeco Coelho',
    author_email='fccoelho@gmail.com',
    description="Tools for dealing with Brazil's Public health data",
    long_description=ld,
    setup_requires=['cffi>=1.0.0'],
    cffi_modules=["pysus/utilities/_build_readdbc.py:ffibuilder"],
    install_requires=['pandas', 'dbfread', 'cffi>=1.0.0']
)
