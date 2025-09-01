from setuptools import setup, find_packages

setup(name='publicspace',
      version='0.1.0',
      description='Tool that divides the space into public and private space, based on open source data available in '
                  'the Netherlands',
      long_description=open('README.md').read(),
      url='',
      author='Emiel Verstegen',
      author_email='emiel.verstegen@rhdhv.com',
      license='MIT',
      packages=find_packages(),
      zip_safe=False,
      install_requires=[
        'geopandas~=1.0.1',
        'pandas~=2.2.3',
        'pyogrio~=0.10.0',
        'shapely~=2.0.6',
        'numpy~=2.1.2',
        'tqdm~=4.66.5',
        'mapbox-vector-tile~=2.1.0'
    ],
      )