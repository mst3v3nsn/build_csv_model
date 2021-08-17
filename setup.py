from setuptools import setup

setup(name='build_csv_model',
      version='1.0',
      description='Module to create .CSV tables for build_csv_model based on queries made to SQL database',
      url='https://github.com/mst3v3nsn/build_csv_model.git',
      author='Matthew Stevenson',
      author_email='mstev019@odu.edu',
      license='MIT',
      packages=['build_csv_model'],
      install_requires=[
            'colorama',
            'pandas',
            'pyodbc',
            'sqlalchemy',
      ],
      classifiers=[
            'Environment :: Console',
            'Operating System :: Microsoft :: Windows',
            'Operating System :: POSIX',
            'Programming Language :: Python',
            'Topic :: Modeling Data Preparation',
      ],
      zip_safe=False)
