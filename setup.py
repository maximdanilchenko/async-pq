from setuptools import find_packages
from setuptools import setup


def readme(fname):
    with open(fname) as fp:
        content = fp.read()
    return content


setup(
    name='async-pq',
    version='0.3.0',
    description='Python async api for creating and managing queues in postgres',
    long_description=readme('./docs/README.md'),
    long_description_content_type="text/markdown",
    author='Danilchenko Maksim',
    author_email='dmax.dev@gmail.com',
    packages=find_packages(exclude=('test*',)),
    package_dir={'async_pq': 'async_pq'},
    include_package_data=True,
    install_requires=['asyncpg'],
    license='MIT',
    url='https://github.com/maximdanilchenko/async-pq',
    zip_safe=False,
    keywords='async postgresql asyncpg queue',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
    ],
    test_suite='tests',
)
