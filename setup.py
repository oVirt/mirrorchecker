from distutils.core import setup

setup(
    name='mirrorchecker',
    version='0.2',
    description='Mirror sites freshness check tool',
    author='Nadav Goldin',
    author_email='ngoldin@redhat.com',
    url='http://mirrorchecker.readthedocs.org',
    scripts=['mirror_checker.py'],
    install_requires=[
        'paramiko>=2.0.2',
        'aiohttp',
        'PyYAML',
    ],
)
