# coding=utf-8
from setuptools import setup, find_packages


filepath = 'README.md'
print(filepath)

setup(
    name='database_auto_bulk_operation',  #
    version="1.1",
    description=(
        'auto  polymerization single database task to bulk tasks and then oprate database'
    ),
    keywords=("database", 'redis','mongo','elasticsearch','mysql','bulk'),
    # long_description=open('README.md', 'r',encoding='utf8').read(),
    long_description_content_type="text/markdown",
    long_description=open(filepath, 'r', encoding='utf8').read(),
    # data_files=[filepath],
    author='bfzs',
    author_email='m13148804508@163.com',
    maintainer='ydf',
    maintainer_email='m13148804508@163.com',
    license='BSD License',
    packages=find_packages(),
    include_package_data=True,
    platforms=["all"],
    url='',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries'
    ],
    install_requires=['nb_log',
                      'decorator_libs',
                      'redis',
                      'pymongo'
                      'elasticsearch',
                      'torndb_for_python3'
                      ]
)
"""
打包上传
python setup.py sdist upload -r pypi


python setup.py sdist & twine upload dist/database_auto_bulk_operation-1.1.tar.gz



python -m pip install database_auto_bulk_operation --upgrade -i https://pypi.org/simple   # 及时的方式，不用等待 阿里云 豆瓣 同步
"""
