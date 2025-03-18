# /*
# * Copyright (c) GBA-NCTI-ISDC. 2022-2024.
# *
# * openGauss embedded is licensed under Mulan PSL v2.
# * You can use this software according to the terms and conditions of the Mulan PSL v2.
# * You may obtain a copy of Mulan PSL v2 at:
# *
# * http://license.coscl.org.cn/MulanPSL2
# *
# * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# * MERCHANTABILITY OR FITFOR A PARTICULAR PURPOSE.
# * See the Mulan PSL v2 for more details.
# * -------------------------------------------------------------------------
# *
# * setup.py
# *
# * IDENTIFICATION
# * openGauss-embedded/interface/python/setup.py
# *
# * -------------------------------------------------------------------------
#  */

#! /usr/bin/env python
# -*- coding: utf-8 -*_
# Author: maihaoran<maihaoran@ncti-gba.cn>
# python setup.py sdist bdist_wheel
from distutils.core import setup
import setuptools

setup(
    name='intarkdb', 
    version='1.3.0',  
    description='intarkdb Python DB API',  
    author='maihaoran', 
    author_email='maihaoran@ncti-gba.cn',  
    url='', 
    packages=setuptools.find_packages(),  
    
    # 依赖包
    install_requires=[
    ],
    classifiers=[
        'Development Status :: 1 - Beta',
        'Operating System :: Linux'  # 你的操作系统
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License', # BSD认证
        'Programming Language :: Python',   # 支持的语言
        'Programming Language :: Python :: 3',  
        'Topic :: Software Development :: Libraries'
    ],
    zip_safe=True,
    python_requires='>=3',
)
