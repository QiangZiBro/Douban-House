#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

@author      : QiangZiBro (qiangzibro@gmail.com)
@created     : 16/06/2022
@filename    : show_db
"""
from utils import db
from config import keywords
result = db.query("select * from houses")
for w in keywords:
    print("-----------------------------------")
    print(w)
    print("-----------------------------------")
    for d in result:
        if w in d["title"]:
            print(d["url"])

