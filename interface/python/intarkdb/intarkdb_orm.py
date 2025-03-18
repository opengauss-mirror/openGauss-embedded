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
# * intarkdb_orm.py
# *
# * IDENTIFICATION
# * openGauss-embedded/interface/python/intarkdb/intarkdb_orm.py
# *
# * -------------------------------------------------------------------------
#  */

from sqlalchemy.dialects import registry

def register():
    # 注册IntarkDB方言
    registry.register("IntarkDB", "intarkdb.orm.dialect", "IntarkDBDialect")
