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
# * StandardError.py
# *
# * IDENTIFICATION
# * openGauss-embedded/interface/python/intarkdb/module/StandardError.py
# *
# * -------------------------------------------------------------------------
#  */

class Warning(Exception):
    def __init__(self, ret=-1, msg=""):
        self.ret = ret
        self.msg = msg
        super().__init__()

    def __str__(self):
        return ("Warning! " + "Error code: " + str(self.ret) + "\nError message: " + str(self.msg) + "\n")


class Error(Exception):
    def __init__(self, ret=-1, msg=""):
        self.ret = ret
        self.msg = msg
        super().__init__()

    def __str__(self):
        return ("Error! " + "Error code: " + str(self.ret) + "\nError message: " + str(self.msg) + "\n")


class InterfaceError(Error):
    def __init__(self, ret=-1, msg=""):
        super().__init__(ret, msg)

    def __str__(self):
        return ("InterfaceError! " + "Error code: " + str(self.ret) + "\nError message: " + str(self.msg) + "\n")


class DatabaseError(Error):
    def __init__(self, ret=-1, msg=""):
        super().__init__(ret, msg)

    def __str__(self):
        return ("DatabaseError! " + "Error code: " + str(self.ret) + "\nError message: " + str(self.msg) + "\n")


class DataError(DatabaseError):
    def __init__(self, ret=-1, msg=""):
        super().__init__(ret, msg)

    def __str__(self):
        return ("DataError! " + "Error code: " + str(self.ret) + "\nError message: " + str(self.msg) + "\n")


class OperationalError(DatabaseError):
    def __init__(self, ret=-1, msg=""):
        super().__init__(ret, msg)

    def __str__(self):
        return ("OperationalError! " + "Error code: " + str(self.ret) + "\nError message: " + str(self.msg) + "\n")


class IntegrityError(DatabaseError):
    def __init__(self, ret=-1, msg=""):
        super().__init__(ret, msg)

    def __str__(self):
        return ("IntegrityError! " + "Error code: " + str(self.ret) + "\nError message: " + str(self.msg) + "\n")


class InternalError(DatabaseError):
    def __init__(self, ret=-1, msg=""):
        super().__init__(ret, msg)

    def __str__(self):
        return ("InternalError! " + "Error code: " + str(self.ret) + "\nError message: " + str(self.msg) + "\n")


class ProgrammingError(DatabaseError):
    def __init__(self, ret=-1, msg=""):
        super().__init__(ret, msg)

    def __str__(self):
        return ("ProgrammingError! " + "Error code: " + str(self.ret) + "\nError message: " + str(self.msg) + "\n")


class NotSupportError(DatabaseError):
    def __init__(self, ret=-1, msg=""):
        super().__init__(ret, msg)

    def __str__(self):
        return ("NotSupportError! " + "Error code: " + str(self.ret) + "\nError message: " + str(self.msg) + "\n")
