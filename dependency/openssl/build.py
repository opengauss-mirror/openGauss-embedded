#!/usr/bin/env python
# coding=utf-8
# description: Python script for open source software build.
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
# date: 2020-06-08

#-------------------------------------------------------------------------#
#usage:                                                                   #
#   python3/python build.py -m all -f openssl-1.1.1g.tar.gz -t "comm,llt" #
#   -m: build mode include all|build|shrink|dist|clean                    #
#   -t: build type include comm,llt,release,debug                         #
#   -f: tar file name                                                     #
#-------------------------------------------------------------------------#

import os
import subprocess
import sys
import argparse

#--------------------------------------------------------#
# open source software build operator                    #
#--------------------------------------------------------#

openssl_source_dir = "openssl"

class OPOperator():
    def __init__(self, mode, filename, compile_types):
        self.mode = mode
        self.filename = filename
        self.local_dir = os.getcwd()
        self.compile_types = compile_types.split(',')
        self.tmp_dir = os.getcwd()

#--------------------------------------------------------#
# parser build source code folder parameter              #
#--------------------------------------------------------#

    def folder_parser(self):
        ls_cmd = 'cd %s; ls ' % (self.local_dir)
        file_str = os.popen(ls_cmd).read()
        file_list = file_str.split('\n')
        for pre_str in file_list:
            if pre_str.find('.tar.gz') != -1:
                source_code = pre_str.split(".tar", 1)
                break
        return source_code[0]

#--------------------------------------------------------#
# parser build patch parameter                           #
#--------------------------------------------------------#

    def patch_parser(self):
        patch_list = []
        ls_cmd = 'cd %s; ls ' % (self.local_dir)
        file_str = os.popen(ls_cmd).read()
        file_list = file_str.split('\n')
        for pre_str in file_list:
            if pre_str.find('patch') != -1:
                patch_list.append(pre_str)
        return patch_list

#--------------------------------------------------------#
# parser build mode parameter                            #
#--------------------------------------------------------#

    def build_mode(self):
        # build mode
        if self.mode == 'build':
            self.build_component()
        elif self.mode == 'all':
            self.build_all()
        elif self.mode == 'shrink':
            self.shrink_component()
        elif self.mode == 'clean':
            self.clean_component()
        elif self.mode == 'dist':
            self.dist_component()
        else:
            print("[ERROR] Unrecognized build parameters, assert!")
            assert False

#--------------------------------------------------------#
# error log handler                                      #
#--------------------------------------------------------#

    def error_handler(self, ret):
        if ret:
            print("[ERROR] Invalid return code, exited")
            assert False

#--------------------------------------------------------#
# build all mode                                         #
#--------------------------------------------------------#

    def build_all(self):
        self.build_component()
        self.shrink_component()
        self.dist_component()
        # self.clean_component()

#--------------------------------------------------------#
# build component mode                                   #
#--------------------------------------------------------#

    def build_component(self):
        source_code_path = '%s/%s' % (self.local_dir, openssl_source_dir)
        patch_list = self.patch_parser()

        if not os.path.exists(source_code_path):
            # tar open source package
            tar_cmd = 'cd %s; mkdir -p "%s"; tar -zxvf %s -C "%s" --strip-components 1' % (self.local_dir, openssl_source_dir, self.filename, openssl_source_dir)
            self.exe_cmd(tar_cmd, True)

            # apply open source patch
            if len(patch_list):
                for pre_patch in patch_list:
                    self.exe_cmd('cd "%s"; patch -p1 < ../%s' % (source_code_path, pre_patch), True)

        cpu_num = self.exe_cmd_get_output('grep -w processor /proc/cpuinfo|wc -l')

        # compile source code type
        for c_type in self.compile_types:
            config_parameters = ''

            if c_type == 'comm':
                config_parameters = 'enable-ssl3-method -fPIC -shared -fstack-protector-strong -g -O2 -Wl,--build-id=none,-z,relro,-z,now,-z,noexecstack'

            elif c_type == 'llt':
                config_parameters = 'no-shared -fPIE -pie -fstack-protector-strong -g -O2 -Wl,--build-id=none,-z,relro,-z,now,-z,noexecstack'

            elif c_type == 'release':
                print ("[WARNING] Not supported build type. line=%d, c_type=%s" % (sys._getframe().f_lineno,c_type) )
                break
            elif c_type == 'debug':
                print ("[WARNING] Not supported build type. line=%d, c_type=%s" % (sys._getframe().f_lineno,c_type) )
                break
            else:
                print ("[WARNING] Not supported build type. line=%d, c_type=%s" % (sys._getframe().f_lineno,c_type) )
                break
            
            # deferent configuration parameter in compile types, copy srouce to deferent folder
            src_rel_path = "%s_src/%s" % (openssl_source_dir, c_type)
            src_abs_path = "%s/%s" % (self.local_dir, src_rel_path)

            if not os.path.exists(src_abs_path):
                self.exe_cmd('cd %s; mkdir -p %s; cp -r %s/*  %s' % (self.local_dir, src_rel_path, openssl_source_dir, src_rel_path))

            install_dir = self.local_dir + '/install/' + c_type
            prepare_cmd = '[ -d "%s" ] || mkdir -p "%s"' % (install_dir, install_dir)
            self.exe_cmd(prepare_cmd, True)

            config_cmd = 'cd %s; ./config %s --prefix=%s --openssldir=%s' % (src_abs_path, config_parameters, install_dir, install_dir)
            self.exe_cmd(config_cmd, True)

            make_cmd = 'cd %s; make -j%s && make install' % (src_abs_path, cpu_num)
            self.exe_cmd(make_cmd, True)
        # finish compile
        print ("[INFO] Build component finished")

#--------------------------------------------------------#
# copy open source component for using mode              #
#--------------------------------------------------------#

    def shrink_component(self):
        # shrink source code type
        self.exe_cmd('mkdir -p %s/install_comm_dist' % (self.local_dir), True)

        for c_type in self.compile_types:
            if c_type == 'comm':
                self.exe_cmd('cp -r %s/install/comm/include %s/install_comm_dist' % (self.local_dir, self.local_dir))
                self.exe_cmd('cp -r %s/install/comm/lib %s/install_comm_dist' % (self.local_dir, self.local_dir))
            elif c_type == 'llt':
                self.exe_cmd('cp -r %s/install/llt/bin %s/install_comm_dist' % (self.local_dir, self.local_dir))
            elif c_type == 'release':
                print ("[WARNING] Not supported build type. line=%d, c_type=%s" % (sys._getframe().f_lineno,c_type) )
            elif c_type == 'debug':
                print ("[WARNING] Not supported build type. line=%d, c_type=%s" % (sys._getframe().f_lineno,c_type) )
            else:
                print ("[WARNING] Not supported build type. line=%d, c_type=%s" % (sys._getframe().f_lineno,c_type) )

        self.exe_cmd('mv %s/install_comm_dist/lib/libssl.a %s/install_comm_dist/lib/libssl_static.a' % (self.local_dir, self.local_dir))
        self.exe_cmd('mv %s/install_comm_dist/lib/libcrypto.a %s/install_comm_dist/lib/libcrypto_static.a' % (self.local_dir, self.local_dir))
        self.exe_cmd('rm -rf %s/install_comm_dist/lib/engines-1.1 %s/install_comm_dist/lib/pkgconfig %s/install_comm_dist/bin/c_rehash' % (self.local_dir, self.local_dir, self.local_dir))
        self.exe_cmd('chmod -x %s/install_comm_dist/lib/lib*.so.1.1' % (self.local_dir))
        # finish shrink
        print ("[INFO] Shrink component finished")

#--------------------------------------------------------#
# move need component into matched platform binary path  #
#--------------------------------------------------------#

    def dist_component(self):
        install_path = '%s/../../output/kernel/dependency/openssl' % (self.local_dir)
        comm_path = "%s/comm" % install_path
        llt_path = "%s/llt" % install_path
        if not os.path.exists(install_path):
            os.makedirs(comm_path)
            os.makedirs(llt_path)
        # move source code type
        for c_type in self.compile_types:
            if c_type == 'comm':
                self.exe_cmd('rm -rf %s/comm/bin/* %s/comm/include/* %s/comm/lib/*' % (install_path, install_path, install_path))
                self.exe_cmd('cp -r %s/install_comm_dist/* %s/comm/' % (self.local_dir, install_path))
            elif c_type == 'llt':
                self.exe_cmd('rm -rf %s/llt/bin/* %s/llt/include/* %s/llt/lib/*' % (install_path, install_path, install_path))
                self.exe_cmd('cp -r %s/install_comm_dist/* %s/llt/' % (self.local_dir, install_path))
            elif c_type == 'release':
                print ("[WARNING] Not supported build type. line=%d, c_type=%s" % (sys._getframe().f_lineno,c_type) )
            elif c_type == 'debug':
                print ("[WARNING] Not supported build type. line=%d, c_type=%s" % (sys._getframe().f_lineno,c_type) )
            else:
                print ("[WARNING] Not supported build type. line=%d, c_type=%s" % (sys._getframe().f_lineno,c_type) )
        # finish dist
        print ("[INFO] Dist component finished")

#--------------------------------------------------------#
# clean component mode                                   #
#--------------------------------------------------------#

    def clean_component(self):
        source_code_path = openssl_source_dir
        # clean source code
        self.exe_cmd('cd %s/%s; make clean' % (self.local_dir, source_code_path), True)

        self.exe_cmd('cd %s; rm -rf %s; rm -rf install; rm -rf install_comm_dist; rm -rf openssl_src' \
            % (self.local_dir, source_code_path)
            , True)

        # finish clean
        print ("[INFO] Clean component finished")

#--------------------------------------------------------#
# base interface for executing command                   #
#--------------------------------------------------------#

    def exe_cmd(self, cmd, is_handle_error=False):
        if sys.version_info < (3, 5):
            ret = subprocess.call(cmd, shell = True)
        else:
            run_tsk = subprocess.run(cmd, shell = True, check = True)
            ret = run_tsk.returncode
        if is_handle_error:
            self.error_handler(ret)
        return ret

    def exe_cmd_get_output(self, cmd):
        status, output = subprocess.getstatusoutput(cmd)
        self.error_handler(status)
        return output.strip()


#--------------------------------------------------------#
# build script operator parameter parser                 #
#--------------------------------------------------------#

def parse_args():
    parser = argparse.ArgumentParser(description='GaussDB Kernel open source software build script')

    parser.add_argument('-m', '--mode', type=str, required=True,
                        help='build mode set, all|build|shrink|dist|clean')
    parser.add_argument('-f', '--filename', type=str, required=True,
                        help='file name set')
    parser.add_argument('-t', '--compile_types', type=str, required=True,
                        help='compile type set "comm,llt,release,debug"')
    return parser.parse_args()

#--------------------------------------------------------#
# main function                                          #
#--------------------------------------------------------#

if __name__ == '__main__':
    args = parse_args()
    Operator = OPOperator(mode = args.mode, filename = args.filename, compile_types = args.compile_types)
    Operator.build_mode()

