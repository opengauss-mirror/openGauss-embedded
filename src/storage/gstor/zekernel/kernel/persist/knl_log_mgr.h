/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * knl_log_mgr.h
 *    Log Manager Header defines
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/kernel/persist/knl_log_mgr.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __KNL_LOG_MGR_H__
#define __KNL_LOG_MGR_H__

#include "cm_defs.h"
#include "knl_log_type.h"
#include "knl_xact_log.h"
#include "knl_buffer_log.h"
#include "knl_buflatch.h"
#include "knl_space_log.h"
#include "knl_heap_log.h"
#include "rcr_btree_log.h"
#include "knl_map_log.h"
#include "pcr_heap_log.h"
#include "pcr_btree_log.h"
#include "dc_log.h"
#include "knl_log_file.h"
#include "lob_log.h"
#include "knl_sequence.h"

#ifdef __cplusplus
extern "C" {
#endif
#ifdef _REPLICATION
static log_manager_t g_lmgrs[] = {
    { RD_ENTER_PAGE, "buf_enter_page", rd_enter_page, print_enter_page, gbp_aly_enter_page, rd_check_punch_entry},
    { RD_LEAVE_PAGE, "buf_leave_page", rd_leave_page, print_leave_page, gbp_aly_leave_page, rd_check_punch_entry },
    /*
     * Notice:
     * RD_TX_XXX must be replayed by log analysis or recovery thread, GBP can not handle it, because of
     * it changes tx_area, which is just a memory area, not related to page.
     */
    { RD_TX_BEGIN,           "tx_begin",           rd_tx_begin,           print_tx_begin,           gbp_aly_tx_begin, rd_check_punch_entry },
    { RD_XA_PHASE1,          "xa_phase1",          rd_xa_phase1,          print_xa_phase1,          gbp_aly_xa_phase1, rd_check_punch_entry },
    { RD_XA_ROLLBACK_PHASE2, "xa_rollback_phase2", rd_xa_rollback_phase2, print_xa_rollback_phase2, gbp_aly_xa_rollback_phase2, rd_check_punch_entry },
    { RD_TX_END,             "tx_end",             rd_tx_end,             print_tx_end,             gbp_aly_tx_end, rd_check_punch_entry },

    /* we replay txn page when do log analysis */
    { RD_ENTER_TXN_PAGE,     "buf_enter_page",     rd_enter_page,         print_enter_page,         gbp_aly_enter_page, rd_check_punch_entry },
    { RD_LEAVE_TXN_PAGE,     "buf_leave_page",     rd_leave_page,         print_leave_page,         gbp_aly_leave_page, rd_check_punch_entry },

    { RD_SPC_CREATE_SPACE,      "space_create_space",    rd_spc_create_space,    print_spc_create_space,    gbp_aly_unsafe_entry, rd_check_punch_entry },
    { RD_SPC_REMOVE_SPACE,      "space_remove_space",    rd_spc_remove_space,    print_spc_remove_space,    gbp_aly_unsafe_entry, rd_check_punch_entry },
    { RD_SPC_CREATE_DATAFILE,   "space_create_datafile", rd_spc_create_datafile, print_spc_create_datafile, gbp_aly_unsafe_entry, rd_check_punch_entry },
    { RD_SPC_REMOVE_DATAFILE,   "space_remove_datafile", rd_spc_remove_datafile, print_spc_remove_datafile, gbp_aly_unsafe_entry, rd_check_punch_entry },
    { RD_SPC_CHANGE_SEGMENT,    "space_change_segment",  rd_spc_change_segment,  print_spc_change_segment,  gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_SPC_UPDATE_HEAD,       "space_update_head",     rd_spc_update_head,     print_spc_update_head,     gbp_aly_unsafe_entry, rd_check_punch_entry },
    { RD_SPC_UPDATE_HWM,        "space_update_hwm",      rd_spc_update_hwm,      print_spc_update_hwm,      gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_SPC_ALLOC_EXTENT,      "space_alloc_extent",    rd_spc_alloc_extent,    print_spc_alloc_extent,    gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_SPC_FREE_EXTENT,       "space_free_extent",     rd_spc_free_extent,     print_spc_free_extent,     gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_SPC_CONCAT_EXTENT,     "space_concat_extent",   rd_spc_concat_extent,   print_spc_concat_extent,   gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_SPC_FREE_PAGE,         "space_free_page",       rd_spc_free_page, print_spc_free_page,             gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_SPC_EXTEND_DATAFILE,   "space_extend_datafile", rd_spc_extend_datafile, print_spc_extend_datafile, gbp_aly_spc_extend_datafile, rd_check_punch_entry },
    { RD_SPC_TRUNCATE_DATAFILE, "space_truncate_datafile", rd_spc_truncate_datafile, print_spc_truncate_datafile, gbp_aly_unsafe_entry, rd_check_punch_entry },
    { RD_SPC_INIT_MAP_HEAD,  "datafile_init_bitmap_head", rd_df_init_map_head, print_df_init_map_head,      gbp_aly_unsafe_entry, rd_check_punch_entry },
    { RD_SPC_ADD_MAP_GROUP,  "datafile_add_bitmap_group", rd_df_add_map_group, print_df_add_map_group,      gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_SPC_INIT_MAP_PAGE,  "datafile_init_bitmap_page", rd_df_init_map_page, print_df_init_map_page,      gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_SPC_CHANGE_MAP,     "datafile_change_bitmap", rd_df_change_map, print_df_change_map,               gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_SPC_EXTEND_UNDO_SEGMENTS,    "spc_extend_undo_segments",        rd_spc_extend_undo_segments, print_spc_extend_undo_segments,           gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_SPC_PUNCH_EXTENTS,    "space_punch_extents",  rd_spc_punch_extents, print_spc_punch_extents, gbp_aly_safe_entry, rd_check_punch_entry},
    
    { RD_HEAP_FORMAT_PAGE,         "heap_format_page",         rd_heap_format_page,         print_heap_format_page, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_CONCAT_PAGE,         "heap_concat_page",         rd_heap_concat_page,         print_heap_concat_page, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_FORMAT_MAP,          "heap_format_map",          rd_heap_format_map,          print_heap_format_map,  gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_FORMAT_ENTRY,        "heap_format_entry",        rd_heap_format_entry,        print_heap_format_entry, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_ALLOC_MAP_NODE,      "heap_alloc_map",           rd_heap_alloc_map_node,      print_heap_alloc_map_node, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_CHANGE_SEG,          "heap_change_segment",      rd_heap_change_seg,          print_heap_change_seg,  gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_SET_MAP,             "heap_set_map",             rd_heap_set_map,             print_heap_set_map,     gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_CHANGE_MAP,          "heap_change_map",          rd_heap_change_map,          print_heap_change_map,  gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_CHANGE_DIR,          "heap_change_dir",          rd_heap_change_dir,          print_heap_change_dir,  gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_INSERT,              "heap_insert",              rd_heap_insert,              print_heap_insert,      gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_UPDATE_INPLACE,      "heap_update_inplace",      rd_heap_update_inplace,      print_heap_update_inplace, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_UPDATE_INPAGE,       "heap_update_inpage",       rd_heap_update_inpage,       print_heap_update_inpage, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_INSERT_MIGR,         "heap_insert_migr",         rd_heap_insert_migr,         print_heap_insert_migr, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_SET_LINK,            "heap_set_link",            rd_heap_set_link,            print_heap_set_link,    gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_REMOVE_MIGR,         "heap_remove_migr",         rd_heap_remove_migr,         print_heap_remove_migr, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_DELETE,              "heap_delete",              rd_heap_delete,              print_heap_delete,      gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_DELETE_LINK,         "heap_delete_link",         rd_heap_delete_link,         print_heap_delete_link, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_LOCK_ROW,            "heap_lock_row",            rd_heap_lock_row,            print_heap_lock_row,    gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_NEW_ITL,             "heap_new_itl",             rd_heap_new_itl,             print_heap_new_itl,     gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_REUSE_ITL,           "heap_reuse_itl",           rd_heap_reuse_itl,           print_heap_reuse_itl,   gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_CLEAN_ITL,           "heap_clean_itl",           rd_heap_clean_itl,           print_heap_clean_itl,   gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_UNDO_CHANGE_DIR,     "heap_undo_change_dir",     rd_heap_undo_change_dir,     print_heap_undo_change_dir, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_UNDO_INSERT,         "heap_undo_insert",         rd_heap_undo_insert,         print_heap_undo_insert, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_UNDO_UPDATE,         "heap_undo_update",         rd_heap_undo_update,         print_heap_undo_update, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_UNDO_UPDATE_FULL,    "heap_undo_update_full",    rd_heap_undo_update_full,    print_heap_undo_update_full, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_UNDO_DELETE,         "heap_undo_delete",         rd_heap_undo_delete,         print_heap_undo_delete, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_UNDO_DELETE_LINK,    "heap_undo_delete_link",    rd_heap_undo_delete_link,    print_heap_undo_delete_link, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_INIT_ITLS,           "heap_init_itls",           rd_heap_init_itls,           print_heap_init_itls,   gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_CHANGE_LIST,         "heap_change_list",         rd_heap_change_list,         print_heap_change_list, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_UNDO_INSERT_LINK,    "heap_undo_insert_link",    rd_heap_undo_insert_link,    print_heap_undo_insert_link, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_SHRINK_MAP,          "heap_shrink_map",          rd_heap_shrink_map,          print_heap_shrink_map,  gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_DELETE_MIGR,         "heap_delete_migr",         rd_heap_delete_migr,         print_heap_delete_migr, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_HEAP_UNDO_UPDATE_LINKRID, "heap_undo_update_linkrid", rd_heap_undo_update_linkrid, print_heap_undo_update_linkrid, gbp_aly_safe_entry, rd_check_punch_entry },

    { RD_UNDO_ALLOC_SEGMENT,  "undo_alloc_segment",  rd_undo_alloc_segment,  print_undo_alloc_segment,  gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_UNDO_CREATE_SEGMENT, "undo_create_segment", rd_undo_create_segment, print_undo_create_segment, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_UNDO_CHANGE_SEGMENT, "undo_change_segment", rd_undo_change_segment, print_undo_change_segment, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_UNDO_EXTEND_TXN,     "undo_extend_txn",     rd_undo_extend_txn,     print_undo_extend_txn,     gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_UNDO_FORMAT_TXN,     "undo_format_txn",     rd_undo_format_txn,     print_undo_format_txn,     gbp_undo_format_txn, rd_check_punch_entry },
    { RD_UNDO_CHANGE_TXN,     "undo_change_txn",     rd_undo_change_txn,     print_undo_change_txn,     gbp_aly_undo_change_txn, rd_check_punch_entry },
    { RD_UNDO_FORMAT_PAGE,    "undo_format_page",    rd_undo_format_page,    print_undo_format_page,    gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_UNDO_CHANGE_PAGE,    "undo_change_page",    rd_undo_change_page,    print_undo_change_page,    gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_UNDO_WRITE,          "undo_write",          rd_undo_write,          print_undo_write,          gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_UNDO_CLEAN,          "undo_clean",          rd_undo_clean,          print_undo_clean,          gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_UNDO_MOVE_TXN,       "undo_move_txn",       rd_undo_move_txn,       print_undo_move_txn,       gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_UNDO_CIPHER_RESERVE, "undo_cipher_reserve", rd_undo_cipher_reserve, print_undo_cipher_reserve, gbp_aly_safe_entry, rd_check_punch_entry },

    { RD_BTREE_INIT_SEG,          "btree_init_segment",      rd_btree_init_segment,       print_btree_init_segment, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_FORMAT_PAGE,       "btree_format_page",       rd_btree_format_page,        print_btree_format_page,  gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_CHANGE_SEG,        "btree_change_segment",    rd_btree_change_seg,         print_btree_change_seg,   gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_CHANGE_CHAIN,      "btree_change_chain",      rd_btree_change_chain,       print_btree_change_chain, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_DELETE,            "btree_delete",            rd_btree_delete,             print_btree_delete,       gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_COMPACT_PAGE,      "btree_compact",           rd_btree_compact,            print_btree_compact,      gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_NEW_ITL,           "btree_new_itl",           rd_btree_new_itl,            print_btree_new_itl,      gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_REUSE_ITL,         "btree_reuse_itl",         rd_btree_reuse_itl,          print_btree_reuse_itl,    gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_CLEAN_ITL,         "btree_clean_itl",         rd_btree_clean_itl,          print_btree_clean_itl,    gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_CLEAN_KEYS,        "btree_clean_moved_keys",  rd_btree_clean_moved_keys,   print_btree_clean_moved_keys, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_INSERT,            "btree_insert",            rd_btree_insert,             print_btree_insert,       gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_COPY_ITL,          "btree_copy_itl",          rd_btree_copy_itl,           print_btree_copy_itl,     gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_COPY_KEY,          "btree_copy_key",          rd_btree_copy_key,           print_btree_copy_key,     gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_UNDO_INSERT,       "btree_undo_insert",       rd_btree_undo_insert,        print_btree_undo_insert,  gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_UNDO_DELETE,       "btee_undo_delete",        rd_btree_undo_delete,        print_btree_undo_delete,  gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_CONSTRUCT_PAGE,    "btree_construct_page",    rd_btree_construct_page, NULL,                         gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_INIT_ENTRY,        "btree_init_entry",        rd_btree_init_entry,         print_btree_init_entry,   gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_CHANGE_ITL_COPIED, "btree_change_itl_copied", rd_btree_change_itl_copied, NULL,                      gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_CLEAN_KEY,         "btree_clean_key",         rd_btree_clean_key,          print_btree_clean_key,    gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_SET_RECYCLE,       "btree_set_recycle",       rd_btree_set_recycle, NULL,                            gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_NEXT_DEL_PAGE,     "btree_next_del_page",     rd_btree_next_del_page, NULL,                          gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_BTREE_UPDATE_PARTID,     "btree_update_partid",     rd_btree_update_partid,      print_update_btree_partid, gbp_aly_safe_entry, rd_check_punch_entry },
    
    { RD_LOB_PAGE_INIT,     "lob_init_page",        rd_lob_page_init,     print_lob_page_init,      gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_LOB_CHANGE_SEG,    "lob_change_segment",   rd_lob_change_seg,    print_lob_change_seg,     gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_LOB_PUT_CHUNK,     "lob_put_chunk",        rd_lob_put_chunk,     print_lob_put_chunk,      gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_LOB_CHANGE_CHUNK,  "lob_change_chunk",     rd_lob_change_chunk,  print_lob_change_chunk,   gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_LOB_PAGE_EXT_INIT, "lob_init_extent_page", rd_lob_page_ext_init, print_lob_page_ext_init,  gbp_aly_safe_entry, rd_check_punch_entry },

    { RD_PCRH_INIT_ITLS,         "pcrh_init_itls",            rd_pcrh_init_itls,            print_pcrh_init_itls,   gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_NEW_ITL,           "pcrh_new_itl",              rd_pcrh_new_itl,              print_pcrh_new_itl,     gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_REUSE_ITL,         "pcrh_reuse_itl",            rd_pcrh_reuse_itl,            print_pcrh_reuse_itl,   gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_CLEAN_ITL,         "pcrh_clean_itl",            rd_pcrh_clean_itl,            print_pcrh_clean_itl,   gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_LOCK_ROW,          "pcrh_lock_row",             rd_pcrh_lock_row,             print_pcrh_lock_row,    gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_INSERT,            "pcrh_insert",               rd_pcrh_insert,               print_pcrh_insert,      gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_UPDATE_INPLACE,    "pcrh_update_inplace",       rd_pcrh_update_inplace,       print_pcrh_update_inplace, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_UPDATE_INPAGE,     "pcrh_update_inpage",        rd_pcrh_update_inpage,        print_pcrh_update_inpage, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_DELETE,            "pcrh_delete",               rd_pcrh_delete,               print_pcrh_delete,      gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_CONVERT_LINK,      "pcrh_convert_link",         rd_pcrh_convert_link,         print_pcrh_convert_link, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_UPDATE_LINK_SSN,   "pcrh_update_link_ssn",      rd_pcrh_update_link_ssn,      print_pcrh_update_link_ssn, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_UPDATE_NEXT_RID,   "pcrh_update_next_rid",      rd_pcrh_update_next_rid,      print_pcrh_update_next_rid, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_UNDO_ITL,          "pcrh_undo_itl",             rd_pcrh_undo_itl,             print_pcrh_undo_itl,    gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_UNDO_INSERT,       "pcrh_undo_insert",          rd_pcrh_undo_insert,          print_pcrh_undo_insert, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_UNDO_DELETE,       "pcrh_undo_delete",          rd_pcrh_undo_delete,          print_pcrh_undo_delete, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_UNDO_UPDATE,       "pcrh_undo_update",          rd_pcrh_undo_update,          print_pcrh_undo_update, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_UNDO_LOCK_LINK,    "pcrh_undo_lock_link",       rd_pcrh_undo_lock_link,       print_pcrh_undo_lock_link, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_UNDO_NEXT_RID,     "pcrh_undo_update_next_rid", rd_pcrh_undo_update_next_rid, print_pcrh_undo_update_next_rid, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_RESET_SELF_CHANGE, "pcrh_reset_self_change",    rd_pcrh_reset_self_change,    print_pcrh_reset_self_change, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRH_UNDO_UPDATE_LINK_SSN, "pcrh_undo_update_link_ssn", rd_pcrh_undo_update_link_ssn, print_pcrh_undo_update_link_ssn, gbp_aly_safe_entry, rd_check_punch_entry },

    { RD_PCRB_NEW_ITL,      "pcrb_new_itl",      rd_pcrb_new_itl,      print_pcrb_new_itl,      gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRB_REUSE_ITL,    "pcrb_reuse_itl",    rd_pcrb_reuse_itl,    print_pcrb_reuse_itl,    gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRB_CLEAN_ITL,    "pcrb_clean_itl",    rd_pcrb_clean_itl,    print_pcrb_clean_itl,    gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRB_INSERT,       "pcrb_insert",       rd_pcrb_insert,       print_pcrb_insert,       gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRB_DELETE,       "pcrb_delete",       rd_pcrb_delete,       print_pcrb_delete,       gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRB_COMPACT_PAGE, "pcrb_compact_page", rd_pcrb_compact_page, print_pcrb_compact_page, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRB_COPY_ITL,     "pcrb_copy_itl",     rd_pcrb_copy_itl,     print_pcrb_copy_itl,     gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRB_COPY_KEY,     "pcrb_copy_key",     rd_pcrb_copy_key,     print_pcrb_copy_key,     gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRB_SET_SCN,      "pcrb_set_scn",      rd_pcrb_set_scn,      print_pcrb_set_scn,      gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRB_SET_COPY_ITL, "pcrb_set_copy_itl", rd_pcrb_set_copy_itl, print_pcrb_set_copy_itl, gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRB_CLEAN_KEYS,   "pcrb_clean_keys",   rd_pcrb_clean_keys,   print_pcrb_clean_keys,   gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRB_UNDO_ITL,     "pcrb_undo_itl",     rd_pcrb_undo_itl,     print_pcrb_undo_itl,     gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRB_UNDO_INSERT,  "pcrb_undo_insert",  rd_pcrb_undo_insert,  print_pcrb_undo_insert,  gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRB_UNDO_DELETE,  "pcrb_undo_delete",  rd_pcrb_undo_delete,  print_pcrb_undo_delete,  gbp_aly_safe_entry, rd_check_punch_entry },
    { RD_PCRB_CLEAN_KEY,    "pcrb_clean_key",    rd_pcrb_clean_key,    print_pcrb_clean_key,    gbp_aly_safe_entry, rd_check_punch_entry },
    
    { RD_PUNCH_FORMAT_PAGE, "spc_format_punch_page",  rd_spc_punch_format_page, print_spc_punch_format_hole, gbp_aly_safe_entry, rd_check_punch_entry },

    { RD_LOGIC_REP_INSERT, "logicRep_insert", rd_logic_rep_head_log, NULL, gbp_aly_unsafe_entry, rd_check_punch_entry },
    { RD_LOGIC_REP_UPDATE, "logicRep_update", rd_logic_rep_head_log, NULL, gbp_aly_unsafe_entry, rd_check_punch_entry },
    { RD_LOGIC_REP_DELETE, "logicRep_delete", rd_logic_rep_head_log, NULL, gbp_aly_unsafe_entry, rd_check_punch_entry },
    { RD_LOGIC_REP_DDL,    "logicRep_ddl",    rd_logic_rep_head_log, NULL, gbp_aly_unsafe_entry, rd_check_punch_entry },

    { RD_LOGIC_OPERATION, "replay_logic", rcy_replay_logic, print_replay_logic, gbp_aly_gbp_logic, rd_check_punch_entry },
};
#else
static log_manager_t g_lmgrs[] = {
    { RD_ENTER_PAGE, "buf_enter_page", rd_enter_page, print_enter_page, NULL, rd_check_punch_entry},
    { RD_LEAVE_PAGE, "buf_leave_page", rd_leave_page, print_leave_page, NULL, rd_check_punch_entry },
    /*
     * Notice:
     * RD_TX_XXX must be replayed by log analysis or recovery thread, GBP can not handle it, because of
     * it changes tx_area, which is just a memory area, not related to page.
     */
    { RD_TX_BEGIN,           "tx_begin",           rd_tx_begin,           print_tx_begin,           NULL, rd_check_punch_entry },
    { RD_XA_PHASE1,          "xa_phase1",          rd_xa_phase1,          print_xa_phase1,          NULL, rd_check_punch_entry },
    { RD_XA_ROLLBACK_PHASE2, "xa_rollback_phase2", rd_xa_rollback_phase2, print_xa_rollback_phase2, NULL, rd_check_punch_entry },
    { RD_TX_END,             "tx_end",             rd_tx_end,             print_tx_end,             NULL, rd_check_punch_entry },

    /* we replay txn page when do log analysis */
    { RD_ENTER_TXN_PAGE,     "buf_enter_page",     rd_enter_page,         print_enter_page,         NULL, rd_check_punch_entry },
    { RD_LEAVE_TXN_PAGE,     "buf_leave_page",     rd_leave_page,         print_leave_page,         NULL, rd_check_punch_entry },

    { RD_SPC_CREATE_SPACE,      "space_create_space",    rd_spc_create_space,    print_spc_create_space,    NULL, rd_check_punch_entry },
    { RD_SPC_REMOVE_SPACE,      "space_remove_space",    rd_spc_remove_space,    print_spc_remove_space,    NULL, rd_check_punch_entry },
    { RD_SPC_CREATE_DATAFILE,   "space_create_datafile", rd_spc_create_datafile, print_spc_create_datafile, NULL, rd_check_punch_entry },
    { RD_SPC_REMOVE_DATAFILE,   "space_remove_datafile", rd_spc_remove_datafile, print_spc_remove_datafile, NULL, rd_check_punch_entry },
    { RD_SPC_CHANGE_SEGMENT,    "space_change_segment",  rd_spc_change_segment,  print_spc_change_segment,  NULL, rd_check_punch_entry },
    { RD_SPC_UPDATE_HEAD,       "space_update_head",     rd_spc_update_head,     print_spc_update_head,     NULL, rd_check_punch_entry },
    { RD_SPC_UPDATE_HWM,        "space_update_hwm",      rd_spc_update_hwm,      print_spc_update_hwm,      NULL, rd_check_punch_entry },
    { RD_SPC_ALLOC_EXTENT,      "space_alloc_extent",    rd_spc_alloc_extent,    print_spc_alloc_extent,    NULL, rd_check_punch_entry },
    { RD_SPC_FREE_EXTENT,       "space_free_extent",     rd_spc_free_extent,     print_spc_free_extent,     NULL, rd_check_punch_entry },
    { RD_SPC_CONCAT_EXTENT,     "space_concat_extent",   rd_spc_concat_extent,   print_spc_concat_extent,   NULL, rd_check_punch_entry },
    { RD_SPC_FREE_PAGE,         "space_free_page",       rd_spc_free_page, print_spc_free_page,             NULL, rd_check_punch_entry },
    { RD_SPC_EXTEND_DATAFILE,   "space_extend_datafile", rd_spc_extend_datafile, print_spc_extend_datafile, gbp_aly_spc_extend_datafile, rd_check_punch_entry },
    { RD_SPC_TRUNCATE_DATAFILE, "space_truncate_datafile", rd_spc_truncate_datafile, print_spc_truncate_datafile, NULL, rd_check_punch_entry },
    { RD_SPC_INIT_MAP_HEAD,  "datafile_init_bitmap_head", rd_df_init_map_head, print_df_init_map_head,      NULL, rd_check_punch_entry },
    { RD_SPC_ADD_MAP_GROUP,  "datafile_add_bitmap_group", rd_df_add_map_group, print_df_add_map_group,      NULL, rd_check_punch_entry },
    { RD_SPC_INIT_MAP_PAGE,  "datafile_init_bitmap_page", rd_df_init_map_page, print_df_init_map_page,      NULL, rd_check_punch_entry },
    { RD_SPC_CHANGE_MAP,     "datafile_change_bitmap", rd_df_change_map, print_df_change_map,               NULL, rd_check_punch_entry },
    { RD_SPC_EXTEND_UNDO_SEGMENTS,    "spc_extend_undo_segments",        rd_spc_extend_undo_segments, print_spc_extend_undo_segments,           NULL, rd_check_punch_entry },
    { RD_SPC_PUNCH_EXTENTS,    "space_punch_extents",  rd_spc_punch_extents, print_spc_punch_extents, NULL, rd_check_punch_entry},
    
    { RD_HEAP_FORMAT_PAGE,         "heap_format_page",         rd_heap_format_page,         print_heap_format_page, NULL, rd_check_punch_entry },
    { RD_HEAP_CONCAT_PAGE,         "heap_concat_page",         rd_heap_concat_page,         print_heap_concat_page, NULL, rd_check_punch_entry },
    { RD_HEAP_FORMAT_MAP,          "heap_format_map",          rd_heap_format_map,          print_heap_format_map,  NULL, rd_check_punch_entry },
    { RD_HEAP_FORMAT_ENTRY,        "heap_format_entry",        rd_heap_format_entry,        print_heap_format_entry, NULL, rd_check_punch_entry },
    { RD_HEAP_ALLOC_MAP_NODE,      "heap_alloc_map",           rd_heap_alloc_map_node,      print_heap_alloc_map_node, NULL, rd_check_punch_entry },
    { RD_HEAP_CHANGE_SEG,          "heap_change_segment",      rd_heap_change_seg,          print_heap_change_seg,  NULL, rd_check_punch_entry },
    { RD_HEAP_SET_MAP,             "heap_set_map",             rd_heap_set_map,             print_heap_set_map,     NULL, rd_check_punch_entry },
    { RD_HEAP_CHANGE_MAP,          "heap_change_map",          rd_heap_change_map,          print_heap_change_map,  NULL, rd_check_punch_entry },
    { RD_HEAP_CHANGE_DIR,          "heap_change_dir",          rd_heap_change_dir,          print_heap_change_dir,  NULL, rd_check_punch_entry },
    { RD_HEAP_INSERT,              "heap_insert",              rd_heap_insert,              print_heap_insert,      NULL, rd_check_punch_entry },
    { RD_HEAP_UPDATE_INPLACE,      "heap_update_inplace",      rd_heap_update_inplace,      print_heap_update_inplace, NULL, rd_check_punch_entry },
    { RD_HEAP_UPDATE_INPAGE,       "heap_update_inpage",       rd_heap_update_inpage,       print_heap_update_inpage, NULL, rd_check_punch_entry },
    { RD_HEAP_INSERT_MIGR,         "heap_insert_migr",         rd_heap_insert_migr,         print_heap_insert_migr, NULL, rd_check_punch_entry },
    { RD_HEAP_SET_LINK,            "heap_set_link",            rd_heap_set_link,            print_heap_set_link,    NULL, rd_check_punch_entry },
    { RD_HEAP_REMOVE_MIGR,         "heap_remove_migr",         rd_heap_remove_migr,         print_heap_remove_migr, NULL, rd_check_punch_entry },
    { RD_HEAP_DELETE,              "heap_delete",              rd_heap_delete,              print_heap_delete,      NULL, rd_check_punch_entry },
    { RD_HEAP_DELETE_LINK,         "heap_delete_link",         rd_heap_delete_link,         print_heap_delete_link, NULL, rd_check_punch_entry },
    { RD_HEAP_LOCK_ROW,            "heap_lock_row",            rd_heap_lock_row,            print_heap_lock_row,    NULL, rd_check_punch_entry },
    { RD_HEAP_NEW_ITL,             "heap_new_itl",             rd_heap_new_itl,             print_heap_new_itl,     NULL, rd_check_punch_entry },
    { RD_HEAP_REUSE_ITL,           "heap_reuse_itl",           rd_heap_reuse_itl,           print_heap_reuse_itl,   NULL, rd_check_punch_entry },
    { RD_HEAP_CLEAN_ITL,           "heap_clean_itl",           rd_heap_clean_itl,           print_heap_clean_itl,   NULL, rd_check_punch_entry },
    { RD_HEAP_UNDO_CHANGE_DIR,     "heap_undo_change_dir",     rd_heap_undo_change_dir,     print_heap_undo_change_dir, NULL, rd_check_punch_entry },
    { RD_HEAP_UNDO_INSERT,         "heap_undo_insert",         rd_heap_undo_insert,         print_heap_undo_insert, NULL, rd_check_punch_entry },
    { RD_HEAP_UNDO_UPDATE,         "heap_undo_update",         rd_heap_undo_update,         print_heap_undo_update, NULL, rd_check_punch_entry },
    { RD_HEAP_UNDO_UPDATE_FULL,    "heap_undo_update_full",    rd_heap_undo_update_full,    print_heap_undo_update_full, NULL, rd_check_punch_entry },
    { RD_HEAP_UNDO_DELETE,         "heap_undo_delete",         rd_heap_undo_delete,         print_heap_undo_delete, NULL, rd_check_punch_entry },
    { RD_HEAP_UNDO_DELETE_LINK,    "heap_undo_delete_link",    rd_heap_undo_delete_link,    print_heap_undo_delete_link, NULL, rd_check_punch_entry },
    { RD_HEAP_INIT_ITLS,           "heap_init_itls",           rd_heap_init_itls,           print_heap_init_itls,   NULL, rd_check_punch_entry },
    { RD_HEAP_CHANGE_LIST,         "heap_change_list",         rd_heap_change_list,         print_heap_change_list, NULL, rd_check_punch_entry },
    { RD_HEAP_UNDO_INSERT_LINK,    "heap_undo_insert_link",    rd_heap_undo_insert_link,    print_heap_undo_insert_link, NULL, rd_check_punch_entry },
    { RD_HEAP_SHRINK_MAP,          "heap_shrink_map",          rd_heap_shrink_map,          print_heap_shrink_map,  NULL, rd_check_punch_entry },
    { RD_HEAP_DELETE_MIGR,         "heap_delete_migr",         rd_heap_delete_migr,         print_heap_delete_migr, NULL, rd_check_punch_entry },
    { RD_HEAP_UNDO_UPDATE_LINKRID, "heap_undo_update_linkrid", rd_heap_undo_update_linkrid, print_heap_undo_update_linkrid, NULL, rd_check_punch_entry },

    { RD_UNDO_ALLOC_SEGMENT,  "undo_alloc_segment",  rd_undo_alloc_segment,  print_undo_alloc_segment,  NULL, rd_check_punch_entry },
    { RD_UNDO_CREATE_SEGMENT, "undo_create_segment", rd_undo_create_segment, print_undo_create_segment, NULL, rd_check_punch_entry },
    { RD_UNDO_CHANGE_SEGMENT, "undo_change_segment", rd_undo_change_segment, print_undo_change_segment, NULL, rd_check_punch_entry },
    { RD_UNDO_EXTEND_TXN,     "undo_extend_txn",     rd_undo_extend_txn,     print_undo_extend_txn,     NULL, rd_check_punch_entry },
    { RD_UNDO_FORMAT_TXN,     "undo_format_txn",     rd_undo_format_txn,     print_undo_format_txn,     gbp_undo_format_txn, rd_check_punch_entry },
    { RD_UNDO_CHANGE_TXN,     "undo_change_txn",     rd_undo_change_txn,     print_undo_change_txn,     gbp_aly_undo_change_txn, rd_check_punch_entry },
    { RD_UNDO_FORMAT_PAGE,    "undo_format_page",    rd_undo_format_page,    print_undo_format_page,    NULL, rd_check_punch_entry },
    { RD_UNDO_CHANGE_PAGE,    "undo_change_page",    rd_undo_change_page,    print_undo_change_page,    NULL, rd_check_punch_entry },
    { RD_UNDO_WRITE,          "undo_write",          rd_undo_write,          print_undo_write,          NULL, rd_check_punch_entry },
    { RD_UNDO_CLEAN,          "undo_clean",          rd_undo_clean,          print_undo_clean,          NULL, rd_check_punch_entry },
    { RD_UNDO_MOVE_TXN,       "undo_move_txn",       rd_undo_move_txn,       print_undo_move_txn,       NULL, rd_check_punch_entry },
    { RD_UNDO_CIPHER_RESERVE, "undo_cipher_reserve", rd_undo_cipher_reserve, print_undo_cipher_reserve, NULL, rd_check_punch_entry },

    { RD_BTREE_INIT_SEG,          "btree_init_segment",      rd_btree_init_segment,       print_btree_init_segment, NULL, rd_check_punch_entry },
    { RD_BTREE_FORMAT_PAGE,       "btree_format_page",       rd_btree_format_page,        print_btree_format_page,  NULL, rd_check_punch_entry },
    { RD_BTREE_CHANGE_SEG,        "btree_change_segment",    rd_btree_change_seg,         print_btree_change_seg,   NULL, rd_check_punch_entry },
    { RD_BTREE_CHANGE_CHAIN,      "btree_change_chain",      rd_btree_change_chain,       print_btree_change_chain, NULL, rd_check_punch_entry },
    { RD_BTREE_DELETE,            "btree_delete",            rd_btree_delete,             print_btree_delete,       NULL, rd_check_punch_entry },
    { RD_BTREE_COMPACT_PAGE,      "btree_compact",           rd_btree_compact,            print_btree_compact,      NULL, rd_check_punch_entry },
    { RD_BTREE_NEW_ITL,           "btree_new_itl",           rd_btree_new_itl,            print_btree_new_itl,      NULL, rd_check_punch_entry },
    { RD_BTREE_REUSE_ITL,         "btree_reuse_itl",         rd_btree_reuse_itl,          print_btree_reuse_itl,    NULL, rd_check_punch_entry },
    { RD_BTREE_CLEAN_ITL,         "btree_clean_itl",         rd_btree_clean_itl,          print_btree_clean_itl,    NULL, rd_check_punch_entry },
    { RD_BTREE_CLEAN_KEYS,        "btree_clean_moved_keys",  rd_btree_clean_moved_keys,   print_btree_clean_moved_keys, NULL, rd_check_punch_entry },
    { RD_BTREE_INSERT,            "btree_insert",            rd_btree_insert,             print_btree_insert,       NULL, rd_check_punch_entry },
    { RD_BTREE_COPY_ITL,          "btree_copy_itl",          rd_btree_copy_itl,           print_btree_copy_itl,     NULL, rd_check_punch_entry },
    { RD_BTREE_COPY_KEY,          "btree_copy_key",          rd_btree_copy_key,           print_btree_copy_key,     NULL, rd_check_punch_entry },
    { RD_BTREE_UNDO_INSERT,       "btree_undo_insert",       rd_btree_undo_insert,        print_btree_undo_insert,  NULL, rd_check_punch_entry },
    { RD_BTREE_UNDO_DELETE,       "btee_undo_delete",        rd_btree_undo_delete,        print_btree_undo_delete,  NULL, rd_check_punch_entry },
    { RD_BTREE_CONSTRUCT_PAGE,    "btree_construct_page",    rd_btree_construct_page, NULL,                         NULL, rd_check_punch_entry },
    { RD_BTREE_INIT_ENTRY,        "btree_init_entry",        rd_btree_init_entry,         print_btree_init_entry,   NULL, rd_check_punch_entry },
    { RD_BTREE_CHANGE_ITL_COPIED, "btree_change_itl_copied", rd_btree_change_itl_copied, NULL,                      NULL, rd_check_punch_entry },
    { RD_BTREE_CLEAN_KEY,         "btree_clean_key",         rd_btree_clean_key,          print_btree_clean_key,    NULL, rd_check_punch_entry },
    { RD_BTREE_SET_RECYCLE,       "btree_set_recycle",       rd_btree_set_recycle, NULL,                            NULL, rd_check_punch_entry },
    { RD_BTREE_NEXT_DEL_PAGE,     "btree_next_del_page",     rd_btree_next_del_page, NULL,                          NULL, rd_check_punch_entry },
    { RD_BTREE_UPDATE_PARTID,     "btree_update_partid",     rd_btree_update_partid,      print_update_btree_partid, NULL, rd_check_punch_entry },
    
    { RD_LOB_PAGE_INIT,     "lob_init_page",        rd_lob_page_init,     print_lob_page_init,      NULL, rd_check_punch_entry },
    { RD_LOB_CHANGE_SEG,    "lob_change_segment",   rd_lob_change_seg,    print_lob_change_seg,     NULL, rd_check_punch_entry },
    { RD_LOB_PUT_CHUNK,     "lob_put_chunk",        rd_lob_put_chunk,     print_lob_put_chunk,      NULL, rd_check_punch_entry },
    { RD_LOB_CHANGE_CHUNK,  "lob_change_chunk",     rd_lob_change_chunk,  print_lob_change_chunk,   NULL, rd_check_punch_entry },
    { RD_LOB_PAGE_EXT_INIT, "lob_init_extent_page", rd_lob_page_ext_init, print_lob_page_ext_init,  NULL, rd_check_punch_entry },

    { RD_PCRH_INIT_ITLS,         "pcrh_init_itls",            rd_pcrh_init_itls,            print_pcrh_init_itls,   NULL, rd_check_punch_entry },
    { RD_PCRH_NEW_ITL,           "pcrh_new_itl",              rd_pcrh_new_itl,              print_pcrh_new_itl,     NULL, rd_check_punch_entry },
    { RD_PCRH_REUSE_ITL,         "pcrh_reuse_itl",            rd_pcrh_reuse_itl,            print_pcrh_reuse_itl,   NULL, rd_check_punch_entry },
    { RD_PCRH_CLEAN_ITL,         "pcrh_clean_itl",            rd_pcrh_clean_itl,            print_pcrh_clean_itl,   NULL, rd_check_punch_entry },
    { RD_PCRH_LOCK_ROW,          "pcrh_lock_row",             rd_pcrh_lock_row,             print_pcrh_lock_row,    NULL, rd_check_punch_entry },
    { RD_PCRH_INSERT,            "pcrh_insert",               rd_pcrh_insert,               print_pcrh_insert,      NULL, rd_check_punch_entry },
    { RD_PCRH_UPDATE_INPLACE,    "pcrh_update_inplace",       rd_pcrh_update_inplace,       print_pcrh_update_inplace, NULL, rd_check_punch_entry },
    { RD_PCRH_UPDATE_INPAGE,     "pcrh_update_inpage",        rd_pcrh_update_inpage,        print_pcrh_update_inpage, NULL, rd_check_punch_entry },
    { RD_PCRH_DELETE,            "pcrh_delete",               rd_pcrh_delete,               print_pcrh_delete,      NULL, rd_check_punch_entry },
    { RD_PCRH_CONVERT_LINK,      "pcrh_convert_link",         rd_pcrh_convert_link,         print_pcrh_convert_link, NULL, rd_check_punch_entry },
    { RD_PCRH_UPDATE_LINK_SSN,   "pcrh_update_link_ssn",      rd_pcrh_update_link_ssn,      print_pcrh_update_link_ssn, NULL, rd_check_punch_entry },
    { RD_PCRH_UPDATE_NEXT_RID,   "pcrh_update_next_rid",      rd_pcrh_update_next_rid,      print_pcrh_update_next_rid, NULL, rd_check_punch_entry },
    { RD_PCRH_UNDO_ITL,          "pcrh_undo_itl",             rd_pcrh_undo_itl,             print_pcrh_undo_itl,    NULL, rd_check_punch_entry },
    { RD_PCRH_UNDO_INSERT,       "pcrh_undo_insert",          rd_pcrh_undo_insert,          print_pcrh_undo_insert, NULL, rd_check_punch_entry },
    { RD_PCRH_UNDO_DELETE,       "pcrh_undo_delete",          rd_pcrh_undo_delete,          print_pcrh_undo_delete, NULL, rd_check_punch_entry },
    { RD_PCRH_UNDO_UPDATE,       "pcrh_undo_update",          rd_pcrh_undo_update,          print_pcrh_undo_update, NULL, rd_check_punch_entry },
    { RD_PCRH_UNDO_LOCK_LINK,    "pcrh_undo_lock_link",       rd_pcrh_undo_lock_link,       print_pcrh_undo_lock_link, NULL, rd_check_punch_entry },
    { RD_PCRH_UNDO_NEXT_RID,     "pcrh_undo_update_next_rid", rd_pcrh_undo_update_next_rid, print_pcrh_undo_update_next_rid, NULL, rd_check_punch_entry },
    { RD_PCRH_RESET_SELF_CHANGE, "pcrh_reset_self_change",    rd_pcrh_reset_self_change,    print_pcrh_reset_self_change, NULL, rd_check_punch_entry },
    { RD_PCRH_UNDO_UPDATE_LINK_SSN, "pcrh_undo_update_link_ssn", rd_pcrh_undo_update_link_ssn, print_pcrh_undo_update_link_ssn, NULL, rd_check_punch_entry },

    { RD_PCRB_NEW_ITL,      "pcrb_new_itl",      rd_pcrb_new_itl,      print_pcrb_new_itl,      NULL, rd_check_punch_entry },
    { RD_PCRB_REUSE_ITL,    "pcrb_reuse_itl",    rd_pcrb_reuse_itl,    print_pcrb_reuse_itl,    NULL, rd_check_punch_entry },
    { RD_PCRB_CLEAN_ITL,    "pcrb_clean_itl",    rd_pcrb_clean_itl,    print_pcrb_clean_itl,    NULL, rd_check_punch_entry },
    { RD_PCRB_INSERT,       "pcrb_insert",       rd_pcrb_insert,       print_pcrb_insert,       NULL, rd_check_punch_entry },
    { RD_PCRB_DELETE,       "pcrb_delete",       rd_pcrb_delete,       print_pcrb_delete,       NULL, rd_check_punch_entry },
    { RD_PCRB_COMPACT_PAGE, "pcrb_compact_page", rd_pcrb_compact_page, print_pcrb_compact_page, NULL, rd_check_punch_entry },
    { RD_PCRB_COPY_ITL,     "pcrb_copy_itl",     rd_pcrb_copy_itl,     print_pcrb_copy_itl,     NULL, rd_check_punch_entry },
    { RD_PCRB_COPY_KEY,     "pcrb_copy_key",     rd_pcrb_copy_key,     print_pcrb_copy_key,     NULL, rd_check_punch_entry },
    { RD_PCRB_SET_SCN,      "pcrb_set_scn",      rd_pcrb_set_scn,      print_pcrb_set_scn,      NULL, rd_check_punch_entry },
    { RD_PCRB_SET_COPY_ITL, "pcrb_set_copy_itl", rd_pcrb_set_copy_itl, print_pcrb_set_copy_itl, NULL, rd_check_punch_entry },
    { RD_PCRB_CLEAN_KEYS,   "pcrb_clean_keys",   rd_pcrb_clean_keys,   print_pcrb_clean_keys,   NULL, rd_check_punch_entry },
    { RD_PCRB_UNDO_ITL,     "pcrb_undo_itl",     rd_pcrb_undo_itl,     print_pcrb_undo_itl,     NULL, rd_check_punch_entry },
    { RD_PCRB_UNDO_INSERT,  "pcrb_undo_insert",  rd_pcrb_undo_insert,  print_pcrb_undo_insert,  NULL, rd_check_punch_entry },
    { RD_PCRB_UNDO_DELETE,  "pcrb_undo_delete",  rd_pcrb_undo_delete,  print_pcrb_undo_delete,  NULL, rd_check_punch_entry },
    { RD_PCRB_CLEAN_KEY,    "pcrb_clean_key",    rd_pcrb_clean_key,    print_pcrb_clean_key,    NULL, rd_check_punch_entry },
    
    { RD_PUNCH_FORMAT_PAGE, "spc_format_punch_page",  rd_spc_punch_format_page, print_spc_punch_format_hole, NULL, rd_check_punch_entry },

    { RD_LOGIC_REP_INSERT, "logicRep_insert", rd_logic_rep_head_log, NULL, NULL, rd_check_punch_entry },
    { RD_LOGIC_REP_UPDATE, "logicRep_update", rd_logic_rep_head_log, NULL, NULL, rd_check_punch_entry },
    { RD_LOGIC_REP_DELETE, "logicRep_delete", rd_logic_rep_head_log, NULL, NULL, rd_check_punch_entry },
    { RD_LOGIC_REP_DDL,    "logicRep_ddl",    rd_logic_rep_head_log, NULL, NULL, rd_check_punch_entry },

    { RD_LOGIC_OPERATION, "replay_logic", rcy_replay_logic, print_replay_logic, gbp_aly_gbp_logic, rd_check_punch_entry },
};

#endif

static logic_log_manager_t g_logic_lmgrs[] = {
    { RD_ADD_LOGFILE,           "alter_add_logfile",        rd_alter_add_logfile,       print_alter_add_logfile },
    { RD_DROP_LOGFILE,          "alter_drop_logfile",       rd_alter_drop_logfile,      print_alter_drop_logfile },
    { RD_CREATE_TABLE,          "create_table",             rd_create_table,            print_create_table },
    { RD_DROP_TABLE,            "drop_table",               rd_drop_table,              print_drop_table },
    { RD_ALTER_TABLE,           "alter_table",              rd_alter_table,             print_alter_table },
    { RD_RENAME_TABLE,          "rename_table",             rd_rename_table,            print_rename_table },
    { RD_DROP_VIEW,             "drop_view",                rd_drop_view,               print_drop_view },
    { RD_CREATE_USER,           "create_user",              rd_create_user,             print_create_user },
    { RD_DROP_USER,             "drop_user",                rd_drop_user,               print_drop_user },
    { RD_ALTER_USER,            "alter_user",               rd_alter_user,              print_alter_user },
    { RD_GRANT_PRIVS,           "alter_privs",              rd_alter_privs,             print_grant_privs },
    { RD_REVOKE_PRIVS,          "alter_privs",              rd_alter_privs,             print_revoke_privs },
    { RD_CREATE_ROLE,           "create_role",              rd_create_role,             print_create_role },
    { RD_DROP_ROLE,             "drop_role",                rd_drop_role,               print_drop_role },
    { RD_FLASHBACK_DROP,        "flashback_drop_table",     rd_flashback_drop_table,    print_flashback_drop_table },
    { RD_SPC_SET_AUTOEXTEND,    "spc_set_autoextend",       rd_spc_set_autoextend,      print_spc_set_autoextend },
    { RD_SPC_RENAME_SPACE,      "spc_rename_space",         rd_spc_rename_space,        print_spc_rename_space },
    { RD_SPC_SET_FLAG,          "spc_set_flag",             rd_spc_set_flag,            print_spc_set_flag },
    { RD_CREATE_SEQUENCE,       "create_sequence",          rd_create_sequence,         print_create_sequence },
    { RD_DROP_SEQUENCE,         "drop_sequence",            rd_drop_sequence,           print_drop_sequence },
    { RD_ALTER_SEQUENCE,        "alter_sequence",           rd_alter_sequence,          print_alter_sequence },
    { RD_SPC_CHANGE_AUTOEXTEND, "spc_change_autoextend",    rd_spc_change_autoextend,   print_spc_change_autoextend },
    { RD_CREATE_PROFILE,        "create_profile",           rd_create_profile,          print_create_profile },
    { RD_ALTER_PROFILE,         "alter_profile",            rd_alter_profile,           print_alter_profile },
    { RD_DROP_PROFILE,          "drop_profile",             rd_drop_profile,            print_drop_profile },
    { RD_CREATE_SYNONYM,        "create_synonym",           rd_create_synonym,          print_create_synonym },
    { RD_DROP_SYNONYM,          "drop_synonym",             rd_drop_synonym,            print_drop_synonym },
    { RD_UPDATE_CORE_INDEX,     "db_update_core_index",     rd_db_update_core_index,    print_db_update_core_index },
    { RD_CREATE_DISTRIBUTE_RULE, "create_distribute_rule",   rd_create_distribute_rule,  print_create_distribute_rule },
    { RD_DROP_DISTRIBUTE_RULE,  "drop_distribute_rule",     rd_drop_distribute_rule,    print_drop_distribute_rule },
    { RD_CREATE_INDEX,          "create_index",             rd_alter_table,             print_alter_table },
    { RD_ALTER_INDEX,           "alter_index",              rd_alter_table,             print_alter_table },

    { RD_DROP_INDEX,            "drop_index",               rd_alter_table,             print_alter_table },
    { RD_SWITCH_UNDO_SPACE,     "alter_switch_undo_space",  rd_switch_undo_space,       NULL },
    { RD_CREATE_MK_BEGIN,       "create_mk_begin",          rd_create_mk_begin,         print_create_mk_begin },
    { RD_CREATE_MK_DATA,        "create_mk_data",           rd_create_mk_data,          print_create_mk_data },
    { RD_CREATE_MK_END,         "create_mk_end",            rd_create_mk_end,           print_create_mk_end },
    { RD_ALTER_SERVER_MK,       "alter_server_mk",          rd_alter_server_mk,         print_alter_server_mk },
    { RD_SPC_SHRINK_CKPT,       "spc_shrink_ckpt",          rd_spc_shrink_ckpt,         print_spc_shrink_ckpt },
    { RD_CREATE_TENANT,         "create_tenant",            rd_create_tenant,           print_create_tenant },
    { RD_ALTER_TENANT,          "alter_tenant",             rd_alter_tenant,            print_alter_tenant },
    { RD_DROP_TENANT,           "drop_tenant",              rd_drop_tenant,             print_drop_tenant },
    { RD_UPDATE_SYSDATA_VERSION, "update_sysdata_version",  rd_update_sysdata_version,  print_update_sysdata_version },
    { RD_CREATE_INDEXES,        "create indexes",           rd_alter_table,             print_alter_table },
};

#define LMGR_COUNT (uint32)(sizeof(g_lmgrs) / sizeof(log_manager_t))
#define LOGIC_LMGR_COUNT (uint32)(sizeof(g_logic_lmgrs) / sizeof(logic_log_manager_t))


#ifdef __cplusplus
}
#endif

#endif
