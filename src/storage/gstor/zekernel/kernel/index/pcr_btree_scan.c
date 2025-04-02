/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * pcr_btree_scan.c
 * implement of pcr btree scan
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/kernel/index/pcr_btree_scan.c
 *
 * -------------------------------------------------------------------------
 */
#include "pcr_btree_scan.h"
#include "rcr_btree_scan.h"
#include "knl_table.h"
#include "knl_context.h"


int32 pcrb_cmp_rowid(pcrb_key_t *key1, pcrb_key_t *key2)
{
    int32 result;

    result = key1->rowid.file > key2->rowid.file ? 1 : (key1->rowid.file < key2->rowid.file ? (-1) : 0);
    if (result != 0) {
        return result;
    }

    result = key1->rowid.page > key2->rowid.page ? 1 : (key1->rowid.page < key2->rowid.page ? (-1) : 0);
    if (result != 0) {
        return result;
    }

    result = key1->rowid.slot > key2->rowid.slot ? 1 : (key1->rowid.slot < key2->rowid.slot ? (-1) : 0);
    return result;
}

int32 pcrb_cmp_column(knl_column_t *column, knl_scan_key_t *scan_key, uint32 idx_col_id, pcrb_key_t *key,
    uint16 *offset)
{
    bool32 key_is_null = !btree_get_bitmap(&key->bitmap, idx_col_id);
    uint8 flag = scan_key->flags[idx_col_id];
    int32 result;

    if (flag == SCAN_KEY_NORMAL) {
        if (SECUREC_UNLIKELY(key_is_null)) {
            return -1;
        }
        char *data1 = scan_key->buf + scan_key->offsets[idx_col_id];
        char *data2 = (char *)key + *offset;
        result = btree_cmp_column_data((void *)data1, data2, column->datatype, offset, GS_TRUE);
    } else if (flag == SCAN_KEY_IS_NULL) {
        result = (key_is_null) ? 0 : (1);
    } else if (flag == SCAN_KEY_LEFT_INFINITE || flag == SCAN_KEY_MINIMAL) {
        result = -1;
    } else {
        result = (flag == SCAN_KEY_MAXIMAL && key_is_null) ? (-1) : 1;
    }

    return result;
}

int32 pcrb_compare_key(index_t *index, knl_scan_key_t *scan_key, pcrb_key_t *key, bool32 cmp_rowid, bool32 *is_same)
{
    dc_entity_t *entity = index->entity;
    knl_column_t *column = NULL;
    int32 result;
    uint32 i;
    uint16 offset;

    if (SECUREC_LIKELY(is_same != NULL)) {
        *is_same = GS_FALSE;
    }

    if (SECUREC_UNLIKELY(key->is_infinite)) {
        return 1;
    }

    offset = sizeof(pcrb_key_t);

    for (i = 0; i < index->desc.column_count; i++) {
        column = dc_get_column(entity, index->desc.columns[i]);
        result = pcrb_cmp_column(column, scan_key, i, key, &offset);
        if (result != 0) {
            return result;
        }
    }
    // bool32 key_is_null;
    // for (int i = 0; i <index->desc.column_count; i++) {
    //     key_is_null = !btree_get_bitmap(&key->bitmap, i);
    //     if(key_is_null){
    //        break;
    //     }
    // }
    // if (cmp_rowid || BTREE_KEY_IS_NULL(key) || key_is_null) {
    if (cmp_rowid || BTREE_KEY_IS_NULL(key)) {
        result = pcrb_cmp_rowid((pcrb_key_t *)scan_key->buf, key);
    } else {
        result = 0;
    }

    if (is_same != NULL) {
        *is_same = (result == 0);
    }

    return result;
}

void pcrb_binary_search(index_t *index, btree_page_t *page, knl_scan_key_t *scan_key, btree_path_info_t *path_info,
    bool32 cmp_rowid, bool32 *is_same)
{
    pcrb_dir_t *dir = NULL;
    pcrb_key_t *cmp_key = NULL;
    int32 result;
    uint16 begin, end, curr;

    curr = 0;
    begin = 0;
    result = 0;
    end = page->keys;

    /* branch node should have at least one key */
    knl_panic_log(page->level == 0 || page->keys > 0,
        "current page's level and keys are abnormal, panic info: "
        "page %u-%u type %u, index %s page level %u page keys %u",
        AS_PAGID(page->head.id).file, AS_PAGID(page->head.id).page, page->head.type, index->desc.name, page->level,
        page->keys);
    if (SECUREC_UNLIKELY(page->keys == 0)) {
        *is_same = GS_FALSE;
    }

    while (begin < end) {
        curr = (end + begin) >> 1;
        dir = pcrb_get_dir(page, curr);
        cmp_key = PCRB_GET_KEY(page, dir);

        result = pcrb_compare_key(index, scan_key, cmp_key, cmp_rowid, is_same);
        if (result < 0) {
            end = curr;
        } else if (result > 0) {
            begin = curr + 1;
        } else {
            break;
        }
    }

    if (result > 0) {
        path_info->path[page->level].slot = curr + ((0 == page->level) ? 1 : 0);
    } else {
        path_info->path[page->level].slot = curr - ((0 == page->level) ? 0 : ((0 == result) ? 0 : 1));
    }
}

/*
 * relocate current CR page by binary search
 * This is necessary in equal fetch scenario, when we first get current page
 * which is not visible to us, then we get the CR page from CR pool. The keys
 * in CR page maybe different, so relocate the path info.
 * @param index, btree page, scan key, path info, cmp_rowid, is_same(output)
 */
void pcrb_rebinary_search(index_t *index, btree_page_t *page, knl_scan_key_t *scan_key, btree_path_info_t *path_info,
    bool32 cmp_rowid, bool32 *is_same)
{
    pcrb_dir_t *dir = NULL;
    pcrb_key_t *cmp_key = NULL;
    int32 result;
    uint16 begin, end, curr;

    knl_panic_log(page->level == 0, "page's level is incorrect, panic info: page %u-%u type %u level %u index %s",
        AS_PAGID(page->head.id).file, AS_PAGID(page->head.id).page, page->head.type, page->level, index->desc.name);

    if (path_info->path[page->level].slot < page->keys) {
        /* positively, we think the location of the key is same in current page and CR page */
        curr = (uint16)path_info->path[page->level].slot;
        dir = pcrb_get_dir(page, curr);
        cmp_key = PCRB_GET_KEY(page, dir);

        result = pcrb_compare_key(index, scan_key, cmp_key, cmp_rowid, is_same);
        if (result == 0) {
            return;
        }

        if (result < 0) {
            end = curr;
            begin = 0;
        } else {
            begin = curr + 1;
            end = page->keys;
        }
    } else {
        begin = 0;
        curr = 0;
        result = 0;
        end = page->keys;
    }

    if (page->keys == 0) {
        *is_same = GS_FALSE;
    }

    while (begin < end) {
        curr = (end + begin) >> 1;
        dir = pcrb_get_dir(page, curr);
        cmp_key = PCRB_GET_KEY(page, dir);

        result = pcrb_compare_key(index, scan_key, cmp_key, cmp_rowid, is_same);
        if (result == 0) {
            break;
        }

        if (result < 0) {
            end = curr;
        } else {
            begin = curr + 1;
        }
    }

    if (result > 0) {
        path_info->path[page->level].slot = curr + ((0 == page->level) ? 1 : 0);
    } else {
        path_info->path[page->level].slot = curr - ((0 == page->level) ? 0 : ((0 == result) ? 0 : 1));
    }
}

static void pcrb_cleanout_itls(knl_session_t *session, knl_cursor_t *cursor, btree_page_t *page, bool32 *changed)
{
    pcr_itl_t *itl = NULL;
    txn_info_t txn_info;
    rd_pcrb_clean_itl_t redo;
    bool32 need_redo = IS_LOGGING_TABLE_BY_TYPE(cursor->dc_type);
    uint8 i;

    // clean current page
    for (i = 0; i < page->itls; i++) {
        itl = pcrb_get_itl(page, i);
        if (!itl->is_active) {
            continue;
        }

        tx_get_pcr_itl_info(session, GS_FALSE, itl, &txn_info);
        if (txn_info.status != (uint8)XACT_END) {
            continue;
        }

        if (page->scn < txn_info.scn) {
            page->scn = txn_info.scn;
        }

        itl->is_active = 0;
        itl->scn = txn_info.scn;
        itl->is_owscn = (uint16)txn_info.is_owscn;
        itl->is_copied = GS_FALSE;

        redo.itl_id = i;
        redo.scn = itl->scn;
        redo.is_owscn = (uint8)itl->is_owscn;
        redo.is_copied = GS_FALSE;
        redo.aligned = (uint8)0;
        if (need_redo) {
            log_put(session, RD_PCRB_CLEAN_ITL, &redo, sizeof(rd_pcrb_clean_itl_t), LOG_ENTRY_FLAG_NONE);
        }
        *changed = GS_TRUE;
    }
}

static void pcrb_cleanout_page(knl_session_t *session, knl_cursor_t *cursor, page_id_t page_id)
{
    btree_page_t *page = NULL;
    bool32 lock_inuse = GS_FALSE;
    bool32 is_changed = GS_FALSE;

    if (SECUREC_UNLIKELY(DB_IS_READONLY(session))) {
        return;
    }

    // may be called during rollback, already in atmatic operation
    if (session->atomic_op) {
        return;
    }

    if (!lock_table_without_xact(session, cursor->dc_entity, &lock_inuse)) {
        cm_reset_error();
        return;
    }

    log_atomic_op_begin(session);
    buf_enter_page(session, page_id, LATCH_MODE_X, ENTER_PAGE_NORMAL);
    page = BTREE_CURR_PAGE;

    if (btree_check_segment_scn(page, PAGE_TYPE_PCRB_NODE, cursor->key_loc.seg_scn) != GS_SUCCESS) {
        buf_leave_page(session, GS_FALSE);
        log_atomic_op_end(session);
        unlock_table_without_xact(session, cursor->dc_entity, lock_inuse);
        return;
    }

    pcrb_cleanout_itls(session, cursor, page, &is_changed);

    cursor->cleanout = GS_FALSE;
    buf_leave_page(session, is_changed);
    log_atomic_op_end(session);

    unlock_table_without_xact(session, cursor->dc_entity, lock_inuse);
}

/*
 * get current heap page during current page cache type
 */
static inline btree_page_t *pcrb_get_curr_page(knl_session_t *session, knl_cursor_t *cursor)
{
    switch (cursor->key_loc.page_cache) {
        case LOCAL_PAGE_CACHE:
            return (btree_page_t *)cursor->page_buf;
        case NO_PAGE_CACHE:
            return BTREE_CURR_PAGE;
        case GLOBAL_PAGE_CACHE:
            return (btree_page_t *)CURR_CR_PAGE;
        default:
            return (btree_page_t *)cursor->page_buf;
    }
}

/*
 * if current page is not local cached page, we should leave it.
 */
static inline void pcrb_leave_curr_page(knl_session_t *session, key_locator_t *locator)
{
    switch (locator->page_cache) {
        case NO_PAGE_CACHE:
            buf_leave_page(session, GS_FALSE);
            break;
        case GLOBAL_PAGE_CACHE:
            pcrp_leave_page(session, GS_FALSE);
            break;
        default:
            break;
    }
}

/*
 * enter a locating btree page
 * 1. cached root page is only used when level are more than 0.
 * 2. in equal fetch, we read current page for all node.
 * 3. in range fetch, we read current page for branch node and CR page for leaf node.
 * @param kernel session, search info, locator, page_id level
 */
static status_t pcrb_enter_locate_page(knl_session_t *session, btree_search_t *search_info, key_locator_t *locator,
    page_id_t page_id, uint32 level)
{
    btree_page_t *page = NULL;

    if (level > 0) {
        /* use root_copy for root scan instead of cr page */
        if (level == search_info->tree_info.level - 1 && search_info->read_root_copy) {
            search_info->read_root_copy = GS_FALSE;
            session->index_root = search_info->btree->root_copy;
            if (BTREE_ROOT_COPY_VALID(session->index_root)) {
                page = (btree_page_t *)BTREE_GET_ROOT_COPY(session->index_root);
                if (btree_check_segment_scn(page, PAGE_TYPE_PCRB_NODE, search_info->seg_scn) == GS_SUCCESS) {
                    session->curr_page = (char *)page;
                    buf_push_page(session, NULL, LATCH_MODE_S);
                    locator->page_cache = NO_PAGE_CACHE;
                    return GS_SUCCESS;
                }
            }
        }
    } else {
        /* CR page is only used in range scan and on leaf node */
        if (!search_info->is_equal) {
            pcrp_enter_page(session, page_id, search_info->query_scn, search_info->ssn);
            page = (btree_page_t *)CURR_CR_PAGE;
            if (page != NULL) {
                if (btree_check_segment_scn(page, PAGE_TYPE_PCRB_NODE, search_info->seg_scn) == GS_SUCCESS) {
                    locator->page_cache = GLOBAL_PAGE_CACHE;
                    knl_panic_log(level == page->level,
                        "current page's level is incorrect, panic info: page %u-%u "
                        "type %u level %u",
                        page_id.file, page_id.page, page->head.type, page->level);
                    return GS_SUCCESS;
                }
                /* current CR page is invalid, just release it */
                pcrp_leave_page(session, GS_TRUE);
            }
        }
    }

    if (level > 0) {
        if (buf_read_page(session, page_id, LATCH_MODE_S, (ENTER_PAGE_NORMAL | ENTER_PAGE_HIGH_AGE)) != GS_SUCCESS) {
            return GS_ERROR;
        }
    } else if (!search_info->is_full_scan) {
        if (buf_read_page(session, page_id, LATCH_MODE_S, ENTER_PAGE_NORMAL) != GS_SUCCESS) {
            return GS_ERROR;
        }
    } else {
        if (buf_read_prefetch_page(session, page_id, LATCH_MODE_S, ENTER_PAGE_NORMAL) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    page = BTREE_CURR_PAGE;
    if (btree_check_segment_scn(page, PAGE_TYPE_PCRB_NODE, search_info->seg_scn) != GS_SUCCESS) {
        buf_leave_page(session, GS_FALSE);
        GS_THROW_ERROR(ERR_INDEX_ALREADY_DROPPED, search_info->btree->index->desc.name);
        return GS_ERROR;
    }

    if (SECUREC_UNLIKELY(!DB_IS_PRIMARY(&session->kernel->db))) {
        if (btree_check_min_scn(search_info->query_scn, search_info->btree->min_scn, page->level) != GS_SUCCESS) {
            buf_leave_page(session, GS_FALSE);
            return GS_ERROR;
        }
    }

    knl_panic_log(level == page->level, "current page's level is incorrect, panic info: page %u-%u type %u level %u",
        page_id.file, page_id.page, page->head.type, page->level);
    locator->page_cache = NO_PAGE_CACHE;
    return GS_SUCCESS;
}

static status_t pcrb_find_leaf(knl_session_t *session, btree_search_t *search_info, key_locator_t *locator,
    knl_scan_key_t *scan_key, btree_path_info_t *path_info, bool32 *is_found)
{
    index_t *index = search_info->btree->index;
    page_id_t page_id;
    pcrb_dir_t *dir = NULL;
    pcrb_key_t *curr_key = NULL;
    btree_page_t *page = NULL;
    bool32 is_same = GS_FALSE;
    bool32 cmp_rowid = search_info->is_dsc_scan ? GS_TRUE : (!(index->desc.primary || index->desc.unique));
    uint32 level;

    page_id = AS_PAGID(search_info->tree_info.root);
    level = (uint32)search_info->tree_info.level - 1;

    /*
     * desc scan always compare rowid, so if this is the first time doing find leaf, slot of level 0 is the key
     * which is the smallest key that larger than scan key(no matter index is unique or not); if this is a retry
     * find leaf process, slot of level 0 is the slot we have scanned last time. Both in these case we do a slot--
     */
    search_info->read_root_copy = GS_TRUE;
    for (;;) {
        if (pcrb_enter_locate_page(session, search_info, locator, page_id, level) != GS_SUCCESS) {
            session->index_root = NULL;
            return GS_ERROR;
        }

        /* no cursor here, cannot use pcrb_get_curr_page */
        page = (locator->page_cache == GLOBAL_PAGE_CACHE) ? (btree_page_t *)CURR_CR_PAGE : BTREE_CURR_PAGE;

        SET_ROWID_PAGE(&path_info->path[page->level], page_id);
        pcrb_binary_search(index, page, scan_key, path_info, cmp_rowid, &is_same);

        if (SECUREC_UNLIKELY(path_info->path[page->level].slot >= page->keys)) {
            if (search_info->is_dsc_scan) {
                knl_panic_log(page->level == 0,
                    "current page's level is incorrect, panic info: page %u-%u type %u"
                    " index %s page_level %u",
                    AS_PAGID(page->head.id).file, AS_PAGID(page->head.id).page, page->head.type, index->desc.name,
                    page->level);
                break;
            }

            page_id = AS_PAGID(page->next);
            if (!IS_INVALID_PAGID(page_id)) {
                pcrb_leave_curr_page(session, locator);
                continue;
            }

            pcrb_leave_curr_page(session, locator);
            session->index_root = NULL;
            *is_found = GS_FALSE;
            return GS_SUCCESS;
        }

        if (page->level == 0) {
            if (search_info->is_equal && !is_same) {
                pcrb_leave_curr_page(session, locator);
                session->index_root = NULL;
                *is_found = GS_FALSE;
                return GS_SUCCESS;
            }
            break;
        }

        dir = pcrb_get_dir(page, (uint32)path_info->path[page->level].slot);
        curr_key = PCRB_GET_KEY(page, dir);
        page_id = pcrb_get_child(curr_key);
        level = page->level - 1;
        pcrb_leave_curr_page(session, locator);
    }

    session->index_root = NULL;

    *is_found = GS_TRUE;
    return GS_SUCCESS;
}

static bool32 pcrb_check_scan_range(knl_cursor_t *cursor, index_t *index, knl_scan_key_t *filter_key,
    key_locator_t *locator, pcrb_key_t *key)
{
    knl_scan_range_t *scan_range = &cursor->scan_range;
    int32 result;

    if (!cursor->index_ss) {
        if (!cursor->key_loc.cmp_end) {
            return GS_TRUE;
        }

        if (cursor->index_dsc) {
            bool32 cmp_rowid = BTREE_NEED_CMP_ROWID(cursor, index);
            if (pcrb_compare_key(index, filter_key, key, cmp_rowid, NULL) <= 0) {
                return GS_TRUE;
            }
        } else {
            bool32 cmp_rowid = (!scan_range->is_equal && BTREE_NEED_CMP_ROWID(cursor, index));
            if (pcrb_compare_key(index, filter_key, key, cmp_rowid, NULL) >= 0) {
                return GS_TRUE;
            }
        }

        return GS_FALSE;
    }

    knl_column_t *column = dc_get_column(index->entity, index->desc.columns[0]);
    uint16 offset = sizeof(pcrb_key_t);

    if (cursor->index_dsc) {
        if (SECUREC_UNLIKELY(key->is_infinite)) {
            return GS_FALSE;
        }

        if (pcrb_cmp_column(column, &scan_range->r_key, 0, key, &offset) > 0) {
            return GS_FALSE;
        }

        for (uint32 i = 1; i < index->desc.column_count; i++) {
            column = dc_get_column(index->entity, index->desc.columns[i]);
            result = pcrb_cmp_column(column, &scan_range->l_key, i, key, &offset);
            if (result > 0) {
                return GS_FALSE;
            } else if (result < 0) {
                return GS_TRUE;
            }
        }
    } else {
        if (pcrb_cmp_column(column, &scan_range->l_key, 0, key, &offset) < 0) {
            return GS_FALSE;
        }

        for (uint32 i = 1; i < index->desc.column_count; i++) {
            column = dc_get_column(index->entity, index->desc.columns[i]);
            result = pcrb_cmp_column(column, &scan_range->r_key, i, key, &offset);
            if (result < 0) {
                return GS_FALSE;
            } else if (result > 0) {
                return GS_TRUE;
            }
        }
    }

    return GS_TRUE;
}

void pcrb_get_end_slot(knl_session_t *session, knl_cursor_t *cursor, btree_page_t *page)
{
    knl_scan_range_t *scan_range;
    index_t *index;
    key_locator_t *locator;
    knl_scan_key_t *end_key = NULL;
    pcrb_dir_t *dir = NULL;
    pcrb_key_t *key = NULL;
    uint16 slot;

    locator = &cursor->key_loc;
    scan_range = &cursor->scan_range;
    index = (index_t *)cursor->index;

    if (cursor->index_dsc) {
        end_key = &scan_range->l_key;
        locator->slot_end = INVALID_SLOT;
        slot = 0;

        for (;;) {
            if (locator->slot - slot < BTREE_COMPARE_SLOT_GAP) {
                return;
            }

            dir = pcrb_get_dir(page, slot);
            key = PCRB_GET_KEY(page, dir);
            if (pcrb_check_scan_range(cursor, index, end_key, locator, key)) {
                locator->slot_end = slot;
                return;
            }

            slot = (locator->slot + slot) >> 1;
        }
    } else {
        end_key = scan_range->is_equal ? &scan_range->l_key : &scan_range->r_key;
        locator->slot_end = INVALID_SLOT;
        slot = page->keys - 1;

        for (;;) {
            if (slot - locator->slot < BTREE_COMPARE_SLOT_GAP) {
                return;
            }

            dir = pcrb_get_dir(page, slot);
            key = PCRB_GET_KEY(page, dir);
            if (pcrb_check_scan_range(cursor, index, end_key, locator, key)) {
                locator->slot_end = slot;
                return;
            }

            slot = (locator->slot + slot) >> 1;
        }
    }
}

/*
 * find current prev page
 * The start_id(B) is the prev page_id on the page of the page_id(A)
 * Maybe the current of B has been split and it's next page is not A.
 * We should find the last page whose next page is A.
 * @param kernel session, search info, locator, page_id(A), start_id(B)
 */
static status_t pcrb_find_prev_page(knl_session_t *session, btree_search_t *search_info, key_locator_t *locator,
    page_id_t page_id, page_id_t start_id, bool32 *located)
{
    btree_page_t *page = NULL;
    page_id_t next_id = start_id;

    *located = GS_TRUE;
    if (pcrb_enter_locate_page(session, search_info, locator, next_id, 0) != GS_SUCCESS) {
        return GS_ERROR;
    }

    /* no cursor here, cannot use pcrb_get_curr_page */
    page = (locator->page_cache == GLOBAL_PAGE_CACHE) ? (btree_page_t *)CURR_CR_PAGE : BTREE_CURR_PAGE;

    next_id = AS_PAGID(page->next);
    if (IS_SAME_PAGID(next_id, page_id)) {
        return GS_SUCCESS;
    } else {
        *located = GS_FALSE;
    }

    return GS_SUCCESS;
}

static status_t pcrb_get_invisible_itl(knl_session_t *session, knl_cursor_t *cursor, knl_scn_t query_scn,
    btree_page_t *cr_page, pcr_itl_t **itl)
{
    pcr_itl_t *item = NULL;
    txn_info_t txn_info;
    uint8 i;

    *itl = NULL;

    for (i = 0; i < cr_page->itls; i++) {
        item = pcrb_get_itl(cr_page, i);
        tx_get_pcr_itl_info(session, GS_TRUE, item, &txn_info);

        if (txn_info.status == (uint8)XACT_END) {
            if (item->is_active) {
                cursor->cleanout = GS_TRUE;
                item->is_active = 0;
                item->scn = txn_info.scn;
                item->is_owscn = (uint16)txn_info.is_owscn;
            }

            if (txn_info.scn <= query_scn) {
                continue;
            }

            if (txn_info.is_owscn) {
                tx_record_sql(session);
                GS_LOG_RUN_ERR("snapshot too old, detail: itl owscn %llu, query scn %llu", txn_info.scn, query_scn);
                GS_THROW_ERROR(ERR_SNAPSHOT_TOO_OLD);
                return GS_ERROR;
            }

            if (cursor->isolevel == (uint8)ISOLATION_SERIALIZABLE) {
                cursor->ssi_conflict = GS_TRUE;
            }

            /* find the recent itl to do CR rollback */
            if ((*itl) == NULL || (*itl)->scn < item->scn) {
                *itl = item;
            }
        } else {
            if (item->xid.value == cursor->xid) {
                if (item->ssn < cursor->ssn) {
                    continue;
                }
            } else if (TX_XA_CONSISTENCY(session)) {
                if ((txn_info.status == (uint8)XACT_PHASE1 || txn_info.status == (uint8)XACT_PHASE2) &&
                    txn_info.scn < query_scn) {
                    GS_LOG_DEBUG_INF("wait prepared transaction %u-%u-%u, status %u, scn %llu, query_scn %llu",
                        item->xid.xmap.seg_id, item->xid.xmap.slot, item->xid.xnum, txn_info.status, txn_info.scn,
                        query_scn);
                    session->wxid = item->xid;
                    ROWID_COPY(session->wrid, cursor->rowid);
                    return GS_SUCCESS;
                }
            }

            /* for active itl, just return to do CR rollback */
            *itl = item;
            return GS_SUCCESS;
        }
    }

    if (*itl != NULL) {
        return GS_SUCCESS;
    }

    /* user current query_scn as CR page scn */
    cr_page->scn = query_scn;

    return GS_SUCCESS;
}

static void pcrb_revert_itl(knl_session_t *session, btree_page_t *cr_page, pcr_itl_t *itl, undo_row_t *ud_row)
{
    itl->xid = *(xid_t *)ud_row->data;
    itl->scn = ud_row->scn;
    itl->is_owscn = ud_row->is_owscn;
    itl->undo_page = ud_row->prev_page;
    itl->undo_slot = ud_row->prev_slot;
    itl->is_active = GS_FALSE;
}

static void pcrb_revert_insert_key(knl_cursor_t *cursor, index_t *index, bool32 cmp_rowid, pcrb_key_t *ud_key,
    btree_page_t *cr_page)
{
    btree_path_info_t path_info;
    knl_scan_key_t scan_key;
    bool32 is_found = GS_FALSE;

    pcrb_decode_key(index, ud_key, &scan_key);
    pcrb_binary_search(index, cr_page, &scan_key, &path_info, cmp_rowid, &is_found);

    /* if current undo key is not in CR page, skip rollback */
    if (is_found) {
        pcrb_dir_t *dir = pcrb_get_dir(cr_page, (uint32)path_info.path[0].slot);
        pcrb_key_t *key = PCRB_GET_KEY(cr_page, dir);
        key->is_deleted = 1;

        ROWID_COPY(key->rowid, ud_key->rowid);

        if (IS_PART_TABLE(cursor->table) && !IS_PART_INDEX(index)) {
            if (IS_COMPART_TABLE(((table_t *)cursor->table)->part_table)) {
                pcrb_set_subpart_id(key, pcrb_get_subpart_id(ud_key));
                pcrb_set_part_id(key, pcrb_get_part_id(ud_key));
            } else {
                pcrb_set_part_id(key, pcrb_get_part_id(ud_key));
            }
        }
    }
}

static void pcrb_revert_insert(knl_session_t *session, knl_cursor_t *cursor, btree_t *btree, btree_page_t *cr_page,
    pcr_itl_t *itl, undo_row_t *ud_row)
{
    index_t *index = btree->index;
    pcrb_key_t *ud_key = (pcrb_key_t *)ud_row->data;
    bool32 cmp_rowid = (index->desc.primary || index->desc.unique) ? GS_FALSE : GS_TRUE;

    knl_panic_log(cr_page->level == 0,
        "current page's level is incorrect, panic info: page %u-%u type %u level %u "
        "table %s index %s",
        AS_PAGID(cr_page->head.id).file, AS_PAGID(cr_page->head.id).page, cr_page->head.type, cr_page->level,
        ((table_t *)cursor->table)->desc.name, index->desc.name);
    pcrb_revert_insert_key(cursor, index, cmp_rowid, ud_key, cr_page);
    itl->ssn = ud_row->ssn;
    itl->undo_page = ud_row->prev_page;
    itl->undo_slot = ud_row->prev_slot;
}

static void pcrb_revert_batch_insert(knl_session_t *session, knl_cursor_t *cursor, btree_t *btree,
    btree_page_t *cr_page, pcr_itl_t *itl, undo_row_t *ud_row)
{
    index_t *index = btree->index;
    bool32 cmp_rowid = (index->desc.primary || index->desc.unique) ? GS_FALSE : GS_TRUE;
    pcrb_undo_batch_insert_t *batch_insert = (pcrb_undo_batch_insert_t *)ud_row->data;
    pcrb_key_t *ud_key = NULL;
    uint16 offset = 0;
    knl_panic_log(cr_page->level == 0,
        "current page's level is incorrect, panic info: page %u-%u type %u level %u "
        "table %s index %s",
        AS_PAGID(cr_page->head.id).file, AS_PAGID(cr_page->head.id).page, cr_page->head.type, cr_page->level,
        ((table_t *)cursor->table)->desc.name, index->desc.name);
    for (uint32 i = 0; i < batch_insert->count; i++) {
        ud_key = (pcrb_key_t *)((char *)batch_insert->keys + offset);
        pcrb_revert_insert_key(cursor, index, cmp_rowid, ud_key, cr_page);
        offset += (uint16)ud_key->size;
    }

    itl->ssn = ud_row->ssn;
    itl->undo_page = ud_row->prev_page;
    itl->undo_slot = ud_row->prev_slot;
}

static void pcrb_revert_delete(knl_session_t *session, knl_cursor_t *cursor, btree_t *btree, btree_page_t *cr_page,
    pcr_itl_t *itl, undo_row_t *ud_row)
{
    index_t *index;
    pcrb_dir_t *dir = NULL;
    pcrb_key_t *key = NULL;
    pcrb_key_t *ud_key;
    knl_scan_key_t scan_key;
    bool32 cmp_rowid;
    bool32 is_found = GS_FALSE;
    btree_path_info_t path_info;

    index = btree->index;
    ud_key = (pcrb_key_t *)ud_row->data;
    cmp_rowid = (index->desc.primary || index->desc.unique) ? GS_FALSE : GS_TRUE;

    knl_panic_log(cr_page->level == 0,
        "current page's level is incorrect, panic info: page %u-%u type %u level %u "
        "table %s index %s",
        AS_PAGID(cr_page->head.id).file, AS_PAGID(cr_page->head.id).page, cr_page->head.type, cr_page->level,
        ((table_t *)cursor->table)->desc.name, index->desc.name);
    pcrb_decode_key(btree->index, ud_key, &scan_key);
    pcrb_binary_search(btree->index, cr_page, &scan_key, &path_info, cmp_rowid, &is_found);

    /* if current undo key is not in CR page, skip rollback */
    if (is_found) {
        dir = pcrb_get_dir(cr_page, (uint32)path_info.path[0].slot);
        key = PCRB_GET_KEY(cr_page, dir);
        key->is_deleted = 0;
    }

    itl->ssn = ud_row->ssn;
    itl->undo_page = ud_row->prev_page;
    itl->undo_slot = ud_row->prev_slot;
}

static void pcrb_reorganize_with_undo(knl_session_t *session, knl_cursor_t *cursor, btree_t *btree,
    btree_page_t *cr_page, pcr_itl_t *itl, undo_row_t *ud_row)
{
    switch (ud_row->type) {
        case UNDO_PCRB_ITL:
            pcrb_revert_itl(session, cr_page, itl, ud_row);
            break;

        case UNDO_PCRB_INSERT:
            pcrb_revert_insert(session, cursor, btree, cr_page, itl, ud_row);
            break;

        case UNDO_PCRB_DELETE:
            pcrb_revert_delete(session, cursor, btree, cr_page, itl, ud_row);
            break;

        case UNDO_PCRB_BATCH_INSERT:
            pcrb_revert_batch_insert(session, cursor, btree, cr_page, itl, ud_row);
            break;

        default:
            knl_panic_log(0, "ud_row type is unknown, panic info: page %u-%u type %u table %s index %s ud_row type %u",
                AS_PAGID(cr_page->head.id).file, AS_PAGID(cr_page->head.id).page, cr_page->head.type,
                ((table_t *)cursor->table)->desc.name, ((index_t *)btree->index)->desc.name, ud_row->type);
            break;
    }
}

static status_t pcrb_reorganize_with_undo_list(knl_session_t *session, knl_cursor_t *cursor, btree_t *btree,
    btree_page_t *cr_page, pcr_itl_t *itl)
{
    itl->is_hist = GS_TRUE;
    if (!itl->is_active) {
        itl->is_active = GS_TRUE;
    }

    for (;;) {
        if (buf_read_page(session, PAGID_U2N(itl->undo_page), LATCH_MODE_S, ENTER_PAGE_NORMAL) != GS_SUCCESS) {
            return GS_ERROR;
        }

        undo_page_t *ud_page = (undo_page_t *)CURR_PAGE;
        if (itl->undo_slot >= ud_page->rows) {
            buf_leave_page(session, GS_FALSE);
            tx_record_sql(session);
            GS_LOG_RUN_ERR("snapshot too old, detail: snapshot slot %u, undo rows %u, query scn %llu",
                (uint32)itl->undo_slot, (uint32)ud_page->rows, cursor->query_scn);
            GS_THROW_ERROR(ERR_SNAPSHOT_TOO_OLD);
            return GS_ERROR;
        }

        undo_row_t *ud_row = UNDO_ROW(ud_page, itl->undo_slot);
        if (itl->xid.value != ud_row->xid.value) {
            buf_leave_page(session, GS_FALSE);
            tx_record_sql(session);
            GS_LOG_RUN_ERR("snapshot too old, detail: snapshot xid %llu, undo row xid %llu, query scn %llu",
                itl->xid.value, ud_row->xid.value, cursor->query_scn);
            GS_THROW_ERROR(ERR_SNAPSHOT_TOO_OLD);
            return GS_ERROR;
        }

        /* support statement level read consistency */
        if (ud_row->xid.value == cursor->xid && ud_row->ssn < cursor->ssn) {
            itl->ssn = ud_row->ssn;
            buf_leave_page(session, GS_FALSE);
            return GS_SUCCESS;
        }

        pcrb_reorganize_with_undo(session, cursor, btree, cr_page, itl, ud_row);

        /* current itl is done, caller should find a new recent itl to do CR rollback */
        if (ud_row->type == UNDO_PCRB_ITL) {
            buf_leave_page(session, GS_FALSE);
            return GS_SUCCESS;
        }

        buf_leave_page(session, GS_FALSE);
    }
}

static status_t pcrb_construct_cr_page(knl_session_t *session, knl_cursor_t *cursor, knl_scn_t query_scn,
    btree_page_t *cr_page)
{
    if (!session->is_timescale || session->rm->prev != GS_INVALID_ID16)
    {
        btree_t *btree;
        pcr_itl_t *itl = NULL;

        btree = CURSOR_BTREE(cursor);
        cursor->ssi_conflict = GS_FALSE;
        bool8 constructed = GS_FALSE;
        for (;;)
        {
            if (pcrb_get_invisible_itl(session, cursor, query_scn, cr_page, &itl) != GS_SUCCESS)
            {
                return GS_ERROR;
            }

            if (itl == NULL || session->wxid.value != GS_INVALID_ID64)
            {
                /*
                 * 1.no invisible itl, just return current CR page
                 * 2.waiting for prepared transaction
                 */
                if (constructed)
                {
                    session->stat.bcr_construct_count++;
                }
                return GS_SUCCESS;
            }
            if (pcrb_reorganize_with_undo_list(session, cursor, btree, cr_page, itl) != GS_SUCCESS)
            {
                return GS_ERROR;
            }
            constructed = GS_TRUE;
        }
    }
    return GS_SUCCESS;
}

static status_t pcrb_local_cache_page(knl_session_t *session, knl_cursor_t *cursor, key_locator_t *locator,
    bool32 *retry)
{
    errno_t ret;
    btree_page_t *page = pcrb_get_curr_page(session, cursor);

    *retry = GS_FALSE;
    if (locator->page_cache == GLOBAL_PAGE_CACHE) {
        ret = memcpy_sp(cursor->page_buf, DEFAULT_PAGE_SIZE, (char *)page, DEFAULT_PAGE_SIZE);
        knl_securec_check(ret);
        pcrp_leave_page(session, GS_FALSE); /* leave CR page */

        locator->page_cache = LOCAL_PAGE_CACHE;
        return GS_SUCCESS;
    }

    ret = memcpy_sp(cursor->page_buf, DEFAULT_PAGE_SIZE, (char *)page, DEFAULT_PAGE_SIZE);
    knl_securec_check(ret);
    pcrb_leave_curr_page(session, locator);

    if (pcrb_construct_cr_page(session, cursor, cursor->query_scn, (btree_page_t *)cursor->page_buf) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (session->wxid.value != GS_INVALID_ID64) {
        if (tx_wait(session, 0, ENQ_TX_READ_WAIT) != GS_SUCCESS) {
            tx_record_rowid(session->wrid);
            return GS_ERROR;
        }
        *retry = GS_TRUE;
        return GS_SUCCESS;
    }

    if (cursor->global_cached) {
        pcrp_alloc_page(session, AS_PAGID(page->head.id), cursor->query_scn, (uint32)cursor->ssn);
        page = (btree_page_t *)CURR_CR_PAGE;
        ret = memcpy_sp((char *)page, DEFAULT_PAGE_SIZE, cursor->page_buf, DEFAULT_PAGE_SIZE);
        knl_securec_check(ret);
        pcrp_leave_page(session, GS_FALSE); /* leave CR page */
    }

    locator->page_cache = LOCAL_PAGE_CACHE;
    return GS_SUCCESS;
}

static status_t pcrb_locate_key(knl_session_t *session, btree_search_t *search_info, knl_cursor_t *cursor,
    knl_scan_key_t *scan_key, btree_path_info_t *path_info)
{
    btree_t *btree = search_info->btree;
    key_locator_t *locator = &cursor->key_loc;
    bool32 is_found = GS_FALSE;

    if (search_info->is_dsc_scan) {
        cm_latch_s(&btree->struct_latch, session->id, GS_FALSE, &session->stat_btree);
    }
    search_info->tree_info.value = cm_atomic_get(&btree->segment->tree_info.value);
    if (!spc_validate_page_id(session, AS_PAGID(&search_info->tree_info.root))) {
        if (search_info->is_dsc_scan) {
            cm_unlatch(&btree->struct_latch, &session->stat_btree);
        }
        GS_THROW_ERROR(ERR_INDEX_ALREADY_DROPPED, btree->index->desc.name);
        return GS_ERROR;
    }

    if (cursor->isolevel == ISOLATION_CURR_COMMITTED) {
        cursor->query_scn = DB_CURR_SCN(session);
        search_info->query_scn = cursor->query_scn;
        cursor->cc_cache_time = KNL_NOW(session);
    }

    if (pcrb_find_leaf(session, search_info, locator, scan_key, path_info, &is_found) != GS_SUCCESS) {
        if (search_info->is_dsc_scan) {
            cm_unlatch(&btree->struct_latch, &session->stat_btree);
        }
        return GS_ERROR;
    }

    if (!is_found) {
        if (search_info->is_dsc_scan) {
            cm_unlatch(&btree->struct_latch, &session->stat_btree);
        }
        cursor->eof = GS_TRUE;
        return GS_SUCCESS;
    }

    if (search_info->is_dsc_scan) {
        if (path_info->path[0].slot == 0) {
            btree_page_t *page = pcrb_get_curr_page(session, cursor);
            page_id_t prev_id = AS_PAGID(page->prev);
            pcrb_leave_curr_page(session, locator);

            path_info->path[0].page = prev_id.page;
            path_info->path[0].file = prev_id.file;

            if (pcrb_enter_locate_page(session, search_info, locator, prev_id, 0) != GS_SUCCESS) {
                cm_unlatch(&btree->struct_latch, &session->stat_btree);
                return GS_ERROR;
            }

            page = pcrb_get_curr_page(session, cursor);
            path_info->path[0].slot = page->keys - 1;
        } else {
            path_info->path[0].slot--;
        }

        cm_unlatch(&btree->struct_latch, &session->stat_btree);
    }

    return GS_SUCCESS;
}

/*
 * check key visible
 * We do this check before we try to construct a CR page in equal fetch
 * The main idea is to reduce CR page construct as little as possible.
 * @attention This function in only called under PCR btree equal fetch.
 * @param kernel session, kernel cursor, query scn, btree page, key slot
 */
static bool32 pcrb_check_key_visible(knl_session_t *session, knl_cursor_t *cursor, knl_scn_t query_scn,
    btree_page_t *page, uint16 slot)
{
    pcrb_dir_t *dir = NULL;
    pcrb_key_t *key = NULL;
    pcr_itl_t *itl = NULL;
    txn_info_t txn_info;

    dir = pcrb_get_dir(page, slot);
    key = PCRB_GET_KEY(page, dir);
    if (key->itl_id == GS_INVALID_ID8) {
        txn_info.scn = page->scn;
        txn_info.status = (uint8)XACT_END;
    } else {
        itl = pcrb_get_itl(page, key->itl_id);
        tx_get_pcr_itl_info(session, GS_TRUE, itl, &txn_info);
    }

    if (txn_info.status == (uint8)XACT_END) {
        if (txn_info.scn <= query_scn) {
            return GS_TRUE;
        }
    } else {
        if (itl->xid.value == cursor->xid && itl->ssn < cursor->ssn) {
            return GS_TRUE;
        }
    }

    return GS_FALSE;
}

static inline void pcrb_init_search_info(btree_search_t *search_info, knl_cursor_t *cursor)
{
    btree_t *btree = CURSOR_BTREE(cursor);

    search_info->btree = btree;
    search_info->seg_scn = (IS_PART_INDEX((cursor)->index) ? ((index_part_t *)(cursor)->index_part)->desc.seg_scn :
                                                             ((index_t *)(cursor)->index)->desc.seg_scn);
    search_info->is_dsc_scan = (bool32)cursor->index_dsc;
    search_info->is_equal =
        cursor->scan_range.is_equal && IS_UNIQUE_PRIMARY_INDEX(btree->index) && (!cursor->index_dsc);
    search_info->is_full_scan = cursor->index_ffs;
    search_info->query_scn = cursor->query_scn;
    search_info->ssn = (uint32)cursor->ssn;
}

static void pcrb_set_locator(knl_session_t *session, knl_cursor_t *cursor, btree_page_t *page,
    btree_search_t search_info, btree_path_info_t path_info)
{
    key_locator_t *locator = &cursor->key_loc;
    btree_t *btree = CURSOR_BTREE(cursor);

    locator->seg_scn = search_info.seg_scn;
    locator->slot = (uint16)path_info.path[0].slot;
    locator->page_id = GET_ROWID_PAGE(path_info.path[0]);
    locator->next_page_id = AS_PAGID(page->next);
    locator->prev_page_id = AS_PAGID(page->prev);
    locator->index_ver = (uint64)cm_atomic_get(&btree->struct_ver);
    locator->is_located = GS_TRUE;
    locator->lsn = page->head.lsn;
    locator->pcn = page->head.pcn;

    if (search_info.is_equal) {
        locator->slot_end = INVALID_SLOT;
    } else {
        pcrb_get_end_slot(session, cursor, page);
    }
}

static status_t pcrb_locate_with_find(knl_session_t *session, knl_cursor_t *cursor, knl_scan_key_t *scan_key)
{
    btree_page_t *page = NULL;
    btree_path_info_t path_info;
    btree_search_t search_info;
    bool32 is_found = GS_FALSE;
    bool32 retry;
    btree_t *btree = CURSOR_BTREE(cursor);
    index_t *index = btree->index;
    key_locator_t *locator = &cursor->key_loc;

    pcrb_init_search_info(&search_info, cursor);
    for (;;) {
        if (pcrb_locate_key(session, &search_info, cursor, scan_key, &path_info) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (cursor->eof) {
            return GS_SUCCESS;
        }

        page = pcrb_get_curr_page(session, cursor);

        if (search_info.is_equal) {
            knl_panic_log(locator->page_cache == NO_PAGE_CACHE,
                "page_cache is abnormal, panic info: page %u-%u "
                "type %u table %s index %s page_cache %u",
                cursor->rowid.file, cursor->rowid.page, page->head.type, ((table_t *)cursor->table)->desc.name,
                index->desc.name, locator->page_cache);
            if (pcrb_check_key_visible(session, cursor, cursor->query_scn, page, (uint16)path_info.path[0].slot)) {
                break;
            }

            pcrp_enter_page(session, GET_ROWID_PAGE(path_info.path[0]), cursor->query_scn, (uint32)cursor->ssn);
            page = (btree_page_t *)CURR_CR_PAGE;

            if (page != NULL) {
                buf_leave_page(session, GS_FALSE);

                bool32 cmp_rowid = search_info.is_dsc_scan ? GS_TRUE : BTREE_NEED_CMP_ROWID(cursor, index);
                pcrb_rebinary_search(index, page, scan_key, &path_info, cmp_rowid, &is_found);
                if (!is_found) {
                    pcrp_leave_page(session, GS_FALSE); /* leave CR page */
                    cursor->eof = GS_TRUE;
                    return GS_SUCCESS;
                }

                locator->page_cache = GLOBAL_PAGE_CACHE;
                break;
            }

            pcrp_alloc_page(session, GET_ROWID_PAGE(path_info.path[0]), cursor->query_scn, (uint32)cursor->ssn);
            page = (btree_page_t *)CURR_CR_PAGE;

            errno_t ret = memcpy_sp((char *)page, DEFAULT_PAGE_SIZE, CURR_PAGE, DEFAULT_PAGE_SIZE);
            knl_securec_check(ret);
            buf_leave_page(session, GS_FALSE);

            if (pcrb_construct_cr_page(session, cursor, cursor->query_scn, page) != GS_SUCCESS) {
                pcrp_leave_page(session, GS_TRUE); /* release CR page */
                tx_record_rowid(session->wrid);
                return GS_ERROR;
            }

            if (session->wxid.value != GS_INVALID_ID64) {
                pcrp_leave_page(session, GS_TRUE); /* release CR page */
                if (tx_wait(session, 0, ENQ_TX_READ_WAIT) != GS_SUCCESS) {
                    return GS_ERROR;
                }
                continue;
            }

            locator->page_cache = GLOBAL_PAGE_CACHE;
            break;
        } else {
            if (pcrb_local_cache_page(session, cursor, locator, &retry) != GS_SUCCESS) {
                return GS_ERROR;
            }

            if (retry) {
                continue;
            }

            page = pcrb_get_curr_page(session, cursor);
            break;
        }
    }

    pcrb_set_locator(session, cursor, page, search_info, path_info);
    return GS_SUCCESS;
}

static status_t pcrb_locate_next_page(knl_session_t *session, knl_cursor_t *cursor, knl_scan_key_t *scan_key)
{
    btree_page_t *page = NULL;
    key_locator_t *locator = NULL;
    btree_search_t search_info;
    page_id_t page_id;
    page_id_t next_pid;
    bool32 located = GS_FALSE;
    bool32 retry = GS_FALSE;

    locator = &cursor->key_loc;
    search_info.btree = CURSOR_BTREE(cursor);
    search_info.seg_scn = locator->seg_scn;
    search_info.is_equal = GS_FALSE;
    search_info.query_scn = cursor->query_scn;
    search_info.ssn = (uint32)cursor->ssn;
    search_info.is_full_scan = cursor->index_ffs;

    page_id = locator->page_id;
    next_pid = cursor->index_dsc ? locator->prev_page_id : locator->next_page_id;

    if (SECUREC_UNLIKELY(session->canceled)) {
        GS_THROW_ERROR(ERR_OPERATION_CANCELED);
        return GS_ERROR;
    }

    if (SECUREC_UNLIKELY(session->killed)) {
        GS_THROW_ERROR(ERR_OPERATION_KILLED);
        return GS_ERROR;
    }

    for (;;) {
        if (IS_INVALID_PAGID(next_pid)) {
            cursor->eof = GS_TRUE;
            return GS_SUCCESS;
        }

        if (cursor->isolevel == ISOLATION_CURR_COMMITTED) {
            cursor->query_scn = DB_CURR_SCN(session);
            cursor->cc_cache_time = KNL_NOW(session);
        }

        if (cursor->index_dsc) {
            if (pcrb_find_prev_page(session, &search_info, locator, page_id, next_pid, &located) != GS_SUCCESS) {
                return GS_ERROR;
            }

            if (!located) {
                pcrb_leave_curr_page(session, locator);
                locator->is_located = GS_FALSE;
                return pcrb_locate_with_find(session, cursor, scan_key);
            }
        } else {
            if (pcrb_enter_locate_page(session, &search_info, locator, next_pid, 0) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }
        page = pcrb_get_curr_page(session, cursor);
        if (page->is_recycled) {
            next_pid = cursor->index_dsc ? AS_PAGID(page->prev) : AS_PAGID(page->next);
            pcrb_leave_curr_page(session, locator);
            continue;
        }

        if (pcrb_local_cache_page(session, cursor, locator, &retry) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (retry) {
            continue;
        }

        page = pcrb_get_curr_page(session, cursor);
        break;
    }

    if (cursor->index_dsc) {
        locator->slot = page->keys - 1;
    } else {
        locator->slot = 0;
    }

    locator->page_id = AS_PAGID(page->head.id);
    locator->next_page_id = AS_PAGID(page->next);
    locator->prev_page_id = AS_PAGID(page->prev);
    locator->lsn = page->head.lsn;
    locator->pcn = page->head.pcn;

    pcrb_get_end_slot(session, cursor, page);

    return GS_SUCCESS;
}

static status_t pcrb_retry_local_cache(knl_session_t *session, knl_cursor_t *cursor, key_locator_t *locator)
{
    bool32 retry = GS_FALSE;

    for (;;) {
        if (pcrb_local_cache_page(session, cursor, locator, &retry) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (retry) {
            continue;
        }
        break;
    }

    return GS_SUCCESS;
}

static status_t pcrb_relocate_curr_page(knl_session_t *session, knl_cursor_t *cursor, knl_scan_key_t *scan_key)
{
    bool32 is_same = GS_FALSE;
    btree_path_info_t path_info;
    btree_search_t search_info;
    btree_t *btree = CURSOR_BTREE(cursor);
    bool32 cmp_rowid = IS_UNIQUE_PRIMARY_INDEX(btree->index) ? GS_FALSE : GS_TRUE;
    btree_page_t *page = NULL;
    key_locator_t *locator = NULL;
    int64 struct_ver;
    bool32 retry = GS_FALSE;

    locator = &cursor->key_loc;

    /* if split happened on this page, re-search from root */
    struct_ver = cm_atomic_get(&btree->struct_ver);
    if (cursor->index_dsc && locator->index_ver != (uint64)struct_ver) {
        locator->is_located = GS_FALSE;
        return pcrb_locate_with_find(session, cursor, scan_key);
    }

    search_info.btree = CURSOR_BTREE(cursor);
    search_info.seg_scn = locator->seg_scn;
    search_info.is_equal = GS_FALSE;
    search_info.query_scn = cursor->query_scn;
    search_info.ssn = (uint32)cursor->ssn;
    search_info.is_full_scan = cursor->index_ffs;

    for (;;) {
        if (pcrb_enter_locate_page(session, &search_info, locator, locator->page_id, 0) != GS_SUCCESS) {
            return GS_ERROR;
        }
        page = pcrb_get_curr_page(session, cursor);

        pcrb_binary_search(btree->index, page, scan_key, &path_info, cmp_rowid, &is_same);
        if (cursor->index_dsc) {
            if (path_info.path[0].slot == 0) {
                pcrb_leave_curr_page(session, locator);
                locator->is_located = GS_FALSE;
                return pcrb_locate_with_find(session, cursor, scan_key);
            }

            locator->slot = (uint16)path_info.path[0].slot - 1;
            if (pcrb_local_cache_page(session, cursor, locator, &retry) != GS_SUCCESS) {
                return GS_ERROR;
            }
            if (retry) {
                continue;
            }

            break;
        }

        if (path_info.path[0].slot < page->keys - (uint16)1) {
            /* if key is still on current page, then move on to next slot */
            locator->slot = (uint16)path_info.path[0].slot + 1;
            if (pcrb_local_cache_page(session, cursor, locator, &retry) != GS_SUCCESS) {
                return GS_ERROR;
            }
            if (retry) {
                continue;
            }
            break;
        }

        locator->page_id = AS_PAGID(page->next);
        pcrb_leave_curr_page(session, locator);

        if (IS_INVALID_PAGID(locator->page_id)) {
            cursor->eof = GS_TRUE;
            return GS_SUCCESS;
        }

        /* if scan key is last key of prev page, no need to binary search on curr page */
        if (is_same) {
            if (pcrb_enter_locate_page(session, &search_info, locator, locator->page_id, 0) != GS_SUCCESS) {
                return GS_ERROR;
            }
            page = pcrb_get_curr_page(session, cursor);
            locator->slot = 0;
            if (pcrb_retry_local_cache(session, cursor, locator) != GS_SUCCESS) {
                return GS_ERROR;
            }

            break;
        }
    }

    page = pcrb_get_curr_page(session, cursor);
    locator->lsn = page->head.lsn;
    locator->pcn = page->head.pcn;
    locator->next_page_id = AS_PAGID(page->next);
    locator->prev_page_id = AS_PAGID(page->prev);
    pcrb_get_end_slot(session, cursor, page);
    return GS_SUCCESS;
}

static inline bool32 pcrb_cached_valid(knl_session_t *session, knl_cursor_t *cursor)
{
    date_t timeout;

    if (cursor->isolevel != (uint8)ISOLATION_CURR_COMMITTED) {
        return GS_TRUE;
    }

    timeout = (date_t)session->kernel->undo_ctx.retention * MICROSECS_PER_SECOND / RETENTION_TIME_PERCENT;

    return (bool32)((KNL_NOW(session) - cursor->cc_cache_time) < timeout);
}


static inline status_t pcrb_locate_curr_page(knl_session_t *session, knl_cursor_t *cursor, knl_scan_key_t *scan_key)
{
    if (pcrb_cached_valid(session, cursor)) {
        if (cursor->index_dsc) {
            cursor->key_loc.slot--;
        } else {
            cursor->key_loc.slot++;
        }
        return GS_SUCCESS;
    }

    if (cursor->isolevel == (uint8)ISOLATION_CURR_COMMITTED) {
        cursor->query_scn = DB_CURR_SCN(session);
        cursor->cc_cache_time = KNL_NOW(session);
    }

    if (pcrb_relocate_curr_page(session, cursor, scan_key) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static inline status_t pcrb_find_key(knl_session_t *session, knl_cursor_t *cursor, knl_scan_key_t *scan_key)
{
    if (!cursor->key_loc.is_located) {
        return pcrb_locate_with_find(session, cursor, scan_key);
    } else if (!cursor->key_loc.is_last_key) {
        /* only used in range scan */
        return pcrb_locate_curr_page(session, cursor, scan_key);
    } else {
        /* only used in range scan */
        return pcrb_locate_next_page(session, cursor, scan_key);
    }
}

static void pcrb_check_part_id(knl_cursor_t *cursor, btree_t *btree, pcrb_key_t *key, bool32 *is_found)
{
    if (IS_PART_TABLE(cursor->table) && !IS_PART_INDEX(btree->index)) {
        table_t *table = (table_t *)cursor->table;
        part_table_t *part_table = table->part_table;
        if (cursor->restrict_part) {
            table_part_t *part = TABLE_GET_PART(table, cursor->part_loc.part_no);
            knl_panic_log(part != NULL, "table part is NULL, panic info: page %u-%u type %u table %s index %s",
                cursor->rowid.file, cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type, table->desc.name,
                ((index_t *)btree->index)->desc.name);

            if (part->desc.part_id != pcrb_get_part_id(key)) {
                *is_found = GS_FALSE;
            }

            return;
        }

        if (cursor->restrict_subpart) {
            knl_panic_log(IS_COMPART_TABLE(part_table),
                "the part_table is not compart table, panic info: "
                "page %u-%u type %u table %s index %s",
                cursor->rowid.file, cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type, table->desc.name,
                ((index_t *)btree->index)->desc.name);
            table_part_t *compart = TABLE_GET_PART(table, cursor->part_loc.part_no);
            knl_panic_log(compart != NULL, "the compart is NULL, panic info: page %u-%u type %u table %s index %s",
                cursor->rowid.file, cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type, table->desc.name,
                ((index_t *)btree->index)->desc.name);
            knl_panic_log(IS_PARENT_TABPART(&compart->desc),
                "the compart is not parent tabpart, panic info: "
                "page %u-%u type %u table %s table_part %s index %s",
                cursor->rowid.file, cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type, table->desc.name,
                compart->desc.name, ((index_t *)btree->index)->desc.name);

            table_part_t *subpart = PART_GET_SUBENTITY(part_table, compart->subparts[cursor->part_loc.subpart_no]);
            knl_panic_log(subpart != NULL, "the subpart is NULL, panic info: page %u-%u type %u table %s index %s",
                cursor->rowid.file, cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type, table->desc.name,
                ((index_t *)btree->index)->desc.name);
            if (subpart->desc.parent_partid != pcrb_get_part_id(key) ||
                subpart->desc.part_id != pcrb_get_subpart_id(key)) {
                *is_found = GS_FALSE;
            }
        }
    }
}

void pcrb_convert_row(knl_session_t *session, knl_index_desc_t *desc, char *key_buf, row_head_t *row, uint16 *bitmap)
{
    pcrb_key_t *key = NULL;
    uint32 copy_size;
    errno_t ret;

    key = (pcrb_key_t *)key_buf;
    row->size = sizeof(row_head_t) + (uint16)key->size - sizeof(pcrb_key_t);
    row->column_count = desc->column_count;

    copy_size = (uint32)key->size - (uint32)sizeof(pcrb_key_t);
    if (copy_size != 0) {
        ret = memcpy_sp((char *)row + sizeof(row_head_t), DEFAULT_PAGE_SIZE - sizeof(row_head_t),
            (char *)key + sizeof(pcrb_key_t), copy_size);
        knl_securec_check(ret);
    }

    *bitmap = key->bitmap;
}

static void pcrb_get_key(knl_session_t *session, knl_cursor_t *cursor, btree_page_t *page, pcrb_key_t *key)
{
    knl_scan_range_t *scan_range;
    pcr_itl_t *itl = NULL;
    txn_info_t txn_info;
    errno_t ret;

    scan_range = &cursor->scan_range;

    BTREE_COPY_ROWID(key, cursor);

    if (IS_INDEX_ONLY_SCAN(cursor)) {
        if (cursor->key_loc.page_cache != NO_PAGE_CACHE || key->itl_id == GS_INVALID_ID8) {
            /* use the current query_scn as key scn */
            cursor->scn = cursor->query_scn;
        } else {
            itl = pcrb_get_itl(page, key->itl_id);
            tx_get_pcr_itl_info(session, GS_TRUE, itl, &txn_info);
            cursor->scn = txn_info.scn;
        }

        pcrb_convert_row(session, &((index_t *)cursor->index)->desc, (char *)key, cursor->row, &cursor->bitmap);
    }

    if (SECUREC_LIKELY(!cursor->key_loc.is_last_key && cursor->isolevel != (uint8)ISOLATION_CURR_COMMITTED)) {
        return;
    }

    if (!cursor->index_dsc) {
        ret = memcpy_sp(scan_range->l_buf, GS_KEY_BUF_SIZE, key, (size_t)key->size);
        knl_securec_check(ret);

        pcrb_decode_key((index_t *)cursor->index, (pcrb_key_t *)scan_range->l_buf, &scan_range->l_key);
    } else {
        ret = memcpy_sp(scan_range->r_buf, GS_KEY_BUF_SIZE, key, (size_t)key->size);
        knl_securec_check(ret);

        pcrb_decode_key((index_t *)cursor->index, (pcrb_key_t *)scan_range->r_buf, &scan_range->r_key);
    }
}

static bool32 pcrb_do_match_cond(knl_cursor_t *cursor, pcrb_key_t *key)
{
    index_t *index = (index_t *)cursor->index;
    key_locator_t *locator = &cursor->key_loc;
    knl_column_t *column = NULL;
    knl_scan_key_t curr_key;
    knl_scan_key_t *l_key = cursor->index_dsc ? &cursor->scan_range.l_key : &cursor->scan_range.org_key;
    knl_scan_key_t *r_key = cursor->index_dsc ? &cursor->scan_range.org_key : &cursor->scan_range.r_key;
    uint8 i;
    uint16 offset;

    curr_key.buf = (char *)key;
    pcrb_decode_key(index, key, &curr_key);
    offset = curr_key.offsets[locator->equal_cols];
    if (locator->match_left) {
        offset = curr_key.offsets[locator->equal_cols];
        for (i = locator->equal_cols; i < index->desc.column_count; i++) {
            column = dc_get_column(index->entity, index->desc.columns[i]);
            offset = curr_key.offsets[i];
            if (pcrb_cmp_column(column, l_key, i, key, &offset) > 0) {
                return GS_FALSE;
            }
        }
    }

    if (locator->match_right) {
        offset = curr_key.offsets[locator->equal_cols];
        for (uint32 i = locator->equal_cols; i < index->desc.column_count; i++) {
            column = dc_get_column(index->entity, index->desc.columns[i]);
            offset = curr_key.offsets[i];
            if (pcrb_cmp_column(column, r_key, i, key, &offset) < 0) {
                return GS_FALSE;
            }
        }
    }

    return GS_TRUE;
}

static inline bool32 pcrb_match_cond(knl_cursor_t *cursor, btree_page_t *page)
{
    if (!cursor->key_loc.match_left && !cursor->key_loc.match_right) {
        return GS_TRUE;
    }

    pcrb_dir_t *dir = pcrb_get_dir(page, cursor->key_loc.slot);
    pcrb_key_t *key = PCRB_GET_KEY(page, dir);

    return pcrb_do_match_cond(cursor, key);
}

static status_t pcrb_fetch_key_dsc(knl_session_t *session, knl_cursor_t *cursor, btree_t *btree, key_locator_t *locator,
    bool32 *is_found)
{
    btree_page_t *page;
    pcrb_dir_t *dir = NULL;
    pcrb_key_t *key = NULL;

    page = pcrb_get_curr_page(session, cursor);
    *is_found = GS_FALSE;

    for (;;) {
        dir = pcrb_get_dir(page, locator->slot);
        key = PCRB_GET_KEY(page, dir);

        if (locator->slot_end == INVALID_SLOT || locator->slot_end > locator->slot) {
            if (!pcrb_check_scan_range(cursor, btree->index, &cursor->scan_range.l_key, locator, key)) {
                locator->is_last_key = GS_TRUE;
                cursor->eof = GS_TRUE;
                return GS_SUCCESS;
            }
        }

        *is_found = (bool32)!key->is_deleted;
        if (*is_found && (cursor->restrict_part || cursor->restrict_subpart)) {
            pcrb_check_part_id(cursor, btree, key, is_found);
        }

        if (*is_found) {
            *is_found = pcrb_match_cond(cursor, page);
        }

        if (*is_found) {
            locator->is_last_key = (locator->slot == 0);
            pcrb_get_key(session, cursor, page, key);
            return GS_SUCCESS;
        }

        if (locator->slot > 0) {
            locator->slot--;
        } else {
            break;
        }
    }

    locator->is_last_key = GS_TRUE;
    return GS_SUCCESS;
}

static status_t pcrb_fetch_key_asc(knl_session_t *session, knl_cursor_t *cursor, btree_t *btree, key_locator_t *locator,
    bool32 *is_found)
{
    pcrb_dir_t *dir = NULL;
    pcrb_key_t *key = NULL;
    btree_page_t *page = pcrb_get_curr_page(session, cursor);
    index_t *index = (index_t *)cursor->index;
    knl_scan_range_t *scan_range = &cursor->scan_range;
    bool32 is_equal = scan_range->is_equal && IS_UNIQUE_PRIMARY_INDEX(index);
    knl_scan_key_t *filter_key = scan_range->is_equal ? &scan_range->l_key : &scan_range->r_key;

    *is_found = GS_FALSE;

    while (locator->slot < page->keys) {
        dir = pcrb_get_dir(page, locator->slot);
        key = PCRB_GET_KEY(page, dir);

        if (!is_equal && (locator->slot_end == INVALID_SLOT || locator->slot_end < locator->slot)) {
            /* for point scan, do not need to compare rowid */
            if (!pcrb_check_scan_range(cursor, btree->index, filter_key, locator, key)) {
                locator->is_last_key = GS_TRUE;
                cursor->eof = GS_TRUE;
                return GS_SUCCESS;
            }
        }

        *is_found = (bool32)!key->is_deleted;
        if (*is_found && (cursor->restrict_part || cursor->restrict_subpart)) {
            pcrb_check_part_id(cursor, btree, key, is_found);
        }

        if (*is_found) {
            *is_found = pcrb_match_cond(cursor, page);
        }

        if (*is_found) {
            locator->is_last_key = (locator->slot == page->keys - 1);
            pcrb_get_key(session, cursor, page, key);
            return GS_SUCCESS;
        }

        if (is_equal) {
            cursor->eof = GS_TRUE;
            return GS_SUCCESS;
        }

        locator->slot++;
    }

    locator->is_last_key = GS_TRUE;
    return GS_SUCCESS;
}

static status_t pcrb_locate_next_scan_key(knl_session_t *session, knl_cursor_t *cursor)
{
    knl_scan_key_t *scan_key = cursor->index_dsc ? &cursor->scan_range.r_key : &cursor->scan_range.l_key;
    knl_scan_key_t next_scan_key;
    btree_t *btree = CURSOR_BTREE(cursor);
    index_t *index = btree->index;
    uint16 len;
    uint16 offset = sizeof(pcrb_key_t);
    btree_page_t *page = pcrb_get_curr_page(session, cursor);
    pcrb_dir_t *dir = pcrb_get_dir(page, (uint32)cursor->key_loc.slot);
    pcrb_key_t *key = PCRB_GET_KEY(page, dir);
    knl_column_t *column = dc_get_column(index->entity, index->desc.columns[0]);

    if (pcrb_cmp_column(column, scan_key, 0, key, &offset) != 0) {
        cursor->eof = GS_FALSE;
        return GS_SUCCESS;
    }

    next_scan_key.buf = (char *)cm_push(session->stack, GS_KEY_BUF_SIZE);
    next_scan_key.flags[0] = scan_key->flags[0];
    pcrb_init_key((pcrb_key_t *)next_scan_key.buf, NULL);

    if (scan_key->flags[0] == SCAN_KEY_NORMAL) {
        char *data = btree_get_column(scan_key, column->datatype, 0, &len, GS_TRUE);
        knl_set_scan_key(&index->desc, &next_scan_key, column->datatype, (const char *)data, len, 0);
    } else {
        knl_set_key_flag(&next_scan_key, scan_key->flags[0], 0);
    }

    uint8 flag = cursor->index_dsc ? SCAN_KEY_LEFT_INFINITE : SCAN_KEY_RIGHT_INFINITE;

    for (uint32 i = 1; i < index->desc.column_count; i++) {
        knl_set_key_flag(&next_scan_key, flag, i);
    }

    btree_search_t search_info;
    btree_path_info_t path_info;

    search_info.btree = btree;
    search_info.seg_scn = cursor->key_loc.seg_scn;
    search_info.tree_info.value = cm_atomic_get(&btree->segment->tree_info.value);
    search_info.is_dsc_scan = cursor->index_dsc;
    search_info.is_equal = GS_FALSE;
    search_info.is_full_scan = GS_FALSE;
    search_info.query_scn = cursor->query_scn;
    search_info.ssn = (uint32)cursor->ssn;
    cursor->eof = GS_FALSE;

    if (pcrb_locate_key(session, &search_info, cursor, &next_scan_key, &path_info) != GS_SUCCESS) {
        cm_pop(session->stack);
        return GS_ERROR;
    }

    if (cursor->eof) {
        cm_pop(session->stack);
        return GS_SUCCESS;
    }

    page = pcrb_get_curr_page(session, cursor);
    errno_t ret = memcpy_sp(cursor->page_buf, DEFAULT_PAGE_SIZE, page, PAGE_SIZE(page->head));
    knl_securec_check(ret);
    pcrb_leave_curr_page(session, &cursor->key_loc);

    cursor->key_loc.page_cache = LOCAL_PAGE_CACHE;
    cursor->key_loc.slot = (uint16)path_info.path[0].slot;
    cm_pop(session->stack);
    return GS_SUCCESS;
}

static void pcrb_set_next_scan_key(knl_session_t *session, knl_cursor_t *cursor, index_t *index, pcrb_key_t *key,
    knl_scan_key_t *scan_key)
{
    knl_scan_key_t next_scan_key;
    knl_column_t *column = NULL;
    char *data = NULL;
    uint16 len;

    next_scan_key.buf = (char *)cm_push(session->stack, GS_KEY_BUF_SIZE);
    pcrb_decode_key(index, key, &next_scan_key);
    errno_t ret = memset_sp(scan_key->buf, sizeof(pcrb_key_t), 0, sizeof(pcrb_key_t));
    knl_securec_check(ret);
    if (SECUREC_UNLIKELY(cursor->index_dsc)) {
        rowid_t max_rid;
        MAXIMIZE_ROWID(max_rid);
        pcrb_init_key((pcrb_key_t *)scan_key->buf, &max_rid);
    } else {
        pcrb_init_key((pcrb_key_t *)scan_key->buf, NULL);
    }

    if (!btree_get_bitmap(&key->bitmap, 0)) {
        knl_set_key_flag(scan_key, SCAN_KEY_IS_NULL, 0);
    } else {
        column = dc_get_column(index->entity, index->desc.columns[0]);
        data = btree_get_column(&next_scan_key, column->datatype, 0, &len, GS_TRUE);
        knl_set_scan_key(&index->desc, scan_key, column->datatype, data, len, 0);
    }

    for (uint32 i = 1; i < index->desc.column_count; i++) {
        scan_key->flags[i] = cursor->scan_range.org_key.flags[i];
        if (scan_key->flags[i] == SCAN_KEY_NORMAL) {
            column = dc_get_column(index->entity, index->desc.columns[i]);
            data = btree_get_column(&cursor->scan_range.org_key, column->datatype, i, &len, GS_TRUE);
            knl_set_scan_key(&index->desc, scan_key, column->datatype, data, len, i);
        }
    }
    cm_pop(session->stack);
}

static status_t pcrb_set_next_range(knl_session_t *session, knl_cursor_t *cursor, btree_t *btree)
{
    knl_scan_key_t *scan_key = cursor->index_dsc ? &cursor->scan_range.r_key : &cursor->scan_range.l_key;
    knl_scan_key_t *end_key = cursor->index_dsc ? &cursor->scan_range.l_key : &cursor->scan_range.r_key;
    index_t *index = btree->index;

    if (pcrb_locate_next_scan_key(session, cursor) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (cursor->eof) {
        return GS_SUCCESS;
    }

    btree_page_t *page = pcrb_get_curr_page(session, cursor);
    pcrb_dir_t *dir = pcrb_get_dir(page, (uint32)cursor->key_loc.slot);
    pcrb_key_t *key = PCRB_GET_KEY(page, dir);

    int32 result = pcrb_compare_key(index, end_key, key, GS_TRUE, NULL);
    cursor->eof = cursor->index_dsc ? (result > 0) : (result < 0);

    if (cursor->eof) {
        pcrb_leave_curr_page(session, &cursor->key_loc);
        return GS_SUCCESS;
    }

    pcrb_set_next_scan_key(session, cursor, index, key, scan_key);
    cursor->key_loc.is_located = GS_FALSE;
    cursor->key_loc.is_last_key = GS_FALSE;

    return GS_SUCCESS;
}

static inline status_t pcrb_check_eof(knl_session_t *session, knl_cursor_t *cursor)
{
    if (!cursor->eof || !cursor->index_ss) {
        return GS_SUCCESS;
    }

    return pcrb_set_next_range(session, cursor, CURSOR_BTREE(cursor));
}

static inline void pcrb_save_org_key(knl_cursor_t *cursor)
{
    cursor->scan_range.org_key = cursor->index_dsc ? cursor->scan_range.r_key : cursor->scan_range.l_key;
    pcrb_key_t *key = (pcrb_key_t *)(cursor->index_dsc ? cursor->scan_range.r_buf : cursor->scan_range.l_buf);

    cursor->scan_range.org_key.buf = cursor->scan_range.org_buf;
    errno_t ret = memcpy_sp(cursor->scan_range.org_buf, GS_KEY_BUF_SIZE, key, (size_t)key->size);
    knl_securec_check(ret);
}

static void pcrb_init_fetch(knl_session_t *session, knl_cursor_t *cursor)
{
    /*
     * for PAGE LEVEL SNAPSHOT, index fast full scan use the same scan mode with index full scan
     * later, we will implement specified scan mode for index ffs, which scan leaf pages by extent.
     */
    cursor->index_ffs = cursor->index_ffs ? GS_TRUE : btree_is_full_scan(&cursor->scan_range);
    btree_init_key_loc(&cursor->key_loc);

    if (SECUREC_UNLIKELY(cursor->index_ss)) {
        if (cursor->scan_range.is_equal) {
            cursor->index_ss = GS_FALSE;
            btree_set_cmp_endpoint(cursor);
            return;
        }

        knl_panic_log(((index_t *)cursor->index)->desc.column_count > BTREE_MIN_SKIP_COLUMNS,
            "index's column_count "
            "is invalid, panic info: page %u-%u type %u table %s index %s column_count %u",
            cursor->rowid.file, cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type,
            ((table_t *)cursor->table)->desc.name, ((index_t *)cursor->index)->desc.name,
            ((index_t *)cursor->index)->desc.column_count);
        pcrb_save_org_key(cursor);
        return;
    }

    btree_init_match_cond(cursor, GS_TRUE);
    if (cursor->index_dsc) {
        if (cursor->key_loc.match_right) {
            pcrb_save_org_key(cursor);
        }
    } else {
        if (cursor->key_loc.match_left) {
            pcrb_save_org_key(cursor);
        }
    }

    btree_set_cmp_endpoint(cursor);
}
/*
 * for PCR btree fetch, we cache the current leaf page except point scan
 */
status_t pcrb_fetch(knl_handle_t handle, knl_cursor_t *cursor)
{
    knl_session_t *session = (knl_session_t *)handle;
    btree_t *btree = NULL;
    status_t status;
    knl_scan_key_t *scan_key = NULL;
    key_locator_t *locator = NULL;
    seg_stat_t temp_stat;

#ifdef _STATISTICS
    SEG_STATS_INIT(session, &temp_stat);
#endif

    btree = CURSOR_BTREE(cursor);
    if (SECUREC_UNLIKELY(btree->segment == NULL)) {
        cursor->eof = GS_TRUE;
        return GS_SUCCESS;
    }

    locator = &cursor->key_loc;
    bool32 equal_fetch = cursor->scan_range.is_equal && IS_UNIQUE_PRIMARY_INDEX(btree->index);
    scan_key = cursor->index_dsc ? &cursor->scan_range.r_key : &cursor->scan_range.l_key;

    if (!locator->is_initialized) {
        pcrb_init_fetch(session, cursor);
    }

    for (;;) {
        /* for equal fetch, skip fetch again, if we have located key */
        if (equal_fetch && locator->is_located) {
            cursor->eof = GS_TRUE;
            return GS_SUCCESS;
        }

        if (pcrb_find_key(session, cursor, scan_key) != GS_SUCCESS) {
#ifdef _STATISTICS
            SEG_STATS_RECORD(session, temp_stat, &btree->stat);
#endif
            return GS_ERROR;
        }

        if (cursor->eof) {
#ifdef _STATISTICS
            SEG_STATS_RECORD(session, temp_stat, &btree->stat);
#endif
            return GS_SUCCESS;
        }

        if (cursor->index_dsc) {
            status = pcrb_fetch_key_dsc(session, cursor, btree, locator, &cursor->is_found);
        } else {
            status = pcrb_fetch_key_asc(session, cursor, btree, locator, &cursor->is_found);
        }

        pcrb_leave_curr_page(session, locator);

        if (status != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (cursor->is_found) {
            if (knl_cursor_ssi_conflict(cursor, GS_FALSE) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }

        /* only cleanout btree page during range scan */
        if (cursor->cleanout && !equal_fetch && locator->is_last_key) {
            pcrb_cleanout_page(session, cursor, locator->page_id);
        }

        if (pcrb_check_eof(session, cursor) != GS_SUCCESS) {
#ifdef _STATISTICS
            SEG_STATS_RECORD(session, temp_stat, &btree->stat);
#endif
            return GS_ERROR;
        }
        if (cursor->eof) {
#ifdef _STATISTICS
            SEG_STATS_RECORD(session, temp_stat, &btree->stat);
#endif
            return GS_SUCCESS;
        }

        if (!cursor->is_found) {
            continue;
        }

        if (IS_INDEX_ONLY_SCAN(cursor)) {
            if (cursor->index_prefetch_row) {
                if (buf_read_page_asynch(session, GET_ROWID_PAGE(cursor->rowid)) != GS_SUCCESS) {
                    GS_LOG_DEBUG_WAR("failed to prefetch page file : %u , page: %llu", (uint32)cursor->rowid.file,
                        (uint64)cursor->rowid.page);
                }
            }

            if (knl_match_cond(session, cursor, &cursor->is_found) != GS_SUCCESS) {
                return GS_ERROR;
            }
        } else {
            if (TABLE_ACCESSOR(cursor)->do_fetch_by_rowid(session, cursor) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }

        if (cursor->is_found) {
#ifdef _STATISTICS
            SEG_STATS_RECORD(session, temp_stat, &btree->stat);
#endif
            return GS_SUCCESS;
        }
    }
}

static void pcrb_get_sub_r_key(knl_session_t *session, index_t *index, pcrb_key_t *key, knl_scan_range_t *range,
    bool8 is_dsc)
{
    page_id_t page_id;
    btree_page_t *page = NULL;
    pcrb_dir_t *dir = NULL;
    pcrb_key_t *r_key = NULL;
    errno_t err;

    r_key = (pcrb_key_t *)range->r_buf;

    if (index->desc.primary || index->desc.unique) {
        for (;;) {
            page_id = pcrb_get_child(key);
            buf_enter_page(session, page_id, LATCH_MODE_S, ENTER_PAGE_NORMAL);
            page = (btree_page_t *)session->curr_page;
            dir = pcrb_get_dir(page, 0);
            key = PCRB_GET_KEY(page, dir);

            if (page->level != 0) {
                buf_leave_page(session, GS_FALSE);
                continue;
            }

            err = memcpy_sp(r_key, GS_KEY_BUF_SIZE, key, (size_t)key->size);
            knl_securec_check(err);
            buf_leave_page(session, GS_FALSE);
            break;
        }
    } else {
        err = memcpy_sp(r_key, GS_KEY_BUF_SIZE, key, (size_t)key->size);
        knl_securec_check(err);
    }

    // index desc scan no need to decrease rowid
    if (!is_dsc) {
        if (r_key->rowid.slot != 0) {
            r_key->rowid.slot--;
        } else {
            knl_panic_log(r_key->rowid.page > 0,
                "rowid's page in r_key is invalid, panic info: index %s "
                "r_key rowid page %u",
                index->desc.name, r_key->rowid.page);
            r_key->rowid.slot = INVALID_SLOT;
            r_key->rowid.page--;
        }
    }
    pcrb_decode_key(index, r_key, &range->r_key);
}

void pcrb_get_parl_schedule(knl_session_t *session, index_t *index, knl_idx_paral_info_t paral_info,
    idx_range_info_t org_info, uint32 root_level, knl_index_paral_range_t *sub_range)
{
    uint32 i = 0;
    uint32 r_border;
    knl_scan_range_t *org_range = paral_info.org_range;
    knl_scan_range_t *range = sub_range->index_range[0];
    page_id_t page_id;
    errno_t ret;

    uint32 step = org_info.keys / sub_range->workers;

    // set first left range
    buf_enter_page(session, org_info.l_page[org_info.level], LATCH_MODE_S, ENTER_PAGE_NORMAL);
    btree_page_t *page = BTREE_CURR_PAGE;
    uint32 slot = org_info.l_slot[org_info.level];
    pcrb_dir_t *dir = pcrb_get_dir(page, slot);
    pcrb_key_t *key = PCRB_GET_KEY(page, dir);

    if (key->is_infinite) {
        ret = memcpy_sp(range->l_buf, GS_KEY_BUF_SIZE, org_range->l_buf, GS_KEY_BUF_SIZE);
        knl_securec_check(ret);
        range->l_key = org_range->l_key;
        range->l_key.buf = range->l_buf;
    } else {
        ret = memcpy_sp(range->l_buf, GS_KEY_BUF_SIZE, key, (size_t)key->size);
        knl_securec_check(ret);
        pcrb_decode_key(index, key, &range->l_key);
    }

    do {
        if (org_info.level == root_level) {
            r_border = slot + step;
        } else {
            page_id = AS_PAGID(page->head.id);
            buf_leave_page(session, GS_FALSE);

            idx_enter_next_range(session, page_id, slot, step, &r_border);
            page = BTREE_CURR_PAGE;
        }

        slot = r_border;
        dir = pcrb_get_dir(page, r_border);
        key = PCRB_GET_KEY(page, dir);
        pcrb_get_sub_r_key(session, index, key, range, paral_info.is_dsc);

        i++; // set next range
        range = sub_range->index_range[i];

        ret = memcpy_sp(range->l_buf, GS_KEY_BUF_SIZE, key, (size_t)key->size);
        knl_securec_check(ret);
        pcrb_decode_key(index, key, &range->l_key);
    } while (i < sub_range->workers - 1);

    // set last right range
    buf_leave_page(session, GS_FALSE);
    ret = memcpy_sp(range->r_buf, GS_KEY_BUF_SIZE, org_range->r_buf, GS_KEY_BUF_SIZE);
    knl_securec_check(ret);
    range->r_key = org_range->r_key;
    range->r_key.buf = range->r_buf;
}

status_t pcrb_check_key_exist(knl_session_t *session, btree_t *btree, char *data, bool32 *exists)
{
    knl_scan_key_t scan_key;
    btree_search_t search_info;
    knl_tree_info_t tree_info;
    btree_path_info_t path_info;
    key_locator_t locator;
    bool32 found = GS_FALSE;
    txn_info_t txn_info;
    pcrb_key_t *key = NULL;
    btree_page_t *page = NULL;
    pcrb_dir_t *dir = NULL;
    pcr_itl_t *itl = NULL;

    *exists = GS_FALSE;
    if (btree->segment == NULL) {
        return GS_SUCCESS;
    }

    key = (pcrb_key_t *)data;
    pcrb_decode_key(btree->index, key, &scan_key);
    search_info.btree = btree;
    search_info.is_dsc_scan = GS_FALSE;
    search_info.is_equal = GS_TRUE;
    search_info.seg_scn = BTREE_SEGMENT(btree->entry, btree->segment)->seg_scn;
    tree_info.value = cm_atomic_get(&BTREE_SEGMENT(btree->entry, btree->segment)->tree_info.value);
    search_info.tree_info = tree_info;

    if (!spc_validate_page_id(session, AS_PAGID(&tree_info.root))) {
        GS_THROW_ERROR(ERR_INDEX_ALREADY_DROPPED, btree->index->desc.name);
        return GS_ERROR;
    }

    for (;;) {
        if (pcrb_find_leaf(session, &search_info, &locator, &scan_key, &path_info, &found) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (!found) {
            *exists = GS_FALSE;
            return GS_SUCCESS;
        }

        page = BTREE_CURR_PAGE;
        dir = pcrb_get_dir(page, (uint32)path_info.path[0].slot);
        key = PCRB_GET_KEY(page, dir);
        if (key->itl_id == GS_INVALID_ID8) {
            break;
        }

        itl = pcrb_get_itl(page, key->itl_id);
        if (itl->xid.value == session->rm->xid.value) {
            break;
        }

        tx_get_pcr_itl_info(session, GS_FALSE, itl, &txn_info);
        if (txn_info.status == (uint8)XACT_END) {
            break;
        }
        session->wxid = itl->xid;
        ROWID_COPY(session->wrid, key->rowid);
        buf_leave_page(session, GS_FALSE);

        btree->stat.row_lock_waits++;
        if (tx_wait(session, session->lock_wait_timeout, ENQ_TX_KEY) != GS_SUCCESS) {
            tx_record_rowid(session->wrid);
            return GS_ERROR;
        }
    }

    *exists = (!key->is_deleted);
    buf_leave_page(session, GS_FALSE);

    return GS_SUCCESS;
}

static status_t pcrb_fetch_depended_asc(knl_session_t *session, knl_cursor_t *cursor)
{
    btree_page_t *page;
    key_locator_t *locator;
    pcrb_key_t *key = NULL;
    pcrb_dir_t *dir = NULL;
    btree_t *btree;
    knl_scan_key_t *filter_key;
    int32 result;
    errno_t ret;

    btree = CURSOR_BTREE(cursor);
    locator = &cursor->key_loc;
    filter_key = &cursor->scan_range.r_key;
    page = pcrb_get_curr_page(session, cursor);

    while (locator->slot < page->keys) {
        dir = pcrb_get_dir(page, locator->slot);
        key = PCRB_GET_KEY(page, dir);

        if (locator->slot_end == INVALID_SLOT || locator->slot_end < locator->slot) {
            result = pcrb_compare_key(btree->index, filter_key, key, GS_FALSE, NULL);
            if (result < 0) {
                pcrb_leave_curr_page(session, locator);
                cursor->eof = GS_TRUE;
                return GS_SUCCESS;
            }
        }

        cursor->is_found = (bool32)!key->is_deleted;

        if (cursor->is_found) {
            ret = memcpy_sp(cursor->scan_range.l_buf, GS_KEY_BUF_SIZE, key, (size_t)key->size);
            knl_securec_check(ret);
            key = (pcrb_key_t *)cursor->scan_range.l_buf;
            locator->is_last_key = (locator->slot == page->keys - 1);
            pcrb_decode_key(btree->index, key, &cursor->scan_range.l_key);
            pcrb_leave_curr_page(session, locator);

            BTREE_COPY_ROWID(key, cursor);

            if (TABLE_ACCESSOR(cursor)->do_fetch_by_rowid(session, cursor) != GS_SUCCESS) {
                return GS_ERROR;
            }

            if (cursor->is_found) {
                return GS_SUCCESS;
            }
        }
        locator->slot++;
    }

    locator->is_last_key = GS_TRUE;
    pcrb_leave_curr_page(session, locator);
    return GS_SUCCESS;
}

status_t pcrb_fetch_depended(knl_session_t *session, knl_cursor_t *cursor)
{
    if (cursor->eof) {
        return GS_SUCCESS;
    }

    btree_t *btree = CURSOR_BTREE(cursor);
    if (btree->segment == NULL) {
        cursor->eof = GS_TRUE;
        return GS_SUCCESS;
    }

    knl_scan_key_t *scan_key = &cursor->scan_range.l_key;
    cursor->index_dsc = GS_FALSE;

    if (!cursor->key_loc.is_initialized) {
        pcrb_init_fetch(session, cursor);
    }

    for (;;) {
        if (pcrb_find_key(session, cursor, scan_key) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (cursor->eof) {
            return GS_SUCCESS;
        }

        if (pcrb_fetch_depended_asc(session, cursor) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (cursor->is_found || cursor->eof) {
            return GS_SUCCESS;
        }
    }
}
