/*
* Copyright (c) GBA-NCTI-ISDC. 2022-2024.
*
* openGauss embedded is licensed under Mulan PSL v2.
* You can use this software according to the terms and conditions of the Mulan PSL v2.
* You may obtain a copy of Mulan PSL v2 at:
*
* http://license.coscl.org.cn/MulanPSL2
*
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FITFOR A PARTICULAR PURPOSE.
* See the Mulan PSL v2 for more details.
* -------------------------------------------------------------------------
*
* alter_test.cpp
*
* IDENTIFICATION
* src/interface/sqlite3_api_wrapper/sqlite3_api_wrapper.c 
*
* -------------------------------------------------------------------------
*/
#include "include/sqlite3.h"
#include "../../src/interface/c/intarkdb_sql.h"
#include "../../src/storage/gstor/zekernel/common/cm_defs.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <math.h>

#define DATA_BASE_LIST_SQL "PRAGMA database_list;"
#define PRAGMA_SQL_STR "PRAGMA "


struct sqlite3 {
	intarkdb_database db;
	intarkdb_connection conn;
	int errCode;
	int64_t last_changes;
	int64_t total_changes;
};

static char *sqlite3_strdup(const char *str);

struct sqlite3_stmt {
	//! The DB object that this statement belongs to
	sqlite3 *db;
	intarkdb_prepared_statement stmt;
	intarkdb_result result;
	int64_t current_row;
	int64_t row_count;
	
};

/*
** Each SQL function is defined by an instance of the following
** structure.  For global built-in functions (ex: substr(), max(), count())
** a pointer to this structure is held in the sqlite3BuiltinFunctions object.
** For per-connection application-defined functions, a pointer to this
** structure is held in the db->aHash hash table.
**
** The u.pHash field is used by the global built-ins.  The u.pDestructor
** field is used by per-connection app-def functions.
*/
struct FuncDef {
  int8_t nArg;             /* Number of arguments.  -1 means unlimited */
  uint32_t funcFlags;       /* Some combination of SQLITE_FUNC_* */
  void *pUserData;     /* User data parameter */
};


/*
** This structure encapsulates a user-function destructor callback (as
** configured using create_function_v2()) and a reference counter. When
** create_function_v2() is called to create a function with a destructor,
** a single object of this type is allocated. FuncDestructor.nRef is set to
** the number of FuncDef objects created (either 1 or 3, depending on whether
** or not the specified encoding is SQLITE_ANY). The FuncDef.pDestructor
** member of each of the new FuncDef objects is set to point to the allocated
** FuncDestructor.
**
** Thereafter, when one of the FuncDef objects is deleted, the reference
** count on this object is decremented. When it reaches 0, the destructor
** is invoked and the FuncDestructor structure freed.
*/
struct FuncDestructor {
  int nRef;
  void (*xDestroy)(void *);
  void *pUserData;
};


struct sqlite3_value {
	union MemValue {
		double r;  /* Real value used when MEM_Real is set in flags */
		int64_t i; /* Integer value used when MEM_Int is set in flags */
		           // int nZero;          /* Extra zero bytes when MEM_Zero and MEM_Blob set */
	} u;
	int type;

	char* str;
	sqlite3 *db; /* The associated database connection */
};


/*
** The "context" argument for an installable function.  A pointer to an
** instance of this structure is the first argument to the routines used
** implement the SQL functions.
**
** There is a typedef for this structure in sqlite.h.  So all routines,
** even the public interface to SQLite, can use a pointer to this structure.
** But this file is the only place where the internal details of this
** structure are known.
**
** This structure is defined inside of vdbeInt.h because it uses substructures
** (Mem) which are only defined there.
*/
struct sqlite3_context {
	sqlite3_value result; // Mem *pOut;              /* The return value is stored here */
	FuncDef pFunc;        /* Pointer to function information */
	                      //   Mem *pMem;              /* Memory cell used to store aggregate context */
	                      //   Vdbe *pVdbe;            /* The VM that owns this context */
	                      //   int iOp;                /* Instruction number of OP_Function */
	int isError;          /* Error code returned by the function. */
	                      //   u8 skipFlag;            /* Skip accumulator loading if true */
	                      //   u8 argc;                /* Number of arguments */
	                      //   sqlite3_value *argv[1]; /* Argument set */
};

int sqlite3_open(const char *filename, /* Database filename (UTF-8) */
                 sqlite3 **ppDb        /* OUT: SQLite db handle */
) {
	return sqlite3_open_v2(filename, ppDb, 0, NULL);
}

int sqlite3_open16(const void *filename, /* Database filename (UTF-8) */
                 sqlite3 **ppDb        /* OUT: SQLite db handle */
) {
	return sqlite3_open_v2((const char *)filename, ppDb, 0, NULL);
}

SQLITE_API int sqlite3_open_v2(const char *filename, /* Database filename (UTF-8) */
                    sqlite3 **ppDb,       /* OUT: SQLite db handle */
                    int flags,            /* Flags */
                    const char *zVfs      /* Name of VFS module to use */
) {
	if (filename && strcmp(filename, ":memory:") == 0) {
		filename = NULL;
	}
	*ppDb = NULL;
	if (zVfs) { /* unsupported so if set we complain */
		fprintf(stderr, "zVfs is not empty !!\n");
		return SQL_ERROR;
	}
	int rc;
	sqlite3 *pDb;
	pDb = (sqlite3 *)malloc(sizeof(sqlite3));
	pDb->last_changes = 0;
	pDb->total_changes = 0;
	rc = intarkdb_open(filename, &(pDb->db));
	
	if (rc != SQL_SUCCESS) {
		fprintf(stderr, "intarkdb_open failed !!\n");
		free(pDb);
		pDb = NULL;
        return SQL_ERROR;
	} 

	rc = intarkdb_connect(pDb->db, &(pDb->conn));

	if (rc != SQL_SUCCESS) {
		fprintf(stderr, "intarkdb_connect failed !!\n");
		free(pDb);
		pDb = NULL;
        return SQL_ERROR;
	}
	*ppDb = pDb;
	return rc;
}

int sqlite3_close(sqlite3 *db) {

	intarkdb_disconnect(&(db->conn));
	intarkdb_close(&(db->db));
	
	if (db) {
		free(db);
		db = NULL;
	}
	return SQL_SUCCESS;
}

int sqlite3_shutdown(void) {
	return SQL_SUCCESS;
}

/* In SQLite this function compiles the query into VDBE bytecode,
 * in the implementation it currently executes the query */
// TODO: prepare the statement instead of executing right away
SQLITE_API int sqlite3_prepare_v2(sqlite3 *db,           /* Database handle */
                        char const *zSql,      /* SQL statement, UTF-8 encoded */
                       int nByte,             /* Maximum length of zSql in bytes. */
                       sqlite3_stmt **ppStmt, /* OUT: Statement handle */
                        char const **pzTail    /* OUT: Pointer to unused portion of zSql */
) {
	if (!db || !ppStmt || !zSql) {
		return SQLITE_MISUSE;
	}

	if (pzTail) {
		*pzTail = zSql + strlen(zSql);
	}
	*ppStmt = NULL;

	*ppStmt = (sqlite3_stmt *)malloc(sizeof(sqlite3_stmt));
	(*ppStmt)->result = intarkdb_init_result();
	(*ppStmt)->current_row = -1;
	(*ppStmt)->row_count = 0;

	if (0 == strncmp(zSql, DATA_BASE_LIST_SQL, strlen(DATA_BASE_LIST_SQL))) {
		(*ppStmt)->current_row = SQLITE_MAX_LENGTH;

		(*ppStmt)->row_count = 0;
		return SQL_SUCCESS;
	} else {
		return intarkdb_prepare(db->conn, zSql, &((*ppStmt)->stmt));
	}
}

/* Prepare the next result to be retrieved */
int sqlite3_step(sqlite3_stmt *pStmt) {
	int ret = SQLITE_OK;
	if (!pStmt) {
		return SQLITE_MISUSE;
	}
	if (!pStmt->result) {
		pStmt->db->errCode = SQL_ERROR;
		return SQL_ERROR;
	}
	if (SQLITE_MAX_LENGTH <= pStmt->current_row) {
		if (SQLITE_MAX_LENGTH == pStmt->current_row) {
			pStmt->current_row = pStmt->current_row + 1;

			return SQLITE_ROW;	
		} else {
			return SQLITE_DONE;
		}
		
	} else if (-1 == pStmt->current_row) {
		 ret = intarkdb_execute_prepared(pStmt->stmt, pStmt->result);
		pStmt->row_count = intarkdb_row_count(pStmt->result);
	}

	if (SQLITE_OK != ret) {
		return ret;
	}
	
	if (pStmt->row_count <= pStmt->current_row + 1 ) {
		sqlite3_reset(pStmt);
		return SQLITE_DONE;
	} 
	
	pStmt->current_row = pStmt->current_row + 1;
	
	return SQLITE_ROW;
}

/* Execute multiple semicolon separated SQL statements
 * and execute the passed callback for each produced result,
 * largely copied from the original sqlite3 source */
int sqlite3_exec(sqlite3 *db,                /* The database on which the SQL executes */
                 const char *zSql,           /* The SQL to be executed */
                 sqlite3_callback xCallback, /* Invoke this callback routine */
                 void *pArg,                 /* First argument to xCallback() */
                 char **pzErrMsg             /* Write error messages here */
) {
	int rc = SQL_SUCCESS;            /* Return code */
	const char *zLeftover;         /* Tail of unprocessed SQL */
	sqlite3_stmt *pStmt = NULL; /* The current SQL statement */
	char **azCols = NULL;       /* Names of result columns */
	char **azVals = NULL;       /* Result values */

	if (zSql == NULL) {
		zSql = "";
	}

	while (rc == SQL_SUCCESS && zSql[0]) {
		int nCol;

		pStmt = NULL;
		rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, &zLeftover);
		if (rc != SQL_SUCCESS) {
			if (pzErrMsg) {
				const char* errmsg = sqlite3_errmsg(db);
				*pzErrMsg = errmsg ? sqlite3_strdup(errmsg) : NULL;
			}
			continue;
		}
		if (!pStmt) {
			/* this happens for a comment or white-space */
			zSql = zLeftover;
			continue;
		}

		nCol = sqlite3_column_count(pStmt);
		azCols = (char **)malloc(nCol * sizeof(const char *));
		azVals = (char **)malloc(nCol * sizeof(const char *));
		if (!azCols || !azVals) {
			goto exec_out;
		}
		for (int i = 0; i < nCol; i++) {
			azCols[i] = (char *)sqlite3_column_name(pStmt, i);
		}

		while (true) {
			rc = sqlite3_step(pStmt);

			/* Invoke the callback function if required */
			if (xCallback && rc == SQLITE_ROW) {
				for (int i = 0; i < nCol; i++) {
					azVals[i] = (char *)sqlite3_column_text(pStmt, i);
					if (!azVals[i] && sqlite3_column_type(pStmt, i) != SQLITE_NULL) {
						fprintf(stderr, "sqlite3_exec: out of memory.\n");
						goto exec_out;
					}
				}
				if (xCallback(pArg, nCol, azVals, azCols)) {
					/* EVIDENCE-OF: R-38229-40159 If the callback function to
					** sqlite3_exec() returns non-zero, then sqlite3_exec() will
					** return SQLITE_ABORT. */
					rc = SQLITE_ABORT;
					sqlite3_finalize(pStmt);
					pStmt = 0;
					fprintf(stderr, "sqlite3_exec: callback returned non-zero. "
					                "Aborting.\n");
					goto exec_out;
				}
			}
			if (rc == SQLITE_DONE) {
				rc = sqlite3_finalize(pStmt);
				pStmt = NULL;
				zSql = zLeftover;
				while (isspace(zSql[0]))
					zSql++;
				break;
			} else if (rc != SQLITE_ROW) {
				// error
				if (pzErrMsg) {
					const char* errmsg = sqlite3_errmsg(db);
					*pzErrMsg = errmsg ? sqlite3_strdup(errmsg) : NULL;
				}
				goto exec_out;
			}
		}

		sqlite3_free(azCols);
		sqlite3_free(azVals);
		azCols = NULL;
		azVals = NULL;
	}

exec_out:
	if (pStmt) {
		sqlite3_finalize(pStmt);
	}
	sqlite3_free(azCols);
	sqlite3_free(azVals);
	if (rc != SQL_SUCCESS && pzErrMsg && !*pzErrMsg) {
		// error but no error message set
		*pzErrMsg = sqlite3_strdup("Unknown error in DuckDB!");
	}
	return rc;
}


int sqlite3_column_count(sqlite3_stmt *pStmt) {
	if (!pStmt || !pStmt->result) {
		return 0;
	}
	return  intarkdb_column_count(pStmt->result);
}

/*
** Return the number of values available from the current row of the
** currently executing statement pStmt.
*/
int sqlite3_data_count(sqlite3_stmt *pStmt){
	if (!pStmt || !pStmt->result) {
		return 0;
	}
	return  intarkdb_column_count(pStmt->result);
}


////////////////////////////
//     sqlite3_column     //
////////////////////////////
int sqlite3_column_type(sqlite3_stmt *stmt, int iCol) {
	if (!stmt || !stmt->result) {
		return 0;
	}
	int type_id = intarkdb_column_type(stmt->result, iCol);
	switch (type_id) {
		case GS_TYPE_BIGINT:
		case GS_TYPE_INTEGER:
		case GS_TYPE_REAL:
		case GS_TYPE_NUMBER:
		case GS_TYPE_UINT32:
		case GS_TYPE_UINT64:
		case GS_TYPE_SMALLINT:
		case GS_TYPE_USMALLINT:
		case GS_TYPE_TINYINT:
		case GS_TYPE_UTINYINT:
			return SQLITE_INTEGER;
		case GS_TYPE_DECIMAL:
		case GS_TYPE_FLOAT:
			return SQLITE_FLOAT;
		case GS_TYPE_DATE:
		case GS_TYPE_CHAR:
		case GS_TYPE_VARCHAR:
		case GS_TYPE_STRING:
			return SQLITE_TEXT;
		case GS_TYPE_BLOB:
			return SQLITE_BLOB;
		default:
			// TODO(wangfenjin): agg function don't have type?
			return SQLITE_TEXT;
	}
	return SQLITE_TEXT;
}

const char *sqlite3_column_name(sqlite3_stmt *stmt, int N) {
	if (!stmt || !stmt->result) {
		return NULL;
	}
	return intarkdb_column_name(stmt->result, N);
}

double sqlite3_column_double(sqlite3_stmt *stmt, int iCol) {
	if (!stmt || !stmt->result) {
		return 0;
	}

	return intarkdb_value_double(stmt->result, stmt->current_row, iCol);
}

int sqlite3_column_int(sqlite3_stmt *stmt, int iCol) {
	if (!stmt || !stmt->result) {
		return 0;
	}

	return intarkdb_value_int32(stmt->result, stmt->current_row, iCol);
}

sqlite3_int64 sqlite3_column_int64(sqlite3_stmt *stmt, int iCol) {
	if (!stmt || !stmt->result) {
		return 0;
	}

	return intarkdb_value_int64(stmt->result, stmt->current_row, iCol);
}

const unsigned char *sqlite3_column_text(sqlite3_stmt *stmt, int iCol) {
	if (!stmt || !stmt->result) {
		return (const unsigned char *)"";
	}
	// FOR PRAGMA database_list;
	if (SQLITE_MAX_LENGTH <= stmt->current_row && 1 == iCol) {
		return (const unsigned char*)"main";
	}

	return (const unsigned char*)intarkdb_value_varchar(stmt->result, stmt->current_row, iCol);
}

const void *sqlite3_column_blob(sqlite3_stmt *stmt, int iCol) {
	if (!stmt || !stmt->result) {
		return NULL;
	}
	// TODO 
	return (void *)intarkdb_value_varchar(stmt->result, stmt->current_row, iCol);
}

int sqlite3_bind_int(sqlite3_stmt *stmt, int idx, int val) {
	if (!stmt) {
		return SQLITE_MISUSE;
	}
	int ret = intarkdb_bind_int32(stmt->stmt,idx, val);
	if (SQL_SUCCESS != ret) {
		return SQL_ERROR;
	}
	return SQL_SUCCESS;
}

int sqlite3_bind_int64(sqlite3_stmt *stmt, int idx, sqlite3_int64 val) {
	if (!stmt) {
		return SQLITE_MISUSE;
	}
	int ret = intarkdb_bind_int64(stmt->stmt,idx, val);
	if (SQL_SUCCESS != ret) {
		return SQL_ERROR;
	}
	return SQL_SUCCESS;
}

int sqlite3_bind_double(sqlite3_stmt *stmt, int idx, double val) {
	if (!stmt) {
		return SQLITE_MISUSE;
	}
	int ret = intarkdb_bind_double(stmt->stmt,idx, val);
	if (SQL_SUCCESS != ret) {
		return SQL_ERROR;
	}
	return SQL_SUCCESS;
}

int sqlite3_bind_null(sqlite3_stmt *stmt, int idx) {
	if (!stmt) {
		return SQLITE_MISUSE;
	}
	int ret = intarkdb_bind_null(stmt->stmt,idx);
	if (SQL_SUCCESS != ret) {
		return SQL_ERROR;
	}
	return SQL_SUCCESS;
	}

SQLITE_API int sqlite3_bind_value(sqlite3_stmt *stmt, int idx, const sqlite3_value *val) {
	fprintf(stderr, "sqlite3_bind_value: unsupported.\n");
	return SQL_ERROR;
}

int sqlite3_bind_text(sqlite3_stmt *stmt, int idx, const char *val, int length, void (*free_func)(void *)) {
	if (!val || !stmt) {
		return SQLITE_MISUSE;
	}
	char *value;
	if (length < 0){
		value = (char*)val;
	} else {
		value = (char *) malloc(sizeof(char) * length + 1);
		memset(value, '\0', sizeof(char) * length + 1);
		strncpy(value, val, length);
	}
	int ret = intarkdb_bind_varchar(stmt->stmt,idx, value);
	if (length >= 0 ) {
		free(value);
	}
	if (SQL_SUCCESS != ret && free_func) {
		free_func((void *)val);
		return SQL_ERROR;
	}
	return SQL_SUCCESS;
}

int sqlite3_bind_blob(sqlite3_stmt *stmt, int idx, const void *val, int length, void (*free_func)(void *)) {
	if (!val) {
		return SQLITE_MISUSE;
	}
	int ret = intarkdb_bind_blob(stmt->stmt, idx, val, length);
	if (SQL_SUCCESS != ret && free_func) {
		free_func((void *)val);
		return SQL_ERROR;
	}
	return SQL_SUCCESS;
	
}
int sqlite3_bind_blob64(sqlite3_stmt *stmt, int idx, const void* val, sqlite3_uint64 n, void(*free_func)(void*)) {
	return sqlite3_bind_blob(stmt, idx, val, n, free_func);
}

int sqlite3_clear_bindings(sqlite3_stmt *stmt) {

	if (!stmt) {
		return SQLITE_MISUSE;
	}
	fprintf(stderr, "sqlite3_clear_bindings: unsupported.\n");
	return SQL_SUCCESS;
}

int sqlite3_finalize(sqlite3_stmt *pStmt) {
	if (pStmt) {
		if (pStmt->result) {
			intarkdb_destroy_result(pStmt->result); 
		}

		free(pStmt);
		pStmt = NULL;
	}
	return SQL_SUCCESS;
}

// /*
// ** Some systems have stricmp().  Others have strcasecmp().  Because
// ** there is no consistency, we will define our own.
// **
// ** IMPLEMENTATION-OF: R-30243-02494 The sqlite3_stricmp() and
// ** sqlite3_strnicmp() APIs allow applications and extensions to compare
// ** the contents of two buffers containing UTF-8 strings in a
// ** case-independent fashion, using the same definition of "case
// ** independence" that SQLite uses internally when comparing identifiers.
// */

const unsigned char sqlite3UpperToLower[] = {
    0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,
    22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,
    44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  97,
    98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
    120, 121, 122, 91,  92,  93,  94,  95,  96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
    110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131,
    132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153,
    154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175,
    176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197,
    198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219,
    220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241,
    242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255};

int sqlite3StrICmp(const char *zLeft, const char *zRight) {
	unsigned char *a, *b;
	int c;
	a = (unsigned char *)zLeft;
	b = (unsigned char *)zRight;
	for (;;) {
		c = (int)sqlite3UpperToLower[*a] - (int)sqlite3UpperToLower[*b];
		if (c || *a == 0)
			break;
		a++;
		b++;
	}
	return c;
}

SQLITE_API int sqlite3_stricmp(const char *zLeft, const char *zRight) {
	if (zLeft == 0) {
		return zRight ? -1 : 0;
	} else if (zRight == 0) {
		return 1;
	}
	return sqlite3StrICmp(zLeft, zRight);
}

SQLITE_API int sqlite3_strnicmp(const char *zLeft, const char *zRight, int N) {
	unsigned char *a, *b;
	if (zLeft == 0) {
		return zRight ? -1 : 0;
	} else if (zRight == 0) {
		return 1;
	}
	a = (unsigned char *)zLeft;
	b = (unsigned char *)zRight;
	while (N-- > 0 && *a != 0 && sqlite3UpperToLower[*a] == sqlite3UpperToLower[*b]) {
		a++;
		b++;
	}
	return N < 0 ? 0 : sqlite3UpperToLower[*a] - sqlite3UpperToLower[*b];
}

void *sqlite3_malloc64(sqlite3_uint64 n) {
	return malloc(n);
}

char *sqlite3_strdup(const char *str) {
	char *result = (char *)sqlite3_malloc64(strlen(str) + 1);
	strcpy(result, str);
	return result;
}

void sqlite3_free(void *pVoid) {
	if (pVoid) {
		free(pVoid);
		pVoid = NULL;
	}
}

void *sqlite3_malloc(int n) {
	return sqlite3_malloc64(n);
}

void *sqlite3_realloc64(void *ptr, sqlite3_uint64 n) {
	return realloc(ptr, n);
}

void *sqlite3_realloc(void *ptr, int n) {
	return sqlite3_realloc64(ptr, n);
}

// TODO: stub
int sqlite3_config(int i, ...) {
	fprintf(stderr, "sqlite3_config: unsupported.\n");
	return SQL_SUCCESS;
}

int sqlite3_errcode(sqlite3 *db) {
	if (!db) {
		return SQLITE_NOMEM;
	}
	return db->errCode; //! We should return the exact error code
}

int sqlite3_extended_errcode(sqlite3 *db) {
	return sqlite3_errcode(db);
}

const char *sqlite3_errmsg(sqlite3 *db) {
	const char *z = NULL;
	if (!db) {
		return sqlite3ErrStr(SQLITE_ERROR);
	}
	if( z==0 ){
		z = sqlite3ErrStr(db->errCode);
	}
	return z;
}

void sqlite3_interrupt(sqlite3 *db) {
	fprintf(stderr, "sqlite3_interrupt: unsupported.\n");
}

int sqlite3_reset(sqlite3_stmt *stmt) {
	if (stmt) {
		intarkdb_free_row(stmt->result);
		stmt->current_row = -1;
		stmt->row_count = 0;
	}
	return SQL_SUCCESS;
}

// support functions for shell.c
// most are dummies, we don't need them really

int sqlite3_db_status(sqlite3 *db, int op, int *pCur, int *pHiwtr, int resetFlg) {
	fprintf(stderr, "sqlite3_db_status: unsupported.\n");
	return -1;
}

int sqlite3_changes(sqlite3 *db) {
	return db->last_changes;
}

int sqlite3_total_changes(sqlite3 *db) {
	return db->total_changes;
}

SQLITE_API sqlite3_int64 sqlite3_last_insert_rowid(sqlite3 *db) {
	fprintf(stderr, "sqlite3_last_insert_rowid: unsupported.\n");
	return SQL_ERROR;
}

// length of varchar or blob value
int sqlite3_column_bytes(sqlite3_stmt *stmt, int iCol) {
	if (!stmt || iCol < 0 || !stmt->result) {
		return 0;
	}
	return strlen((const char*)sqlite3_column_text(stmt, iCol));
}

// length of varchar or blob value
int sqlite3_column_bytes16(sqlite3_stmt *stmt, int iCol) {
	if (!stmt || iCol < 0 || !stmt->result) {
		return 0;
	}
	return strlen((const char*)sqlite3_column_text(stmt, iCol));
}

sqlite3_value *sqlite3_column_value(sqlite3_stmt *stmt, int iCol) {
	fprintf(stderr, "sqlite3_column_value: unsupported.\n");
	return NULL;
}

int sqlite3_db_config(sqlite3 *db, int op, ...) {
	fprintf(stderr, "sqlite3_db_config: unsupported.\n");
	return -1;
}

int sqlite3_get_autocommit(sqlite3 *db) {
	fprintf(stderr, "sqlite3_get_autocommit: unsupported.\n");
	return -1;
}

int sqlite3_limit(sqlite3 *db, int id, int newVal) {
	fprintf(stderr, "sqlite3_limit: unsupported.\n");
	return -1;
}

int sqlite3_enable_load_extension(sqlite3 *db, int onoff) {
	fprintf(stderr, "sqlite3_enable_load_extension: unsupported.\n");
	return -1;
}

int sqlite3_load_extension(sqlite3 *db,       /* Load the extension into this database connection */
                           const char *zFile, /* Name of the shared library containing extension */
                           const char *zProc, /* Entry point.  Derived from zFile if 0 */
                           char **pzErrMsg    /* Put error message here if not 0 */
) {
	fprintf(stderr, "sqlite3_load_extension: unsupported.\n");
	return -1;
}

int sqlite3_create_module(sqlite3 *db,             /* SQLite connection to register module with */
                          const char *zName,       /* Name of the module */
                          const sqlite3_module *p, /* Methods for the module */
                          void *pClientData        /* Client data for xCreate/xConnect */
) {
	fprintf(stderr, "sqlite3_create_module: unsupported.\n");
	return -1;
}

int sqlite3_create_function(sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep, void *pApp,
                            void (*xFunc)(sqlite3_context *, int, sqlite3_value **),
                            void (*xStep)(sqlite3_context *, int, sqlite3_value **),
                            void (*xFinal)(sqlite3_context *)) {
	fprintf(stderr, "sqlite3_create_function: unsupported.\n");
	return SQL_SUCCESS;
}

SQLITE_API void sqlite3_result_error(sqlite3_context *context, const char *msg, int n_bytes) {
	context->isError = SQL_ERROR;
	sqlite3_result_text(context, msg, n_bytes, NULL);
}

SQLITE_API void sqlite3_result_int(sqlite3_context *context, int val) {
	sqlite3_result_int64(context, val);
}

SQLITE_API void sqlite3_result_int64(sqlite3_context *context, sqlite3_int64 val) {
	fprintf(stderr, "sqlite3_result_int64: unsupported.\n");
	context->result.u.i = val;
}

SQLITE_API void sqlite3_result_null(sqlite3_context *context) {
	fprintf(stderr, "sqlite3_result_null: unsupported.\n");
}

SQLITE_API void sqlite3_result_text(sqlite3_context *context, const char *str_c, int n_chars, void (*xDel)(void *)) {
	if (!str_c) {
		context->isError = SQLITE_MISUSE;
		return;
	}
	if (n_chars < 0) {
		n_chars = strlen(str_c);
	}
	if (xDel && xDel != SQLITE_TRANSIENT) {
		xDel((void *)str_c);
	}
}

const void *sqlite3_value_blob(sqlite3_value *val) {
	return sqlite3_value_text(val);
}

const unsigned char *sqlite3_value_text(sqlite3_value *val) {
	// TODO
	if (!val) {
		val->db->errCode = SQLITE_MISUSE;
		return NULL;
	}
	val->db->errCode = SQLITE_MISMATCH;

	return (const unsigned char*)val->str;
}

SQLITE_API const void *sqlite3_value_text16(sqlite3_value *val) {
	return sqlite3_value_text(val);
}

SQLITE_API const void *sqlite3_value_text16le(sqlite3_value *val) {
	return sqlite3_value_text(val);
}

SQLITE_API const void *sqlite3_value_text16be(sqlite3_value *val) {
	return sqlite3_value_text(val);
}

int sqlite3_create_collation(sqlite3 *db, const char *zName, int eTextRep, void *pArg,
                                        int (*xCompare)(void *, int, const void *, int, const void *)) {
	fprintf(stderr, "sqlite3_create_collation: unsupported.\n");
	return SQL_ERROR;
}

SQLITE_API int sqlite3_threadsafe(void) {
	fprintf(stderr, "sqlite3_threadsafe: unsupported.\n");
	return SQL_SUCCESS;
}

SQLITE_API sqlite3_stmt *sqlite3_next_stmt(sqlite3 *pDb, sqlite3_stmt *pStmt) {
	fprintf(stderr, "sqlite3_next_stmt: unsupported.\n");
	return NULL;
}

SQLITE_API int sqlite3_collation_needed(sqlite3 *db, void *val, void (* func)(void *v, sqlite3 *db, int eTextRep, const char * str)) {
	fprintf(stderr, "sqlite3_collation_needed: unsupported.\n");
	return SQL_ERROR;
}

SQLITE_API int sqlite3_create_collation_v2(sqlite3 *db, const char *zName, int eTextRep, void *pArg,
                                           int (*xCompare)(void *, int, const void *, int, const void *),
                                           void (*xDestroy)(void *)) {
	fprintf(stderr, "sqlite3_create_collation_v2: unsupported.\n");
	return SQL_ERROR;
}


/*
** Return a static string that describes the kind of error specified in the
** argument.
*/
const char *sqlite3ErrStr(int rc){
	// printf("sqlite3ErrStr and rc is %d\n", rc);

  static const char* const aMsg[] = {
    /* SQLITE_OK          */ "not an error",
    /* SQLITE_ERROR       */ "SQL logic error",
    /* SQLITE_INTERNAL    */ 0,
    /* SQLITE_PERM        */ "access permission denied",
    /* SQLITE_ABORT       */ "query aborted",
    /* SQLITE_BUSY        */ "database is locked",
    /* SQLITE_LOCKED      */ "database table is locked",
    /* SQLITE_NOMEM       */ "out of memory",
    /* SQLITE_READONLY    */ "attempt to write a readonly database",
    /* SQLITE_INTERRUPT   */ "interrupted",
    /* SQLITE_IOERR       */ "disk I/O error",
    /* SQLITE_CORRUPT     */ "database disk image is malformed",
    /* SQLITE_NOTFOUND    */ "unknown operation",
    /* SQLITE_FULL        */ "database or disk is full",
    /* SQLITE_CANTOPEN    */ "unable to open database file",
    /* SQLITE_PROTOCOL    */ "locking protocol",
    /* SQLITE_EMPTY       */ 0,
    /* SQLITE_SCHEMA      */ "database schema has changed",
    /* SQLITE_TOOBIG      */ "string or blob too big",
    /* SQLITE_CONSTRAINT  */ "constraint failed",
    /* SQLITE_MISMATCH    */ "datatype mismatch",
    /* SQLITE_MISUSE      */ "bad parameter or other API misuse",
#ifdef SQLITE_DISABLE_LFS
    /* SQLITE_NOLFS       */ "large file support is disabled",
#else
    /* SQLITE_NOLFS       */ 0,
#endif
    /* SQLITE_AUTH        */ "authorization denied",
    /* SQLITE_FORMAT      */ 0,
    /* SQLITE_RANGE       */ "column index out of range",
    /* SQLITE_NOTADB      */ "file is not a database",
    /* SQLITE_NOTICE      */ "notification message",
    /* SQLITE_WARNING     */ "warning message",
  };
  const char *zErr = "unknown error";
  switch( rc ){
    case SQLITE_ABORT_ROLLBACK: {
      zErr = "abort due to ROLLBACK";
      break;
    }
    case SQLITE_ROW: {
      zErr = "another row available";
      break;
    }
    case SQLITE_DONE: {
      zErr = "no more rows available";
      break;
    }
    default: {
      rc &= 0xff;
      if( ALWAYS(rc>=0) && rc<ArraySize(aMsg) && aMsg[rc]!=0 ){
        zErr = aMsg[rc];
      }
      break;
    }
  }
  return zErr;
}


/*
** This routine installs a default busy handler that waits for the
** specified number of milliseconds before returning 0.
*/
int sqlite3_busy_timeout(sqlite3 *db, int ms){

	fprintf(stderr, "sqlite3_busy_timeout: unsupported.\n");
	return SQLITE_ERROR;
}


/*
** sqlite3_snprintf() works like snprintf() except that it ignores the
** current locale settings.  This is important for SQLite because we
** are not able to use a "," as the decimal point in place of "." as
** specified by some locales.
**
** Oops:  The first two arguments of sqlite3_snprintf() are backwards
** from the snprintf() standard.  Unfortunately, it is too late to change
** this without breaking compatibility, so we just have to live with the
** mistake.
**
** sqlite3_vsnprintf() is the varargs version.
*/
char *sqlite3_vsnprintf(int n, char *zBuf, const char *zFormat, va_list ap){
	if( n<=0 ) return zBuf;
	vsnprintf(zBuf, n, zFormat, ap);
	return zBuf;
}

char *sqlite3_snprintf(int n, char *zBuf, const char *zFormat, ...){
	va_list ap;
	if( n<=0 ) return zBuf;
	va_start(ap, zFormat);
	sqlite3_vsnprintf(n, zBuf, zFormat, ap);
	va_end(ap);
	return zBuf;
}

/*
** Return a string that describes the kind of error specified in the
** argument.  For now, this simply calls the internal sqlite3ErrStr()
** function.
*/
const char *sqlite3_errstr(int rc){
  return sqlite3ErrStr(rc);
}

#define STRACCUM_NOMEM         1
#define STRACCUM_TOOBIG        2
#define SQLITE_PRINTF_INTERNAL 0x01 /* Internal-use-only converters allowed */
#define SQLITE_PRINTF_SQLFUNC  0x02 /* SQL function arguments to VXPrintf */
#define SQLITE_PRINTF_MALLOCED 0x04 /* True if xText is allocated space */

#define isMalloced(X) (((X)->printfFlags & SQLITE_PRINTF_MALLOCED) != 0)

void sqlite3StrAccumAppend(StrAccum *p, const char *z, int N);
void sqlite3AppendChar(StrAccum *p, int N, char c);
void sqlite3StrAccumAppendAll(StrAccum *p, const char *z);
void sqlite3StrAccumReset(StrAccum *p);

/*
** The "printf" code that follows dates from the 1980's.  It is in
** the public domain.
**
**************************************************************************
**
** This file contains code for a set of "printf"-like routines.  These
** routines format strings much like the printf() from the standard C
** library, though the implementation here has enhancements to support
** SQLite.
*/

/*
** Conversion types fall into various categories as defined by the
** following enumeration.
*/
#define etRADIX     0 /* non-decimal integer types.  %x %o */
#define etFLOAT     1 /* Floating point.  %f */
#define etEXP       2 /* Exponentional notation. %e and %E */
#define etGENERIC   3 /* Floating or exponential, depending on exponent. %g */
#define etSIZE      4 /* Return number of characters processed so far. %n */
#define etSTRING    5 /* Strings. %s */
#define etDYNSTRING 6 /* Dynamically allocated strings. %z */
#define etPERCENT   7 /* Percent symbol. %% */
#define etCHARX     8 /* Characters. %c */
/* The rest are extensions, not normally found in printf() */
#define etSQLESCAPE 9 /* Strings with '\'' doubled.  %q */
#define etSQLESCAPE2                                                                                                   \
	10 /* Strings with '\'' doubled and enclosed in '',                                                                \
	     NULL pointers replaced by SQL NULL.  %Q */
#define etTOKEN      11 /* a pointer to a Token structure */
#define etSRCLIST    12 /* a pointer to a SrcList */
#define etPOINTER    13 /* The %p conversion */
#define etSQLESCAPE3 14 /* %w -> Strings with '\"' doubled */
#define etORDINAL    15 /* %r -> 1st, 2nd, 3rd, 4th, etc.  English only */
#define etDECIMAL    16 /* %d or %u, but not %x, %o */

#define etINVALID 17 /* Any unrecognized conversion type */

/*
** An "etByte" is an 8-bit unsigned value.
*/
typedef unsigned char etByte;

/*
** Each builtin conversion character (ex: the 'd' in "%d") is described
** by an instance of the following structure
*/
typedef struct et_info { /* Information about each format field */
	char fmttype;        /* The format field code letter */
	etByte base;         /* The base for radix conversion */
	etByte flags;        /* One or more of FLAG_ constants below */
	etByte type;         /* Conversion paradigm */
	etByte charset;      /* Offset into aDigits[] of the digits string */
	etByte prefix;       /* Offset into aPrefix[] of the prefix string */
} et_info;

/*
** Allowed values for et_info.flags
*/
#define FLAG_SIGNED 1 /* True if the value to convert is signed */
#define FLAG_STRING 4 /* Allow infinite precision */

/*
** The following table is searched linearly, so it is good to put the
** most frequently used conversion types first.
*/
static const char aDigits[] = "0123456789ABCDEF0123456789abcdef";
static const char aPrefix[] = "-x0\000X0";
static const et_info fmtinfo[] = {
    {'d', 10, 1, etDECIMAL, 0, 0},
    {'s', 0, 4, etSTRING, 0, 0},
    {'g', 0, 1, etGENERIC, 30, 0},
    {'z', 0, 4, etDYNSTRING, 0, 0},
    {'q', 0, 4, etSQLESCAPE, 0, 0},
    {'Q', 0, 4, etSQLESCAPE2, 0, 0},
    {'w', 0, 4, etSQLESCAPE3, 0, 0},
    {'c', 0, 0, etCHARX, 0, 0},
    {'o', 8, 0, etRADIX, 0, 2},
    {'u', 10, 0, etDECIMAL, 0, 0},
    {'x', 16, 0, etRADIX, 16, 1},
    {'X', 16, 0, etRADIX, 0, 4},
#ifndef SQLITE_OMIT_FLOATING_POINT
    {'f', 0, 1, etFLOAT, 0, 0},
    {'e', 0, 1, etEXP, 30, 0},
    {'E', 0, 1, etEXP, 14, 0},
    {'G', 0, 1, etGENERIC, 14, 0},
#endif
    {'i', 10, 1, etDECIMAL, 0, 0},
    {'n', 0, 0, etSIZE, 0, 0},
    {'%', 0, 0, etPERCENT, 0, 0},
    {'p', 16, 0, etPOINTER, 0, 1},

    /* All the rest are undocumented and are for internal use only */
    {'T', 0, 0, etTOKEN, 0, 0},
    {'S', 0, 0, etSRCLIST, 0, 0},
    {'r', 10, 1, etORDINAL, 0, 0},
};

/*
** If SQLITE_OMIT_FLOATING_POINT is defined, then none of the floating point
** conversions will work.
*/
#ifndef SQLITE_OMIT_FLOATING_POINT
/*
** "*val" is a double such that 0.1 <= *val < 10.0
** Return the ascii code for the leading digit of *val, then
** multiply "*val" by 10.0 to renormalize.
**
** Example:
**     input:     *val = 3.14159
**     output:    *val = 1.4159    function return = '3'
**
** The counter *cnt is incremented each time.  After counter exceeds
** 16 (the number of significant digits in a 64-bit float) '0' is
** always returned.
*/
static char et_getdigit(LONGDOUBLE_TYPE *val, int *cnt) {
	int digit;
	LONGDOUBLE_TYPE d;
	if ((*cnt) <= 0)
		return '0';
	(*cnt)--;
	digit = (int)*val;
	d = digit;
	digit += '0';
	*val = (*val - d) * 10.0;
	return (char)digit;
}
#endif /* SQLITE_OMIT_FLOATING_POINT */

/*
** Set the StrAccum object to an error mode.
*/
static void setStrAccumError(StrAccum *p, u8 eError) {
	assert(eError == STRACCUM_NOMEM || eError == STRACCUM_TOOBIG);
	p->accError = eError;
	p->nAlloc = 0;
}

/*
** On machines with a small stack size, you can redefine the
** SQLITE_PRINT_BUF_SIZE to be something smaller, if desired.
*/
#ifndef SQLITE_PRINT_BUF_SIZE
#define SQLITE_PRINT_BUF_SIZE 70
#endif
#define etBUFSIZE SQLITE_PRINT_BUF_SIZE /* Size of the output buffer */

/*
** Render a string given by "fmt" into the StrAccum object.
*/
void sqlite3VXPrintf(StrAccum *pAccum, /* Accumulate results here */
                     const char *fmt,  /* Format string */
                     va_list ap        /* arguments */
) {
	int c;                     /* Next character in the format string */
	char *bufpt;               /* Pointer to the conversion buffer */
	int precision;             /* Precision of the current field */
	int length;                /* Length of the field */
	int idx;                   /* A general purpose loop counter */
	int width;                 /* Width of the current field */
	etByte flag_leftjustify;   /* True if "-" flag is present */
	etByte flag_prefix;        /* '+' or ' ' or 0 for prefix */
	etByte flag_alternateform; /* True if "#" flag is present */
	etByte flag_altform2;      /* True if "!" flag is present */
	etByte flag_zeropad;       /* True if field width constant starts with zero */
	etByte flag_long;          /* 1 for the "l" flag, 2 for "ll", 0 by default */
	etByte done;               /* Loop termination flag */
	etByte cThousand;          /* Thousands separator for %d and %u */
	etByte xtype = etINVALID;  /* Conversion paradigm */
	u8 bArgList;               /* True for SQLITE_PRINTF_SQLFUNC */
	char prefix;               /* Prefix character.  "+" or "-" or " " or '\0'. */
	sqlite_uint64 longvalue;   /* Value for integer types */
	LONGDOUBLE_TYPE realvalue; /* Value for real types */
	const et_info *infop;      /* Pointer to the appropriate info structure */
	char *zOut;                /* Rendering buffer */
	int nOut;                  /* Size of the rendering buffer */
	char *zExtra = 0;          /* Malloced memory used by some conversion */
#ifndef SQLITE_OMIT_FLOATING_POINT
	int exp, e2;     /* exponent of real numbers */
	int nsd;         /* Number of significant digits returned */
	double rounder;  /* Used for rounding floating point values */
	etByte flag_dp;  /* True if decimal point should be shown */
	etByte flag_rtz; /* True if trailing zeros should be removed */
#endif
	char buf[etBUFSIZE]; /* Conversion buffer */

	/* pAccum never starts out with an empty buffer that was obtained from
	** malloc().  This precondition is required by the mprintf("%z...")
	** optimization. */
	assert(pAccum->nChar > 0 || (pAccum->printfFlags & SQLITE_PRINTF_MALLOCED) == 0);

	bufpt = 0;
	if ((pAccum->printfFlags & SQLITE_PRINTF_SQLFUNC) != 0) {
		assert(0);
	} else {
		bArgList = 0;
	}
	for (; (c = (*fmt)) != 0; ++fmt) {
		if (c != '%') {
			bufpt = (char *)fmt;
#if HAVE_STRCHRNUL
			fmt = strchrnul(fmt, '%');
#else
			do {
				fmt++;
			} while (*fmt && *fmt != '%');
#endif
			sqlite3StrAccumAppend(pAccum, bufpt, (int)(fmt - bufpt));
			if (*fmt == 0)
				break;
		}
		if ((c = (*++fmt)) == 0) {
			sqlite3StrAccumAppend(pAccum, "%", 1);
			break;
		}
		/* Find out what flags are present */
		flag_leftjustify = flag_prefix = cThousand = flag_alternateform = flag_altform2 = flag_zeropad = 0;
		done = 0;
		do {
			switch (c) {
			case '-':
				flag_leftjustify = 1;
				break;
			case '+':
				flag_prefix = '+';
				break;
			case ' ':
				flag_prefix = ' ';
				break;
			case '#':
				flag_alternateform = 1;
				break;
			case '!':
				flag_altform2 = 1;
				break;
			case '0':
				flag_zeropad = 1;
				break;
			case ',':
				cThousand = ',';
				break;
			default:
				done = 1;
				break;
			}
		} while (!done && (c = (*++fmt)) != 0);
		/* Get the field width */
		if (c == '*') {
			if (bArgList) {
				assert(0);
			} else {
				width = va_arg(ap, int);
			}
			if (width < 0) {
				flag_leftjustify = 1;
				width = width >= -2147483647 ? -width : 0;
			}
			c = *++fmt;
		} else {
			unsigned wx = 0;
			while (c >= '0' && c <= '9') {
				wx = wx * 10 + c - '0';
				c = *++fmt;
			}
			width = wx & 0x7fffffff;
		}
		assert(width >= 0);
#ifdef SQLITE_PRINTF_PRECISION_LIMIT
		if (width > SQLITE_PRINTF_PRECISION_LIMIT) {
			width = SQLITE_PRINTF_PRECISION_LIMIT;
		}
#endif

		/* Get the precision */
		if (c == '.') {
			c = *++fmt;
			if (c == '*') {
				if (bArgList) {
					assert(0);
				} else {
					precision = va_arg(ap, int);
				}
				c = *++fmt;
				if (precision < 0) {
					precision = precision >= -2147483647 ? -precision : -1;
				}
			} else {
				unsigned px = 0;
				while (c >= '0' && c <= '9') {
					px = px * 10 + c - '0';
					c = *++fmt;
				}
				precision = px & 0x7fffffff;
			}
		} else {
			precision = -1;
		}
		assert(precision >= (-1));
#ifdef SQLITE_PRINTF_PRECISION_LIMIT
		if (precision > SQLITE_PRINTF_PRECISION_LIMIT) {
			precision = SQLITE_PRINTF_PRECISION_LIMIT;
		}
#endif

		/* Get the conversion type modifier */
		if (c == 'l') {
			flag_long = 1;
			c = *++fmt;
			if (c == 'l') {
				flag_long = 2;
				c = *++fmt;
			}
		} else {
			flag_long = 0;
		}
		/* Fetch the info entry for the field */
		infop = &fmtinfo[0];
		xtype = etINVALID;
		for (idx = 0; idx < ArraySize(fmtinfo); idx++) {
			if (c == fmtinfo[idx].fmttype) {
				infop = &fmtinfo[idx];
				xtype = infop->type;
				break;
			}
		}

		/*
		** At this point, variables are initialized as follows:
		**
		**   flag_alternateform          TRUE if a '#' is present.
		**   flag_altform2               TRUE if a '!' is present.
		**   flag_prefix                 '+' or ' ' or zero
		**   flag_leftjustify            TRUE if a '-' is present or if the
		**                               field width was negative.
		**   flag_zeropad                TRUE if the width began with 0.
		**   flag_long                   1 for "l", 2 for "ll"
		**   width                       The specified field width.  This is
		**                               always non-negative.  Zero is the default.
		**   precision                   The specified precision.  The default
		**                               is -1.
		**   xtype                       The class of the conversion.
		**   infop                       Pointer to the appropriate info struct.
		*/
		switch (xtype) {
		case etPOINTER:
			flag_long = sizeof(char *) == sizeof(i64) ? 2 : sizeof(char *) == sizeof(long int) ? 1 : 0;
			/* Fall through into the next case */
		case etORDINAL:
		case etRADIX:
			cThousand = 0;
			/* Fall through into the next case */
		case etDECIMAL:
			if (infop->flags & FLAG_SIGNED) {
				i64 v;
				if (bArgList) {
					assert(0);
				} else if (flag_long) {
					if (flag_long == 2) {
						v = va_arg(ap, i64);
					} else {
						v = va_arg(ap, long int);
					}
				} else {
					v = va_arg(ap, int);
				}
				if (v < 0) {
					if (v == SMALLEST_INT64) {
						longvalue = ((u64)1) << 63;
					} else {
						longvalue = -v;
					}
					prefix = '-';
				} else {
					longvalue = v;
					prefix = flag_prefix;
				}
			} else {
				if (bArgList) {
					assert(0);
				} else if (flag_long) {
					if (flag_long == 2) {
						longvalue = va_arg(ap, u64);
					} else {
						longvalue = va_arg(ap, unsigned long int);
					}
				} else {
					longvalue = va_arg(ap, unsigned int);
				}
				prefix = 0;
			}
			if (longvalue == 0)
				flag_alternateform = 0;
			if (flag_zeropad && precision < width - (prefix != 0)) {
				precision = width - (prefix != 0);
			}
			if (precision < etBUFSIZE - 10 - etBUFSIZE / 3) {
				nOut = etBUFSIZE;
				zOut = buf;
			} else {
				u64 n = (u64)precision + 10 + precision / 3;
				zOut = zExtra = sqlite3Malloc(n);
				if (zOut == 0) {
					setStrAccumError(pAccum, STRACCUM_NOMEM);
					return;
				}
				nOut = (int)n;
			}
			bufpt = &zOut[nOut - 1];
			if (xtype == etORDINAL) {
				static const char zOrd[] = "thstndrd";
				int x = (int)(longvalue % 10);
				if (x >= 4 || (longvalue / 10) % 10 == 1) {
					x = 0;
				}
				*(--bufpt) = zOrd[x * 2 + 1];
				*(--bufpt) = zOrd[x * 2];
			}
			{
				const char *cset = &aDigits[infop->charset];
				u8 base = infop->base;
				do { /* Convert to ascii */
					*(--bufpt) = cset[longvalue % base];
					longvalue = longvalue / base;
				} while (longvalue > 0);
			}
			length = (int)(&zOut[nOut - 1] - bufpt);
			while (precision > length) {
				*(--bufpt) = '0'; /* Zero pad */
				length++;
			}
			if (cThousand) {
				int nn = (length - 1) / 3; /* Number of "," to insert */
				int ix = (length - 1) % 3 + 1;
				bufpt -= nn;
				for (idx = 0; nn > 0; idx++) {
					bufpt[idx] = bufpt[idx + nn];
					ix--;
					if (ix == 0) {
						bufpt[++idx] = cThousand;
						nn--;
						ix = 3;
					}
				}
			}
			if (prefix)
				*(--bufpt) = prefix;                   /* Add sign */
			if (flag_alternateform && infop->prefix) { /* Add "0" or "0x" */
				const char *pre;
				char x;
				pre = &aPrefix[infop->prefix];
				for (; (x = (*pre)) != 0; pre++)
					*(--bufpt) = x;
			}
			length = (int)(&zOut[nOut - 1] - bufpt);
			break;
		case etFLOAT:
		case etEXP:
		case etGENERIC:
			if (bArgList) {
				assert(0);
			} else {
				realvalue = va_arg(ap, double);
			}
#ifdef SQLITE_OMIT_FLOATING_POINT
			length = 0;
#else
			if (precision < 0)
				precision = 6; /* Set default precision */
			if (realvalue < 0.0) {
				realvalue = -realvalue;
				prefix = '-';
			} else {
				prefix = flag_prefix;
			}
			if (xtype == etGENERIC && precision > 0)
				precision--;
			for (idx = precision & 0xfff, rounder = 0.5; idx > 0; idx--, rounder *= 0.1) {
			}
			if (xtype == etFLOAT)
				realvalue += rounder;
			/* Normalize realvalue to within 10.0 > realvalue >= 1.0 */
			exp = 0;
			if (sqlite3IsNaN((double)realvalue)) {
				bufpt = "NaN";
				length = 3;
				break;
			}
			if (realvalue > 0.0) {
				LONGDOUBLE_TYPE scale = 1.0;
				while (realvalue >= 1e100 * scale && exp <= 350) {
					scale *= 1e100;
					exp += 100;
				}
				while (realvalue >= 1e10 * scale && exp <= 350) {
					scale *= 1e10;
					exp += 10;
				}
				while (realvalue >= 10.0 * scale && exp <= 350) {
					scale *= 10.0;
					exp++;
				}
				realvalue /= scale;
				while (realvalue < 1e-8) {
					realvalue *= 1e8;
					exp -= 8;
				}
				while (realvalue < 1.0) {
					realvalue *= 10.0;
					exp--;
				}
				if (exp > 350) {
					bufpt = buf;
					buf[0] = prefix;
					memcpy(buf + (prefix != 0), "Inf", 4);
					length = 3 + (prefix != 0);
					break;
				}
			}
			bufpt = buf;
			/*
			** If the field type is etGENERIC, then convert to either etEXP
			** or etFLOAT, as appropriate.
			*/
			if (xtype != etFLOAT) {
				realvalue += rounder;
				if (realvalue >= 10.0) {
					realvalue *= 0.1;
					exp++;
				}
			}
			if (xtype == etGENERIC) {
				flag_rtz = !flag_alternateform;
				if (exp < -4 || exp > precision) {
					xtype = etEXP;
				} else {
					precision = precision - exp;
					xtype = etFLOAT;
				}
			} else {
				flag_rtz = flag_altform2;
			}
			if (xtype == etEXP) {
				e2 = 0;
			} else {
				e2 = exp;
			}
			if (MAX(e2, 0) + (i64)precision + (i64)width > etBUFSIZE - 15) {
				bufpt = zExtra = sqlite3Malloc(MAX(e2, 0) + (i64)precision + (i64)width + 15);
				if (bufpt == 0) {
					setStrAccumError(pAccum, STRACCUM_NOMEM);
					return;
				}
			}
			zOut = bufpt;
			nsd = 16 + flag_altform2 * 10;
			flag_dp = (precision > 0 ? 1 : 0) | flag_alternateform | flag_altform2;
			/* The sign in front of the number */
			if (prefix) {
				*(bufpt++) = prefix;
			}
			/* Digits prior to the decimal point */
			if (e2 < 0) {
				*(bufpt++) = '0';
			} else {
				for (; e2 >= 0; e2--) {
					*(bufpt++) = et_getdigit(&realvalue, &nsd);
				}
			}
			/* The decimal point */
			if (flag_dp) {
				*(bufpt++) = '.';
			}
			/* "0" digits after the decimal point but before the first
			** significant digit of the number */
			for (e2++; e2 < 0; precision--, e2++) {
				assert(precision > 0);
				*(bufpt++) = '0';
			}
			/* Significant digits after the decimal point */
			while ((precision--) > 0) {
				*(bufpt++) = et_getdigit(&realvalue, &nsd);
			}
			/* Remove trailing zeros and the "." if no digits follow the "." */
			if (flag_rtz && flag_dp) {
				while (bufpt[-1] == '0')
					*(--bufpt) = 0;
				assert(bufpt > zOut);
				if (bufpt[-1] == '.') {
					if (flag_altform2) {
						*(bufpt++) = '0';
					} else {
						*(--bufpt) = 0;
					}
				}
			}
			/* Add the "eNNN" suffix */
			if (xtype == etEXP) {
				*(bufpt++) = aDigits[infop->charset];
				if (exp < 0) {
					*(bufpt++) = '-';
					exp = -exp;
				} else {
					*(bufpt++) = '+';
				}
				if (exp >= 100) {
					*(bufpt++) = (char)((exp / 100) + '0'); /* 100's digit */
					exp %= 100;
				}
				*(bufpt++) = (char)(exp / 10 + '0'); /* 10's digit */
				*(bufpt++) = (char)(exp % 10 + '0'); /* 1's digit */
			}
			*bufpt = 0;

			/* The converted number is in buf[] and zero terminated. Output it.
			** Note that the number is in the usual order, not reversed as with
			** integer conversions. */
			length = (int)(bufpt - zOut);
			bufpt = zOut;

			/* Special case:  Add leading zeros if the flag_zeropad flag is
			** set and we are not left justified */
			if (flag_zeropad && !flag_leftjustify && length < width) {
				int i;
				int nPad = width - length;
				for (i = width; i >= nPad; i--) {
					bufpt[i] = bufpt[i - nPad];
				}
				i = prefix != 0;
				while (nPad--)
					bufpt[i++] = '0';
				length = width;
			}
#endif /* !defined(SQLITE_OMIT_FLOATING_POINT) */
			break;
		case etSIZE:
			if (!bArgList) {
				*(va_arg(ap, int *)) = pAccum->nChar;
			}
			length = width = 0;
			break;
		case etPERCENT:
			buf[0] = '%';
			bufpt = buf;
			length = 1;
			break;
		case etCHARX:
			if (bArgList) {
				assert(0);

			} else {
				unsigned int ch = va_arg(ap, unsigned int);
				if (ch < 0x00080) {
					buf[0] = ch & 0xff;
					length = 1;
				} else if (ch < 0x00800) {
					buf[0] = 0xc0 + (u8)((ch >> 6) & 0x1f);
					buf[1] = 0x80 + (u8)(ch & 0x3f);
					length = 2;
				} else if (ch < 0x10000) {
					buf[0] = 0xe0 + (u8)((ch >> 12) & 0x0f);
					buf[1] = 0x80 + (u8)((ch >> 6) & 0x3f);
					buf[2] = 0x80 + (u8)(ch & 0x3f);
					length = 3;
				} else {
					buf[0] = 0xf0 + (u8)((ch >> 18) & 0x07);
					buf[1] = 0x80 + (u8)((ch >> 12) & 0x3f);
					buf[2] = 0x80 + (u8)((ch >> 6) & 0x3f);
					buf[3] = 0x80 + (u8)(ch & 0x3f);
					length = 4;
				}
			}
			if (precision > 1) {
				width -= precision - 1;
				if (width > 1 && !flag_leftjustify) {
					sqlite3AppendChar(pAccum, width - 1, ' ');
					width = 0;
				}
				while (precision-- > 1) {
					sqlite3StrAccumAppend(pAccum, buf, length);
				}
			}
			bufpt = buf;
			flag_altform2 = 1;
			goto adjust_width_for_utf8;
		case etSTRING:
		case etDYNSTRING:
			if (bArgList) {
				assert(0);
	
			} else {
				bufpt = va_arg(ap, char *);
			}
			if (bufpt == 0) {
				bufpt = "";
			} else if (xtype == etDYNSTRING) {
				zExtra = bufpt;
			}
			if (precision >= 0) {
				if (flag_altform2) {
					/* Set length to the number of bytes needed in order to display
					** precision characters */
					unsigned char *z = (unsigned char *)bufpt;
					while (precision-- > 0 && z[0]) {
						SQLITE_SKIP_UTF8(z);
					}
					length = (int)(z - (unsigned char *)bufpt);
				} else {
					for (length = 0; length < precision && bufpt[length]; length++) {
					}
				}
			} else {
				length = 0x7fffffff & (int)strlen(bufpt);
			}
		adjust_width_for_utf8:
			if (flag_altform2 && width > 0) {
				/* Adjust width to account for extra bytes in UTF-8 characters */
				int ii = length - 1;
				while (ii >= 0)
					if ((bufpt[ii--] & 0xc0) == 0x80)
						width++;
			}
			break;
		case etSQLESCAPE:    /* %q: Escape ' characters */
		case etSQLESCAPE2:   /* %Q: Escape ' and enclose in '...' */
		case etSQLESCAPE3: { /* %w: Escape " characters */
			int i, j, k, n, isnull;
			int needQuote;
			char ch;
			char q = ((xtype == etSQLESCAPE3) ? '"' : '\''); /* Quote character */
			char *escarg;

			if (bArgList) {
				assert(0);
			} else {
				escarg = va_arg(ap, char *);
			}
			isnull = escarg == 0;
			if (isnull)
				escarg = (xtype == etSQLESCAPE2 ? "NULL" : "(NULL)");
			/* For %q, %Q, and %w, the precision is the number of byte (or
			** characters if the ! flags is present) to use from the input.
			** Because of the extra quoting characters inserted, the number
			** of output characters may be larger than the precision.
			*/
			k = precision;
			for (i = n = 0; k != 0 && (ch = escarg[i]) != 0; i++, k--) {
				if (ch == q)
					n++;
				if (flag_altform2 && (ch & 0xc0) == 0xc0) {
					while ((escarg[i + 1] & 0xc0) == 0x80) {
						i++;
					}
				}
			}
			needQuote = !isnull && xtype == etSQLESCAPE2;
			n += i + 3;
			if (n > etBUFSIZE) {
				bufpt = zExtra = sqlite3Malloc(n);
				if (bufpt == 0) {
					setStrAccumError(pAccum, STRACCUM_NOMEM);
					return;
				}
			} else {
				bufpt = buf;
			}
			j = 0;
			if (needQuote)
				bufpt[j++] = q;
			k = i;
			for (i = 0; i < k; i++) {
				bufpt[j++] = ch = escarg[i];
				if (ch == q)
					bufpt[j++] = ch;
			}
			if (needQuote)
				bufpt[j++] = q;
			bufpt[j] = 0;
			length = j;
			goto adjust_width_for_utf8;
		}
		case etTOKEN: {
			assert(0);
			break;
		}
		case etSRCLIST: {
			assert(0);
			break;
		}
		default: {
			assert(xtype == etINVALID);
			return;
		}
		} /* End switch over the format type */
		/*
		** The text of the conversion is pointed to by "bufpt" and is
		** "length" characters long.  The field width is "width".  Do
		** the output.  Both length and width are in bytes, not characters,
		** at this point.  If the "!" flag was present on string conversions
		** indicating that width and precision should be expressed in characters,
		** then the values have been translated prior to reaching this point.
		*/
		width -= length;
		if (width > 0) {
			if (!flag_leftjustify)
				sqlite3AppendChar(pAccum, width, ' ');
			sqlite3StrAccumAppend(pAccum, bufpt, length);
			if (flag_leftjustify)
				sqlite3AppendChar(pAccum, width, ' ');
		} else {
			sqlite3StrAccumAppend(pAccum, bufpt, length);
		}

		if (zExtra) {
			free(zExtra);
			zExtra = 0;
		}
	} /* End for loop over the format string */
} /* End of function */

/*
** Enlarge the memory allocation on a StrAccum object so that it is
** able to accept at least N more bytes of text.
**
** Return the number of bytes of text that StrAccum is able to accept
** after the attempted enlargement.  The value returned might be zero.
*/
static int sqlite3StrAccumEnlarge(StrAccum *p, int N) {
	char *zNew;
	assert(p->nChar + (i64)N >= p->nAlloc); /* Only called if really needed */
	if (p->accError) {
		return 0;
	}
	if (p->mxAlloc == 0) {
		N = p->nAlloc - p->nChar - 1;
		setStrAccumError(p, STRACCUM_TOOBIG);
		return N;
	} else {
		char *zOld = isMalloced(p) ? p->zText : 0;
		i64 szNew = p->nChar;
		szNew += N + 1;
		if (szNew + p->nChar <= p->mxAlloc) {
			/* Force exponential buffer size growth as long as it does not overflow,
			** to avoid having to call this routine too often */
			szNew += p->nChar;
		}
		if (szNew > p->mxAlloc) {
			sqlite3StrAccumReset(p);
			setStrAccumError(p, STRACCUM_TOOBIG);
			return 0;
		} else {
			p->nAlloc = (int)szNew;
		}
		if (p->db) {
			assert(0);
		} else {
			zNew = sqlite3_realloc64(zOld, p->nAlloc);
		}
		if (zNew) {
			assert(p->zText != 0 || p->nChar == 0);
			if (!isMalloced(p) && p->nChar > 0)
				memcpy(zNew, p->zText, p->nChar);
			p->zText = zNew;
			p->printfFlags |= SQLITE_PRINTF_MALLOCED;
		} else {
			sqlite3StrAccumReset(p);
			setStrAccumError(p, STRACCUM_NOMEM);
			return 0;
		}
	}
	return N;
}

/*
** Append N copies of character c to the given string buffer.
*/
void sqlite3AppendChar(StrAccum *p, int N, char c) {
	if (p->nChar + (i64)N >= p->nAlloc && (N = sqlite3StrAccumEnlarge(p, N)) <= 0) {
		return;
	}
	while ((N--) > 0)
		p->zText[p->nChar++] = c;
}

/*
** The StrAccum "p" is not large enough to accept N new bytes of z[].
** So enlarge if first, then do the append.
**
** This is a helper routine to sqlite3StrAccumAppend() that does special-case
** work (enlarging the buffer) using tail recursion, so that the
** sqlite3StrAccumAppend() routine can use fast calling semantics.
*/
static void enlargeAndAppend(StrAccum *p, const char *z, int N) {
	N = sqlite3StrAccumEnlarge(p, N);
	if (N > 0) {
		memcpy(&p->zText[p->nChar], z, N);
		p->nChar += N;
	}
}

/*
** Append N bytes of text from z to the StrAccum object.  Increase the
** size of the memory allocation for StrAccum if necessary.
*/
void sqlite3StrAccumAppend(StrAccum *p, const char *z, int N) {
	assert(z != 0 || N == 0);
	assert(p->zText != 0 || p->nChar == 0 || p->accError);
	assert(N >= 0);
	assert(p->accError == 0 || p->nAlloc == 0);
	if (p->nChar + N >= p->nAlloc) {
		enlargeAndAppend(p, z, N);
	} else if (N) {
		assert(p->zText);
		p->nChar += N;
		memcpy(&p->zText[p->nChar - N], z, N);
	}
}

/*
** Append the complete text of zero-terminated string z[] to the p string.
*/
void sqlite3StrAccumAppendAll(StrAccum *p, const char *z) {
	sqlite3StrAccumAppend(p, z, strlen(z));
}

/*
** Finish off a string by making sure it is zero-terminated.
** Return a pointer to the resulting string.  Return a NULL
** pointer if any kind of error was encountered.
*/
static char *strAccumFinishRealloc(StrAccum *p) {
	char *zText;
	assert(p->mxAlloc > 0 && !isMalloced(p));
	zText = malloc(p->nChar + 1);
	if (zText) {
		memcpy(zText, p->zText, p->nChar + 1);
		p->printfFlags |= SQLITE_PRINTF_MALLOCED;
	} else {
		setStrAccumError(p, STRACCUM_NOMEM);
	}
	p->zText = zText;
	return zText;
}
char *sqlite3StrAccumFinish(StrAccum *p) {
	if (p->zText) {
		p->zText[p->nChar] = 0;
		if (p->mxAlloc > 0 && !isMalloced(p)) {
			return strAccumFinishRealloc(p);
		}
	}
	return p->zText;
}

/*
** Reset an StrAccum string.  Reclaim all malloced memory.
*/
void sqlite3StrAccumReset(StrAccum *p) {
	if (isMalloced(p)) {
		free(p->zText);
		p->printfFlags &= ~SQLITE_PRINTF_MALLOCED;
	}
	p->zText = 0;
}

/*
** Initialize a string accumulator.
**
** p:     The accumulator to be initialized.
** db:    Pointer to a database connection.  May be NULL.  Lookaside
**        memory is used if not NULL. db->mallocFailed is set appropriately
**        when not NULL.
** zBase: An initial buffer.  May be NULL in which case the initial buffer
**        is malloced.
** n:     Size of zBase in bytes.  If total space requirements never exceed
**        n then no memory allocations ever occur.
** mx:    Maximum number of bytes to accumulate.  If mx==0 then no memory
**        allocations will ever occur.
*/
void sqlite3StrAccumInit(StrAccum *p, void *db, char *zBase, int n, int mx) {
	p->zText = zBase;
	assert(!db);
	p->db = db;
	p->nAlloc = n;
	p->mxAlloc = mx;
	p->nChar = 0;
	p->accError = 0;
	p->printfFlags = 0;
}

/*
** Print into memory obtained from sqlite3_malloc().  Omit the internal
** %-conversion extensions.
*/
char *sqlite3_vmprintf(const char *zFormat, va_list ap) {
	char *z;
	char zBase[SQLITE_PRINT_BUF_SIZE];
	StrAccum acc;

	sqlite3StrAccumInit(&acc, 0, zBase, sizeof(zBase), SQLITE_MAX_LENGTH);
	sqlite3VXPrintf(&acc, zFormat, ap);
	z = sqlite3StrAccumFinish(&acc);
	return z;
}

/*
** Print into memory obtained from sqlite3_malloc()().  Omit the internal
** %-conversion extensions.
*/
char *sqlite3_mprintf(const char *zFormat, ...) {
	va_list ap;
	char *z;
	va_start(ap, zFormat);
	z = sqlite3_vmprintf(zFormat, ap);
	va_end(ap);
	return z;
}
