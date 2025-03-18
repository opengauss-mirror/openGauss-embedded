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
#include "include/intarkdb.h"
#include "include/sqlite3.h"
#include "interface/c/intarkdb_sql.h"
#include "storage/gstor/zekernel/common/cm_defs.h"


#include <cassert>
#include <chrono>
#include <climits>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <string>
#include <thread>


#define DATA_BASE_LIST_SQL "PRAGMA database_list;"
#define PRAGMA_SQL_STR "PRAGMA "

using namespace std;

#ifdef _MSC_VER
#define SQLITE_INT_TO_PTR(X) ((void *)(uintptr_t)(X))
#define SQLITE_PTR_TO_INT(X) ((int)(intptr_t)(X))
#else
#define SQLITE_INT_TO_PTR(X) ((void *)(__PTRDIFF_TYPE__)(X))
#define SQLITE_PTR_TO_INT(X) ((int)(__PTRDIFF_TYPE__)(X))
#endif
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
	int32_t blob_len;
	
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

	// char* str;
	std::string str;
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

int sqlite3_open(const char *filename, /* Database filename (UTF- 8) */
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
int sqlite3_close_v2(sqlite3 *db) {

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
				// const char* errmsg = sqlite3_errmsg(db);
				// *pzErrMsg = errmsg ? sqlite3_strdup(errmsg) : NULL;
				*pzErrMsg = pStmt->result->msg ? sqlite3_strdup(pStmt->result->msg) : NULL;
			}
			continue;
		}
		if (!pStmt) {
			/* this happens for a comment or white-space */
			zSql = zLeftover;
			continue;
		}

		// nCol = sqlite3_column_count(pStmt);
		// azCols = (char **)malloc(nCol * sizeof(const char *));
		// azVals = (char **)malloc(nCol * sizeof(const char *));
		// if (!azCols || !azVals) {
		// 	goto exec_out;
		// }
		// for (int i = 0; i < nCol; i++) {
		// 	azCols[i] = (char *)sqlite3_column_name(pStmt, i);
		// }

		while (true) {
			rc = sqlite3_step(pStmt);
			int i;
			
			/* Invoke the callback function if required */
			if (xCallback && rc == SQLITE_ROW) {
				nCol = sqlite3_column_count(pStmt);
				azCols = (char **)malloc(nCol * sizeof(const char *));
				azVals = (char **)malloc(nCol * sizeof(const char *));
				 
				if (!azCols || !azVals) {
					goto exec_out;
				}
				for (i = 0; i < nCol; i++) {
					azCols[i] = (char *)sqlite3_column_name(pStmt, i);
				}
				for ( i = 0; i < nCol; i++) {
					int val_len = strlen((char *)sqlite3_column_text(pStmt, i));
					azVals[i] = (char *)malloc(val_len+1);
					strcpy(azVals[i],(char *)sqlite3_column_text(pStmt, i));
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
					// const char* errmsg = sqlite3_errmsg(db);
					// *pzErrMsg = errmsg ? sqlite3_strdup(errmsg) : NULL;
					*pzErrMsg = pStmt->result->msg ? sqlite3_strdup(pStmt->result->msg) : NULL;
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
	int count  = intarkdb_column_count(pStmt->result);
	return  count;
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
		case GS_TYPE_REAL:
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
	return intarkdb_value_blob(stmt->result, stmt->current_row, iCol,&(stmt->blob_len));
	// return (void *)intarkdb_value_varchar(stmt->result, stmt->current_row, iCol);
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

#include <vector>
SQLITE_API int sqlite3_bind_zeroblob(sqlite3_stmt *stmt, int idx, int length) {

	if(length < 0)
		length =0;
	std::vector<char> blob_data(length, 0x00);

	return sqlite3_bind_blob(stmt, idx, blob_data.data(), blob_data.size(),SQLITE_STATIC);
}

SQLITE_API int sqlite3_clear_bindings(sqlite3_stmt *pStmt){
	if (!pStmt) {
		return SQLITE_MISUSE;
	}
	int rc = SQLITE_ERROR;
	rc = intarkdb_clear_bindings(pStmt->stmt);
	return rc;
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
	if( pVoid==0 ) return;
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
	return (sqlite3_int64)intarkdb_last_insert_rowid(db->db);
}

// length of varchar or blob value
// int sqlite3_column_bytes(sqlite3_stmt *stmt, int iCol) {
// 	if (!stmt || iCol < 0 || !stmt->result) {
// 		return 0;
// 	}
// 	return strlen((const char*)sqlite3_column_text(stmt, iCol));
// }
int sqlite3_column_bytes(sqlite3_stmt *stmt, int iCol) {

	
	const void *data = sqlite3_column_blob(stmt, iCol);
    if (data != NULL) {
        return stmt->blob_len;
    } else {
        // 如果是 NULL 值,则检查是否为文本类型
        const char *text = (const char *)sqlite3_column_text(stmt, iCol);
        if (text != NULL) {
            return strlen(text);
        } else {
            // 如果既不是 BLOB 也不是 TEXT,则返回 0
            return 0;
        }
    }
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
	if (!stmt || !stmt->result) {
		return nullptr;
	}
	// char* val = intarkdb_column_value(stmt->result,iCol);
	// sqlite3_value  *value = (sqlite3_value  *)malloc(sizeof(sqlite3_value));
	// return nullptr;

}

int sqlite3_db_config(sqlite3 *db, int op, ...) {
	fprintf(stderr, "sqlite3_db_config: unsupported.\n");
	return -1;
}

int sqlite3_get_autocommit(sqlite3 *db) {
	
 	return intarkdb_is_autocommit(&(db->conn));
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

SQLITE_API const char *sqlite3_sql(sqlite3_stmt *pStmt){
	if(!pStmt || !pStmt->stmt) return nullptr;
	return intarkdb_sql(pStmt->stmt);
}

SQLITE_API char *sqlite3_expanded_sql(sqlite3_stmt *pStmt){
	if(!pStmt || !pStmt->stmt) return nullptr;
	// reference : https://www.sqlite.org/c3ref/expanded_sql.html
	// the result of sqlite3_expaned_sql should be freed by sqlite3_free
	char* expanded_sql = intarkdb_expanded_sql(pStmt->stmt);
	char* buff = sqlite3_strdup(expanded_sql);
	return buff;
}

SQLITE_API int sqlite3_stmt_readonly(sqlite3_stmt *pStmt){
	if(!pStmt || !pStmt->stmt) return 0;
	return intarkdb_prepare_is_select(pStmt->stmt);

}
SQLITE_API void sqlite3_result_error(sqlite3_context *context, const char *msg, int n_bytes) {
	context->isError = SQL_ERROR;
	sqlite3_result_text(context, msg, n_bytes, NULL);
}

SQLITE_API void sqlite3_result_int(sqlite3_context *context, int val) {
	sqlite3_result_int64(context, (int64_t)val);
}

SQLITE_API void sqlite3_result_int64(sqlite3_context *context, sqlite3_int64 val) {
	context->result.u.i = val;
	context->result.type = SQLITE_INTEGER;
}

SQLITE_API void sqlite3_result_null(sqlite3_context *context) {
	context->result.type = SQLITE_NULL;
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

	return (const unsigned char*)val->str.c_str();
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
	// fprintf(stderr, "sqlite3_threadsafe: unsupported.\n");
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
// char *sqlite3_vsnprintf(int n, char *zBuf, const char *zFormat, va_list ap){
// 	if( n<=0 ) return zBuf;
// 	vsnprintf(zBuf, n, zFormat, ap);
// 	return zBuf;
// }

// char *sqlite3_snprintf(int n, char *zBuf, const char *zFormat, ...){
// 	va_list ap;
// 	if( n<=0 ) return zBuf;
// 	va_start(ap, zFormat);
// 	sqlite3_vsnprintf(n, zBuf, zFormat, ap);
// 	va_end(ap);
// 	return zBuf;
// }

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

// SQLITE_API void sqlite3StrAccumAppend(StrAccum *p, const char *z, int N);
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
** Reset an StrAccum string.  Reclaim all malloced memory.
*/
void sqlite3StrAccumReset(StrAccum *p) {
	if (isMalloced(p)) {
		free(p->zText);
		p->printfFlags &= ~SQLITE_PRINTF_MALLOCED;
	}
	p->zText = 0;
}

// #ifdef  ___test
SQLITE_API const char *sqlite3_libversion(void){
	 return get_version(); 
}

SQLITE_API int sqlite3_libversion_number(void){
	return SQLITE_VERSION_NUMBER;
}

double sqlite3_value_double(sqlite3_value *pVal) {
	if (!pVal) {
			pVal->db->errCode = SQLITE_MISUSE;
			return 0.0;
		}
	double res;
	switch (pVal->type) {
		case SQLITE_FLOAT:
			return pVal->u.r;
		case SQLITE_INTEGER:
			return (double)pVal->u.i;
		case SQLITE_TEXT:
		case SQLITE_BLOB:
			try{
				res =  std::stod(pVal->str);
			}
			catch(const std::exception& e){
				break;
			}
			return res;
		default:
			break;
		}
	pVal->db->errCode = SQLITE_MISMATCH;
	return 0.0;
}

int sqlite3_value_int(sqlite3_value *pVal) {
	int64_t res = sqlite3_value_int64(pVal);
	if (res >= INT_MIN  && res <= INT_MAX) {
		return res;
	}
	pVal->db->errCode = SQLITE_MISMATCH;
	return 0;
}

sqlite3_int64 sqlite3_value_int64(sqlite3_value *pVal) {
	if (!pVal) {
		pVal->db->errCode = SQLITE_MISUSE;
		return 0;
	}
	int64_t res;
	switch (pVal->type) {
	case SQLITE_INTEGER:
		return pVal->u.i;
	case SQLITE_FLOAT:
		return (int64_t)pVal->u.r;
		break;
	case SQLITE_TEXT:
	case SQLITE_BLOB:
		try{
			res =  std::stoi(pVal->str);
		}
		catch(const std::exception& e){
			break;
		}
		return res;
		// char* endPtr;
		// int64_t result = strtoll(pVal->str, &endPtr, 10);
		// if(endPtr == '\0'){
		// 	return result;
		// }
		
	default:
		break;
	}
	pVal->db->errCode = SQLITE_MISMATCH;
	return 0;
}
SQLITE_API int sqlite3_value_bytes(sqlite3_value *pVal) {
	if (pVal->type == SQLITE_TEXT || pVal->type == SQLITE_BLOB) {
		// return strlen(pVal->str);
		return pVal->str.size();
	}
	return 0;
}

SQLITE_API int sqlite3_value_type(sqlite3_value *pVal) {
	return (int)pVal->type;
}

SQLITE_API int sqlite3_value_numeric_type(sqlite3_value *pVal) {
	return 0;
}

SQLITE_API void sqlite3_result_blob(sqlite3_context *context, const void *blob, int n_bytes, void (*xDel)(void *)) {
	sqlite3_result_blob64(context, blob, n_bytes, xDel);
}

SQLITE_API void sqlite3_result_blob64(sqlite3_context *context, const void *blob, sqlite3_uint64 n_bytes,
                                      void (*xDel)(void *)) {
	if (!blob) {
		context->isError = SQLITE_MISUSE;
		return;
	}
	context->result.type = SQLITE_BLOB;
	context->result.str = string((char *)blob, n_bytes);
	if (xDel && xDel != SQLITE_TRANSIENT) {
		xDel((void *)blob);
	}
}

int sqlite3_initialize(void) {
	return SQLITE_OK;
}
SQLITE_API sqlite3_context *sqlite3_create_context(sqlite3 *db, int nParam, int nArg, void *(*xPrint)(void*, const char*)) {
  if (!db) {
	// 如果 db 为 NULL，抛出错误
    return nullptr;
  }

  // 创建一个新的 sqlite3_context 结构体
  sqlite3_context *context = (sqlite3_context *)sqlite3Malloc(sizeof(sqlite3_context));
  if (!context) {
    // 如果内存分配失败，抛出错误
	return nullptr;
}

  // 初始化新创建的 context 结构体
  context->result.db = db;
//   context->nParam = nParam;
//   context->nArg = nArg;
//   context->xPrint = xPrint;

  // 返回新创建的 context 结构体
  return context;
}

SQLITE_API sqlite3 *sqlite3_context_db_handle(sqlite3_context *context) {
	if(!context || !context->result.db) return nullptr;
	return context->result.db;
}
SQLITE_API const char *sqlite3_db_filename(sqlite3 *db, const char *zDbName)
{
	return intarkdb_db_path(db->db);
}
SQLITE_API int sqlite3_enable_shared_cache(int enable){
	return SQLITE_OK;
}
SQLITE_API int sqlite3_db_release_memory(sqlite3 *db){
	return SQLITE_OK;
}
SQLITE_API int sqlite3_extended_result_codes(sqlite3 *db, int onoff){
	return SQLITE_ERROR;
}
SQLITE_API int sqlite3_complete(const char *zSql){
	return true;
}
// SQLITE_API int sqlite3_busy_timeout(sqlite3 *db, int ms){
// 	return SQLITE_OK;
// }

SQLITE_API int sqlite3_bind_parameter_count(sqlite3_stmt *pStmt){
	return intarkdb_prepare_nparam(pStmt->stmt);
}


int sqlite3Strlen30(const char *z){
  if( z==0 ) return 0;
  return 0x3fffffff & (int)strlen(z);
}
/*
** This routine frees the space the sqlite3_get_table() malloced.
*/
SQLITE_API void sqlite3_free_table(
  char **azResult            /* Result returned from sqlite3_get_table() */
){
  if( azResult ){
    int i, n;
    azResult--;
    assert( azResult!=0 );
    n = SQLITE_PTR_TO_INT(azResult[0]);
    for(i=1; i<n; i++){ if( azResult[i] ) sqlite3_free(azResult[i]); }
    sqlite3_free(azResult);
  } 
}

SQLITE_API int sqlite3_prepare_v3(
  sqlite3 *db,              /* Database handle. */
  const char *zSql,         /* UTF-8 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  unsigned int prepFlags,   /* Zero or more SQLITE_PREPARE_* flags */
  sqlite3_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const char **pzTail       /* OUT: End of parsed string */
){
	return sqlite3_prepare_v2(db,zSql,nBytes,ppStmt,pzTail);
}


extern "C" {
/*
** This structure is used to pass data from sqlite3_get_table() through
** to the callback function is uses to build the result.
*/
typedef struct TabResult {
  char **azResult;   /* Accumulated output */
  char *zErrMsg;     /* Error message text, if an error occurs */
  u32 nAlloc;        /* Slots allocated for azResult[] */
  u32 nRow;          /* Number of rows in the result */
  u32 nColumn;       /* Number of columns in the result */
  u32 nData;         /* Slots used in azResult[].  (nRow+1)*nColumn */
  int rc;            /* Return code from sqlite3_exec() */
} TabResult;

void * sqlite3Realloc(char* ptr, int nAlloc) {
	void *azNew;
    azNew = realloc(ptr, nAlloc);
    if (!azNew) {
        return NULL; /* Failed to allocate new array */
    }
    return azNew;
}

static int sqlite3_get_table_cb(void *pArg, int nCol, char **argv, char **colv){
  TabResult *p = (TabResult*)pArg;  /* Result accumulator */
  int need;                         /* Slots needed in p->azResult[] */
  int i;                            /* Loop counter */
  char *z;                          /* A single column of result */

  /* Make sure there is enough space in p->azResult to hold everything
  ** we need to remember from this invocation of the callback.
  */
  if( p->nRow==0 && argv!=0 ){
    need = nCol*2;
  }else{
    need = nCol;
  }
  if( p->nData + need > p->nAlloc ){
    char **azNew;
    p->nAlloc = p->nAlloc*2 + need;
    azNew = (char **)realloc( p->azResult, sizeof(char*)*p->nAlloc );
    if( azNew==0 ) goto malloc_failed;
    p->azResult = azNew;
  }

  /* If this is the first row, then generate an extra row containing
  ** the names of all columns.
  */
  if( p->nRow==0 ){
    p->nColumn = nCol;
    for(i=0; i<nCol; i++){
      z = sqlite3_mprintf("%s", colv[i]);
      if( z==0 ) goto malloc_failed;
      p->azResult[p->nData++] = z;
    }
  }else if( (int)p->nColumn!=nCol ){
    sqlite3_free(p->zErrMsg);
    p->zErrMsg = sqlite3_mprintf(
       "sqlite3_get_table() called with two or more incompatible queries"
    );
    p->rc = SQLITE_ERROR;
    return 1;
  }

  /* Copy over the row data
  */
  if( argv!=0 ){
    for(i=0; i<nCol; i++){
      if( argv[i]==0 ){
        z = 0;
      }else{
        int n = sqlite3Strlen30(argv[i])+1;
        z = (char *)malloc( n );
        if( z==0 ) goto malloc_failed;
        memcpy(z, argv[i], n);
		if(argv[i])
			free(argv[i]);
      }
      p->azResult[p->nData++] = z;
    }
    p->nRow++;
  }
  return 0;

malloc_failed:
  p->rc = SQLITE_NOMEM_BKPT;
  return 1;
}



SQLITE_API int sqlite3_get_table(
  sqlite3 *db,                /* The database on which the SQL executes */
  const char *zSql,           /* The SQL to be executed */
  char ***pazResult,          /* Write the result table here */
  int *pnRow,                 /* Write the number of rows in the result here */
  int *pnColumn,              /* Write the number of columns of result here */
  char **pzErrMsg             /* Write error messages here */
){
  int rc;
  TabResult res;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlite3SafetyCheckOk(db) || pazResult==0 ) return SQLITE_MISUSE_BKPT;
#endif
  *pazResult = 0;
  if( pnColumn ) *pnColumn = 0;
  if( pnRow ) *pnRow = 0;
  if( pzErrMsg ) *pzErrMsg = 0;
  res.zErrMsg = 0;
  res.nRow = 0;
  res.nColumn = 0;
  res.nData = 1;
  res.nAlloc = 20;
  res.rc = SQLITE_OK;
  res.azResult = (char **)malloc(sizeof(char*)*res.nAlloc );
  if( res.azResult==0 ){
     db->errCode = SQLITE_NOMEM;
     return SQLITE_NOMEM_BKPT;
  }
  res.azResult[0] = 0;
  rc = sqlite3_exec(db, zSql, sqlite3_get_table_cb, &res, pzErrMsg);
  assert( sizeof(res.azResult[0])>= sizeof(res.nData) );
  res.azResult[0] = (char *)SQLITE_INT_TO_PTR(res.nData);
  if( (rc&0xff)==SQLITE_ABORT ){
    sqlite3_free_table(&res.azResult[1]);
    if( res.zErrMsg ){
      if( pzErrMsg ){
        sqlite3_free(*pzErrMsg);
        *pzErrMsg = sqlite3_mprintf("%s",res.zErrMsg);
      }
      sqlite3_free(res.zErrMsg);
    }
    db->errCode = res.rc;  /* Assume 32-bit assignment is atomic */
    return res.rc;
  }
  sqlite3_free(res.zErrMsg);
  if( rc!=SQLITE_OK ){
    sqlite3_free_table(&res.azResult[1]);
    return rc;
  }
  if( res.nAlloc>res.nData ){
    char **azNew;
    azNew = (char **)realloc( (char *)res.azResult, sizeof(char*)*res.nData );
    if( azNew==0 ){
      sqlite3_free_table(&res.azResult[1]);
      db->errCode = SQLITE_NOMEM;
      return SQLITE_NOMEM_BKPT;
    }
    res.azResult = azNew;
  }
  *pazResult = &res.azResult[1];
  if( pnColumn ) *pnColumn = res.nColumn;
  if( pnRow ) *pnRow = res.nRow;
  return rc;
}

}
// #endif