/*****************************************************************************
 *
 * Create a new Postgres DBMS role
 *
 *****************************************************************************/

CreateRoleStmt:
			CREATE_P ROLE RoleId opt_with OptRoleList
				{
					PGCreateRoleStmt *n = makeNode(PGCreateRoleStmt);

					n->stmt_type = ROLESTMT_ROLE;
					n->role = $3;
					n->options = $5;
					$$ = (PGNode *) n;
				}
		;

/*
 * Options for CREATE ROLE and ALTER ROLE (also used by CREATE/ALTER USER
 * for backwards compatibility).  Note: the only option required by SQL99
 * is "WITH ADMIN name".
 */
OptRoleList:
			OptRoleList CreateOptRoleElem			{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

AlterOptRoleList:
			AlterOptRoleList AlterOptRoleElem		{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

AlterOptRoleElem:
			PASSWORD Sconst
				{
					$$ = makeDefElem("password",
									 (PGNode *) makeString($2), @1);
				}
			| PASSWORD NULL_P
				{
					$$ = makeDefElem("password", NULL, @1);
				}
			| ENCRYPTED PASSWORD Sconst
				{
					/*
					 * These days, passwords are always stored in encrypted
					 * form, so there is no difference between PASSWORD and
					 * ENCRYPTED PASSWORD.
					 */
					$$ = makeDefElem("password",
									 (PGNode *) makeString($3), @1);
				}
			| UNENCRYPTED PASSWORD Sconst
				{
					ereport(ERROR,
							(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("UNENCRYPTED PASSWORD is no longer supported"),
							 errhint("Remove UNENCRYPTED to store the password in encrypted form instead."),
							 parser_errposition(@1)));
				}
			| INHERIT
				{
					$$ = makeDefElem("inherit", (PGNode *) makeInteger(true), @1);
				}
			| CONNECTION LIMIT SignedIconst
				{
					$$ = makeDefElem("connectionlimit", (PGNode *) makeInteger($3), @1);
				}
			| VALID UNTIL Sconst
				{
					$$ = makeDefElem("validUntil", (PGNode *) makeString($3), @1);
				}
		/*	Supported but not documented for roles, for use by ALTER GROUP. */
			| USER role_list
				{
					$$ = makeDefElem("rolemembers", (PGNode *) $2, @1);
				}
			| IDENT
				{
					/*
					 * We handle identifiers that aren't parser keywords with
					 * the following special-case codes, to avoid bloating the
					 * size of the main parser.
					 */
					if (strcmp($1, "superuser") == 0)
						$$ = makeDefElem("superuser", (PGNode *) makeInteger(true), @1);
					else if (strcmp($1, "nosuperuser") == 0)
						$$ = makeDefElem("superuser", (PGNode *) makeInteger(false), @1);
					else if (strcmp($1, "createrole") == 0)
						$$ = makeDefElem("createrole", (PGNode *) makeInteger(true), @1);
					else if (strcmp($1, "nocreaterole") == 0)
						$$ = makeDefElem("createrole", (PGNode *) makeInteger(false), @1);
					else if (strcmp($1, "replication") == 0)
						$$ = makeDefElem("isreplication", (PGNode *) makeInteger(true), @1);
					else if (strcmp($1, "noreplication") == 0)
						$$ = makeDefElem("isreplication", (PGNode *) makeInteger(false), @1);
					else if (strcmp($1, "createdb") == 0)
						$$ = makeDefElem("createdb", (PGNode *) makeInteger(true), @1);
					else if (strcmp($1, "nocreatedb") == 0)
						$$ = makeDefElem("createdb", (PGNode *) makeInteger(false), @1);
					else if (strcmp($1, "login") == 0)
						$$ = makeDefElem("canlogin", (PGNode *) makeInteger(true), @1);
					else if (strcmp($1, "nologin") == 0)
						$$ = makeDefElem("canlogin", (PGNode *) makeInteger(false), @1);
					else if (strcmp($1, "bypassrls") == 0)
						$$ = makeDefElem("bypassrls", (PGNode *) makeInteger(true), @1);
					else if (strcmp($1, "nobypassrls") == 0)
						$$ = makeDefElem("bypassrls", (PGNode *) makeInteger(false), @1);
					else if (strcmp($1, "noinherit") == 0)
					{
						/*
						 * Note that INHERIT is a keyword, so it's handled by main parser, but
						 * NOINHERIT is handled here.
						 */
						$$ = makeDefElem("inherit", (PGNode *) makeInteger(false), @1);
					}
					else
						ereport(ERROR,
								(errcode(PG_ERRCODE_SYNTAX_ERROR),
								 errmsg("unrecognized role option \"%s\"", $1),
									 parser_errposition(@1)));
				}
		;

CreateOptRoleElem:
			AlterOptRoleElem			{ $$ = $1; }
			/* The following are not supported by ALTER ROLE/USER/GROUP */
			| SYSID Iconst
				{
					$$ = makeDefElem("sysid", (PGNode *) makeInteger($2), @1);
				}
			| ADMIN role_list
				{
					$$ = makeDefElem("adminmembers", (PGNode *) $2, @1);
				}
			| ROLE role_list
				{
					$$ = makeDefElem("rolemembers", (PGNode *) $2, @1);
				}
			| IN_P ROLE role_list
				{
					$$ = makeDefElem("addroleto", (PGNode *) $3, @1);
				}
			| IN_P GROUP_P role_list
				{
					$$ = makeDefElem("addroleto", (PGNode *) $3, @1);
				}
		;

/* Role specifications */
RoleId:		RoleSpec
				{
					PGRoleSpec   *spc = (PGRoleSpec *) $1;

					switch (spc->roletype)
					{
						case ROLESPEC_CSTRING:
							$$ = spc->rolename;
							break;
						case ROLESPEC_PUBLIC:
							ereport(ERROR,
									(errcode(PG_ERRCODE_RESERVED_NAME),
									 errmsg("role name \"%s\" is reserved",
											"public"),
									 parser_errposition(@1)));
							break;
						case ROLESPEC_SESSION_USER:
							ereport(ERROR,
									(errcode(PG_ERRCODE_RESERVED_NAME),
									 errmsg("%s cannot be used as a role name here",
											"SESSION_USER"),
									 parser_errposition(@1)));
							break;
						case ROLESPEC_CURRENT_USER:
							ereport(ERROR,
									(errcode(PG_ERRCODE_RESERVED_NAME),
									 errmsg("%s cannot be used as a role name here",
											"CURRENT_USER"),
									 parser_errposition(@1)));
							break;
						case ROLESPEC_CURRENT_ROLE:
							ereport(ERROR,
									(errcode(PG_ERRCODE_RESERVED_NAME),
									 errmsg("%s cannot be used as a role name here",
											"CURRENT_ROLE"),
									 parser_errposition(@1)));
							break;
					}
				}
			;

RoleSpec:	NonReservedWord
				{
					/*
					 * "public" and "none" are not keywords, but they must
					 * be treated specially here.
					 */
					PGRoleSpec   *n;

					if (strcmp($1, "public") == 0)
					{
						n = (PGRoleSpec *) makeRoleSpec(ROLESPEC_PUBLIC, @1);
						n->roletype = ROLESPEC_PUBLIC;
					}
					else if (strcmp($1, "none") == 0)
					{
						ereport(ERROR,
								(errcode(PG_ERRCODE_RESERVED_NAME),
								 errmsg("role name \"%s\" is reserved",
										"none"),
								 parser_errposition(@1)));
					}
					else
					{
						n = makeRoleSpec(ROLESPEC_CSTRING, @1);
						n->rolename = pstrdup($1);
					}
					$$ = n;
				}
/***************************************************************************** conflicts
			| CURRENT_ROLE
				{
					$$ = makeRoleSpec(ROLESPEC_CURRENT_ROLE, @1);
				}
			| CURRENT_USER
				{
					$$ = makeRoleSpec(ROLESPEC_CURRENT_USER, @1);
				}
			| SESSION_USER
				{
					$$ = makeRoleSpec(ROLESPEC_SESSION_USER, @1);
				}
 *****************************************************************************/
		;

role_list:	RoleSpec
				{ $$ = list_make1($1); }
			| role_list ',' RoleSpec
				{ $$ = lappend($1, $3); }
		;

/*****************************************************************************
 *
 * Create a new Postgres DBMS user (role with implied login ability)
 *
 *****************************************************************************/

CreateUserStmt:
			CREATE_P USER RoleId opt_with OptRoleList
				{
					PGCreateRoleStmt *n = makeNode(PGCreateRoleStmt);

					n->stmt_type = ROLESTMT_USER;
					n->role = $3;
					n->options = $5;
					$$ = (PGNode *) n;
				}
		;


/*****************************************************************************
 *
 * Alter a postgresql DBMS role
 *
 *****************************************************************************/

AlterRoleStmt:
			ALTER ROLE RoleSpec opt_with AlterOptRoleList
				 {
					PGAlterRoleStmt *n = makeNode(PGAlterRoleStmt);

					n->role = $3;
					n->action = +1;	/* add, if there are members */
					n->options = $5;
					$$ = (PGNode *) n;
				 }
			| ALTER USER RoleSpec opt_with AlterOptRoleList
				 {
					PGAlterRoleStmt *n = makeNode(PGAlterRoleStmt);

					n->role = $3;
					n->action = +1;	/* add, if there are members */
					n->options = $5;
					$$ = (PGNode *) n;
				 }
		;

opt_in_database:
			   /* EMPTY */					{ $$ = NULL; }
			| IN_P DATABASE name	{ $$ = $3; }
		;

AlterRoleSetStmt:
			ALTER ROLE RoleSpec opt_in_database SetResetClause
				{
					PGAlterRoleSetStmt *n = makeNode(PGAlterRoleSetStmt);

					n->role = $3;
					n->database = $4;
					n->setstmt = $5;
					$$ = (PGNode *) n;
				}
			| ALTER ROLE ALL opt_in_database SetResetClause
				{
					PGAlterRoleSetStmt *n = makeNode(PGAlterRoleSetStmt);

					n->role = NULL;
					n->database = $4;
					n->setstmt = $5;
					$$ = (PGNode *) n;
				}
			| ALTER USER RoleSpec opt_in_database SetResetClause
				{
					PGAlterRoleSetStmt *n = makeNode(PGAlterRoleSetStmt);

					n->role = $3;
					n->database = $4;
					n->setstmt = $5;
					$$ = (PGNode *) n;
				}
			| ALTER USER ALL opt_in_database SetResetClause
				{
					PGAlterRoleSetStmt *n = makeNode(PGAlterRoleSetStmt);

					n->role = NULL;
					n->database = $4;
					n->setstmt = $5;
					$$ = (PGNode *) n;
				}
		;

/* SetResetClause allows SET or RESET without LOCAL */
SetResetClause:
			SET set_rest					{ $$ = $2; }
			| VariableResetStmt				{ $$ = (PGVariableSetStmt *) $1; }
		;


/*****************************************************************************
 *
 * Drop a postgresql DBMS role
 *
 * XXX Ideally this would have CASCADE/RESTRICT options, but a role
 * might own objects in multiple databases, and there is presently no way to
 * implement cascading to other databases.  So we always behave as RESTRICT.
 *****************************************************************************/

DropRoleStmt:
			DROP ROLE role_list
				{
					PGDropRoleStmt *n = makeNode(PGDropRoleStmt);

					n->missing_ok = false;
					n->roles = $3;
					n->stmt_type = ROLESTMT_ROLE;
					$$ = (PGNode *) n;
				}
			| DROP ROLE IF_P EXISTS role_list
				{
					PGDropRoleStmt *n = makeNode(PGDropRoleStmt);

					n->missing_ok = true;
					n->roles = $5;
					n->stmt_type = ROLESTMT_ROLE;
					$$ = (PGNode *) n;
				}
			| DROP USER role_list
				{
					PGDropRoleStmt *n = makeNode(PGDropRoleStmt);

					n->missing_ok = false;
					n->roles = $3;
					n->stmt_type = ROLESTMT_USER;
					$$ = (PGNode *) n;
				}
			| DROP USER IF_P EXISTS role_list
				{
					PGDropRoleStmt *n = makeNode(PGDropRoleStmt);

					n->roles = $5;
					n->missing_ok = true;
					n->stmt_type = ROLESTMT_USER;
					$$ = (PGNode *) n;
				}
			| DROP GROUP_P role_list
				{
					PGDropRoleStmt *n = makeNode(PGDropRoleStmt);

					n->missing_ok = false;
					n->roles = $3;
					n->stmt_type = ROLESTMT_GROUP;
					$$ = (PGNode *) n;
				}
			| DROP GROUP_P IF_P EXISTS role_list
				{
					PGDropRoleStmt *n = makeNode(PGDropRoleStmt);

					n->missing_ok = true;
					n->roles = $5;
					n->stmt_type = ROLESTMT_GROUP;
					$$ = (PGNode *) n;
				}
			;


/*****************************************************************************
 *
 * GRANT and REVOKE statements
 *
 *****************************************************************************/

GrantStmt:	GRANT privileges ON privilege_target TO grantee_list
			opt_grant_grant_option opt_granted_by
				{
					PGGrantStmt *n = makeNode(PGGrantStmt);

					n->is_grant = true;
					n->privileges = $2;
					n->targtype = ($4)->targtype;
					n->objtype = ($4)->objtype;
					n->objects = ($4)->objs;
					n->grantees = $6;
					n->grant_option = $7;
					n->grantor = $8;
					$$ = (PGNode *) n;
				}
		;

RevokeStmt:
			REVOKE privileges ON privilege_target
			FROM grantee_list opt_granted_by opt_drop_behavior
				{
					PGGrantStmt *n = makeNode(PGGrantStmt);

					n->is_grant = false;
					n->grant_option = false;
					n->privileges = $2;
					n->targtype = ($4)->targtype;
					n->objtype = ($4)->objtype;
					n->objects = ($4)->objs;
					n->grantees = $6;
					n->grantor = $7;
					n->behavior = $8;
					$$ = (PGNode *) n;
				}
			| REVOKE GRANT OPTION FOR privileges ON privilege_target
			FROM grantee_list opt_granted_by opt_drop_behavior
				{
					PGGrantStmt *n = makeNode(PGGrantStmt);

					n->is_grant = false;
					n->grant_option = true;
					n->privileges = $5;
					n->targtype = ($7)->targtype;
					n->objtype = ($7)->objtype;
					n->objects = ($7)->objs;
					n->grantees = $9;
					n->grantor = $10;
					n->behavior = $11;
					$$ = (PGNode *) n;
				}
		;


/*
 * Privilege names are represented as strings; the validity of the privilege
 * names gets checked at execution.  This is a bit annoying but we have little
 * choice because of the syntactic conflict with lists of role names in
 * GRANT/REVOKE.  What's more, we have to call out in the "privilege"
 * production any reserved keywords that need to be usable as privilege names.
 */

/* either ALL [PRIVILEGES] or a list of individual privileges */
privileges: privilege_list
				{ $$ = $1; }
			| ALL
				{ $$ = NIL; }
			| ALL PRIVILEGES
				{ $$ = NIL; }
			| ALL '(' columnList ')'
				{
					PGAccessPriv *n = makeNode(PGAccessPriv);

					n->priv_name = NULL;
					n->cols = $3;
					$$ = list_make1(n);
				}
			| ALL PRIVILEGES '(' columnList ')'
				{
					PGAccessPriv *n = makeNode(PGAccessPriv);

					n->priv_name = NULL;
					n->cols = $4;
					$$ = list_make1(n);
				}
		;

privilege_list:	privilege							{ $$ = list_make1($1); }
			| privilege_list ',' privilege			{ $$ = lappend($1, $3); }
		;

privilege:	SELECT opt_column_list
			{
				PGAccessPriv *n = makeNode(PGAccessPriv);

				n->priv_name = pstrdup($1);
				n->cols = $2;
				$$ = n;
			}
		| REFERENCES opt_column_list
			{
				PGAccessPriv *n = makeNode(PGAccessPriv);

				n->priv_name = pstrdup($1);
				n->cols = $2;
				$$ = n;
			}
		| CREATE_P opt_column_list
			{
				PGAccessPriv *n = makeNode(PGAccessPriv);

				n->priv_name = pstrdup($1);
				n->cols = $2;
				$$ = n;
			}
		| ALTER SYSTEM_P
			{
				PGAccessPriv *n = makeNode(PGAccessPriv);
				n->priv_name = pstrdup("alter system");
				n->cols = NIL;
				$$ = n;
			}
		| ColId opt_column_list
			{
				PGAccessPriv *n = makeNode(PGAccessPriv);

				n->priv_name = $1;
				n->cols = $2;
				$$ = n;
			}
		;

parameter_name_list:
		parameter_name
			{
				$$ = list_make1(makeString($1));
			}
		| parameter_name_list ',' parameter_name
			{
				$$ = lappend($1, makeString($3));
			}
		;

parameter_name:
		ColId
			{
				$$ = $1;
			}
		| parameter_name '.' ColId
			{
				$$ = psprintf("%s.%s", $1, $3);
			}
		;


/* Don't bother trying to fold the first two rules into one using
 * opt_table.  You're going to get conflicts.
 */
privilege_target:
			qualified_name_list
				{
					PGPrivTarget *n = (PGPrivTarget *) palloc(sizeof(PGPrivTarget));

					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = PG_OBJECT_TABLE;
					n->objs = $1;
					$$ = n;
				}
			| TABLE qualified_name_list
				{
					PGPrivTarget *n = (PGPrivTarget *) palloc(sizeof(PGPrivTarget));

					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = PG_OBJECT_TABLE;
					n->objs = $2;
					$$ = n;
				}
			| SEQUENCE qualified_name_list
				{
					PGPrivTarget *n = (PGPrivTarget *) palloc(sizeof(PGPrivTarget));

					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = PG_OBJECT_SEQUENCE;
					n->objs = $2;
					$$ = n;
				}
			| FOREIGN DATA_P WRAPPER name_list
				{
					PGPrivTarget *n = (PGPrivTarget *) palloc(sizeof(PGPrivTarget));

					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = PG_OBJECT_FDW;
					n->objs = $4;
					$$ = n;
				}
			| FOREIGN SERVER name_list
				{
					PGPrivTarget *n = (PGPrivTarget *) palloc(sizeof(PGPrivTarget));

					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = PG_OBJECT_FOREIGN_SERVER;
					n->objs = $3;
					$$ = n;
				}
			| DATABASE name_list
				{
					PGPrivTarget *n = (PGPrivTarget *) palloc(sizeof(PGPrivTarget));

					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = PG_OBJECT_DATABASE;
					n->objs = $2;
					$$ = n;
				}
			| DOMAIN_P any_name_list
				{
					PGPrivTarget *n = (PGPrivTarget *) palloc(sizeof(PGPrivTarget));

					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = PG_OBJECT_DOMAIN;
					n->objs = $2;
					$$ = n;
				}
			| LANGUAGE name_list
				{
					PGPrivTarget *n = (PGPrivTarget *) palloc(sizeof(PGPrivTarget));

					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = PG_OBJECT_LANGUAGE;
					n->objs = $2;
					$$ = n;
				}
			| SCHEMA name_list
				{
					PGPrivTarget *n = (PGPrivTarget *) palloc(sizeof(PGPrivTarget));

					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = PG_OBJECT_SCHEMA;
					n->objs = $2;
					$$ = n;
				}
			| TABLESPACE name_list
				{
					PGPrivTarget *n = (PGPrivTarget *) palloc(sizeof(PGPrivTarget));

					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = PG_OBJECT_TABLESPACE;
					n->objs = $2;
					$$ = n;
				}
			| TYPE_P any_name_list
				{
					PGPrivTarget *n = (PGPrivTarget *) palloc(sizeof(PGPrivTarget));

					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = PG_OBJECT_TYPE;
					n->objs = $2;
					$$ = n;
				}
			| ALL TABLES IN_P SCHEMA name_list
				{
					PGPrivTarget *n = (PGPrivTarget *) palloc(sizeof(PGPrivTarget));

					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = PG_OBJECT_TABLE;
					n->objs = $5;
					$$ = n;
				}
			| ALL SEQUENCES IN_P SCHEMA name_list
				{
					PGPrivTarget *n = (PGPrivTarget *) palloc(sizeof(PGPrivTarget));

					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = PG_OBJECT_SEQUENCE;
					n->objs = $5;
					$$ = n;
				}
			| ALL FUNCTIONS IN_P SCHEMA name_list
				{
					PGPrivTarget *n = (PGPrivTarget *) palloc(sizeof(PGPrivTarget));

					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = PG_OBJECT_FUNCTION;
					n->objs = $5;
					$$ = n;
				}
		;


grantee_list:
			grantee									{ $$ = list_make1($1); }
			| grantee_list ',' grantee				{ $$ = lappend($1, $3); }
		;

grantee:
			RoleSpec								{ $$ = $1; }
			| GROUP_P RoleSpec						{ $$ = $2; }
		;


opt_grant_grant_option:
			WITH GRANT OPTION { $$ = true; }
			| /*EMPTY*/ { $$ = false; }
		;

/*****************************************************************************
 *
 * GRANT and REVOKE ROLE statements
 *
 *****************************************************************************/

GrantRoleStmt:
			GRANT privilege_list TO role_list opt_granted_by
				{
					PGGrantRoleStmt *n = makeNode(PGGrantRoleStmt);

					n->is_grant = true;
					n->granted_roles = $2;
					n->grantee_roles = $4;
					n->opt = NIL;
					n->grantor = $5;
					$$ = (PGNode *) n;
				}
		  | GRANT privilege_list TO role_list WITH grant_role_opt_list opt_granted_by
				{
					PGGrantRoleStmt *n = makeNode(PGGrantRoleStmt);

					n->is_grant = true;
					n->granted_roles = $2;
					n->grantee_roles = $4;
					n->opt = $6;
					n->grantor = $7;
					$$ = (PGNode *) n;
				}
		;

RevokeRoleStmt:
			REVOKE privilege_list FROM role_list opt_granted_by opt_drop_behavior
				{
					PGGrantRoleStmt *n = makeNode(PGGrantRoleStmt);

					n->is_grant = false;
					n->opt = NIL;
					n->granted_roles = $2;
					n->grantee_roles = $4;
					n->grantor = $5;
					n->behavior = $6;
					$$ = (PGNode *) n;
				}
			| REVOKE ColId OPTION FOR privilege_list FROM role_list opt_granted_by opt_drop_behavior
				{
					PGGrantRoleStmt *n = makeNode(PGGrantRoleStmt);
					PGDefElem *opt;

					opt = makeDefElem(pstrdup($2),
									  (PGNode *) makeInteger(false), @2);
					n->is_grant = false;
					n->opt = list_make1(opt);
					n->granted_roles = $5;
					n->grantee_roles = $7;
					n->grantor = $8;
					n->behavior = $9;
					$$ = (PGNode *) n;
				}
		;

grant_role_opt_list:
			grant_role_opt_list ',' grant_role_opt	{ $$ = lappend($1, $3); }
			| grant_role_opt						{ $$ = list_make1($1); }
		;

grant_role_opt:
		ColLabel grant_role_opt_value
			{
				$$ = makeDefElem(pstrdup($1), $2, @1);
			}
		;

grant_role_opt_value:
		OPTION			{ $$ = (PGNode *) makeInteger(true); }
		| TRUE_P		{ $$ = (PGNode *) makeInteger(true); }
		| FALSE_P		{ $$ = (PGNode *) makeInteger(false); }
		;

opt_granted_by: GRANTED BY RoleSpec						{ $$ = $3; }
			| /*EMPTY*/									{ $$ = NULL; }
		;
