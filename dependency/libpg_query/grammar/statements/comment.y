/*****************************************************************************
 *
 * COMMENT ON <object> IS <text>
 *
 *****************************************************************************/

CommentStmt:
			COMMENT ON object_type_any_name any_name IS comment_text
				{
					PGCommentStmt *n = makeNode(PGCommentStmt);

					n->objtype = $3;
					n->object = (PGNode *) $4;
					n->comment = $6;
					$$ = (PGNode *) n;
				}
			| COMMENT ON COLUMN any_name IS comment_text
				{
					PGCommentStmt *n = makeNode(PGCommentStmt);

					n->objtype = PG_OBJECT_COLUMN;
					n->object = (PGNode *) $4;
					n->comment = $6;
					$$ = (PGNode *) n;
				}
		;

/* object types taking any_name/any_name_list */
object_type_any_name:
			TABLE									{ $$ = PG_OBJECT_TABLE; }
			| SEQUENCE								{ $$ = PG_OBJECT_SEQUENCE; }
			| VIEW									{ $$ = PG_OBJECT_VIEW; }
			| MATERIALIZED VIEW						{ $$ = PG_OBJECT_MATVIEW; }
			| INDEX									{ $$ = PG_OBJECT_INDEX; }
			| FOREIGN TABLE							{ $$ = PG_OBJECT_FOREIGN_TABLE; }
			| COLLATION								{ $$ = PG_OBJECT_COLLATION; }
			| CONVERSION_P							{ $$ = PG_OBJECT_CONVERSION; }
			| STATISTICS							{ $$ = PG_OBJECT_STATISTIC_EXT; }
			| TEXT_P SEARCH PARSER					{ $$ = PG_OBJECT_TSPARSER; }
			| TEXT_P SEARCH DICTIONARY				{ $$ = PG_OBJECT_TSDICTIONARY; }
			| TEXT_P SEARCH TEMPLATE				{ $$ = PG_OBJECT_TSTEMPLATE; }
			| TEXT_P SEARCH CONFIGURATION			{ $$ = PG_OBJECT_TSCONFIGURATION; }
		;

comment_text:
			Sconst								{ $$ = $1; }
			| NULL_P							{ $$ = NULL; }
		;
