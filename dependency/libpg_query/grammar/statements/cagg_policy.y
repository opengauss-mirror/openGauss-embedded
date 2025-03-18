/*****************************************************************************
 *
 *		QUERY :
 *				CREATE CAGGPOLICY policy_name INTO table_name AS
 *
 *****************************************************************************/
CaggPolicyStmt: CREATE_P CAGGPOLICY policy_name INTO qualified_name AS subs_query_text 
			REFREASH_INTERVAL refreash_interval_text 
			OFFSET start_offset_text end_offset_text 
			BUCKET_WIDTH bucket_field_text bucket_width_text 
			INITTIME_P init_time_text
				{
					PGCaggPolicyStmt *n = makeNode(PGCaggPolicyStmt);

					n->policyname = $3;
					n->relation = $5;
					n->subs_query = $7;
					n->refreash_interval = $9;
					n->start_offset = $11;
					n->end_offset = $12;
					n->bucket_field = $14;
					n->bucket_width = $15;
					n->init_time = $17;
					$$ = (PGNode *) n;
				}
		| CREATE_P CAGGPOLICY policy_name INTO qualified_name AS subs_query_text 
			REFREASH_INTERVAL refreash_interval_text 
			OFFSET start_offset_text end_offset_text 
			BUCKET_WIDTH bucket_field_text bucket_width_text 
				{
					PGCaggPolicyStmt *n = makeNode(PGCaggPolicyStmt);

					n->policyname = $3;
					n->relation = $5;
					n->subs_query = $7;
					n->refreash_interval = $9;
					n->start_offset = $11;
					n->end_offset = $12;
					n->bucket_field = $14;
					n->bucket_width = $15;
					n->init_time = NULL;
					$$ = (PGNode *) n;
				}
		| CREATE_P CAGGPOLICY policy_name INTO qualified_name AS subs_query_text 
			REFREASH_INTERVAL refreash_interval_text 
			OFFSET start_offset_text end_offset_text 
				{
					PGCaggPolicyStmt *n = makeNode(PGCaggPolicyStmt);

					n->policyname = $3;
					n->relation = $5;
					n->subs_query = $7;
					n->refreash_interval = $9;
					n->start_offset = $11;
					n->end_offset = $12;
					n->bucket_field = NULL;
					n->bucket_width = NULL;
					n->init_time = NULL;
					$$ = (PGNode *) n;
				}
		| CREATE_P CAGGPOLICY policy_name INTO qualified_name AS subs_query_text 
			REFREASH_INTERVAL refreash_interval_text 
				{
					PGCaggPolicyStmt *n = makeNode(PGCaggPolicyStmt);

					n->policyname = $3;
					n->relation = $5;
					n->subs_query = $7;
					n->refreash_interval = $9;
					n->start_offset = NULL;
					n->end_offset = NULL;
					n->bucket_field = NULL;
					n->bucket_width = NULL;
					n->init_time = NULL;
					$$ = (PGNode *) n;
				}
		| CREATE_P CAGGPOLICY policy_name INTO qualified_name AS subs_query_text 
				{
					PGCaggPolicyStmt *n = makeNode(PGCaggPolicyStmt);

					n->policyname = $3;
					n->relation = $5;
					n->subs_query = $7;
					n->refreash_interval = NULL;
					n->start_offset = NULL;
					n->end_offset = NULL;
					n->bucket_field = NULL;
					n->bucket_width = NULL;
					n->init_time = NULL;
					$$ = (PGNode *) n;
				}
		;

policy_name: ColId									{ $$ = $1; };

subs_query_text:
			Sconst								{ $$ = $1; }
			| NULL_P							{ $$ = NULL; }
		;

refreash_interval_text:
			Sconst								{ $$ = $1; }
			| NULL_P							{ $$ = NULL; }
		;

start_offset_text:
			Sconst								{ $$ = $1; }
			| NULL_P							{ $$ = NULL; }
		;

end_offset_text:
			Sconst								{ $$ = $1; }
			| NULL_P							{ $$ = NULL; }
		;

bucket_field_text:
			Sconst								{ $$ = $1; }
			| NULL_P							{ $$ = NULL; }
		;

bucket_width_text:
			Sconst								{ $$ = $1; }
			| NULL_P							{ $$ = NULL; }
		;

init_time_text:
			Sconst								{ $$ = $1; }
			| NULL_P							{ $$ = NULL; }
		;
