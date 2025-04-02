/*****************************************************************************
 *
 *	QUERY:
 *		CREATE SYNONYM <synonymname> FOR <objectname>
 *
 *****************************************************************************/
SynonymStmt: CREATE_P SYNONYM qualified_name FOR qualified_name 
				{
					PGSynonymStmt *n = makeNode(PGSynonymStmt);
					n->synonym = $3;
					n->table = $5;
					$$ = (PGNode *) n;
				}
		;
