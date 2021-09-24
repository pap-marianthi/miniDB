from pyparsing import (
    Word,
    delimitedList,
    Optional,
    Group,
    alphas,
    alphanums,
    Forward,
    oneOf,
    quotedString,
    infixNotation,
    opAssoc,
    restOfLine,
    CaselessKeyword,
    ParserElement,
    pyparsing_common as ppc,
)

ParserElement.enablePackrat()

# define SQL tokens
selectStmt = Forward()
SELECT, FROM, WHERE, AND, OR, IN, IS, NOT, TOP, NULL, ORDER, BY, SAVE, AS, INNER, JOIN, ON = map(
    CaselessKeyword, "select from where and or in is not top null order by save as inner join on".split()
)
NOT_NULL = NOT + NULL
SAVE_AS = SAVE + AS
ORDER_BY = ORDER + BY
INNER_JOIN = INNER + JOIN

columnName = Word(alphas, alphanums + "_$").setName("column name")
columnName.addParseAction(ppc.upcaseTokens)
columnNameList = Group(delimitedList(columnName).setName("column_list"))

tableName = Word(alphas, alphanums + "_$").setName("table name")
tableName.addParseAction(ppc.upcaseTokens)

binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True).setName("binop")
realNum = ppc.real().setName("real number")
intNum = ppc.signed_integer()

columnRval = (
    realNum | intNum | quotedString | columnName
).setName("column_rvalue")

whereCondition = Group(
    (columnName + binop + columnRval)
    | (columnName + IN + Group("(" + delimitedList(columnRval).setName("in_values_list") + ")"))
    | (columnName + IN + Group("(" + selectStmt + ")"))
    | (columnName + IS + (NULL | NOT_NULL))
).setName("where_condition")

whereExpression = infixNotation(
    whereCondition,
    [
        (NOT, 1, opAssoc.RIGHT),
        (AND, 2, opAssoc.LEFT),
        (OR, 2, opAssoc.LEFT),
    ],
).setName("where_expression")

# define the grammar
selectStmt <<= (
    SELECT
    + ("*" | columnNameList)("columns")
    + FROM
    + tableName("table")
	+ Optional(INNER_JOIN + Word(alphanums) + ON + Word(alphanums), "")("inner_join")
    + Optional(Group(WHERE + whereExpression), "")("where")
	+ Optional(Group(TOP + Word(alphanums)), "")("top")
	+ Optional(ORDER_BY + Word(alphanums), "")("order_by")
	+ Optional((Word("ASC") | Word("DESC")), "")("asc")
	+ Optional(SAVE_AS + Word(alphanums), "")("save_as")
).setName("select_statement")

simpleSQL = selectStmt

# define Oracle comment format, and ignore them
oracleSqlComment = "--" + restOfLine
simpleSQL.ignore(oracleSqlComment)

if __name__ == "__main__":
    simpleSQL.runTests(
        """\
		# FAIL multiple tables
        Select A, B, C from Table1, Table2
        # FAIL - invalid SELECT keyword
        Xelect A, B, C from Sys.dual
        # FAIL - invalid FROM keyword
        Select A, B, C frox Sys.dual
        # FAIL - incomplete statement
        Select
        # FAIL - incomplete statement
        Select * from
        # FAIL - invalid column
        Select &&& frox Sys.dual
		# OK - Simple query
        SELECT * from XYZZY
        # OK - multiple columns
		Select A, B, C from Table1
        # OK - Where clause
        Select A from Table1 where a in ('RED','GREEN','BLUE')
        # OK - compound where clause
        Select A from Table1 where a in ('RED','GREEN','BLUE') and b in (10,20,30)
        # OK - where clause with comparison operator
        Select A,b from table1 where id1 eq id2
		# OK - Standard
		SELECT * FROM table WHERE field>100 TOP k ORDER BY column ASC SAVE AS table2
		# OK - Standard with join
		SELECT * FROM table1 INNER JOIN table2 ON condition WHERE field>100 TOP k ORDER BY column ASC SAVE AS table2
        """
    )