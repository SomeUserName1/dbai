/* Generated By:JJTree&JavaCC: Do not edit this line. QueryExpressionParserConstants.java */
package minibase.query.optimizer.parser;


/**
 * Token literal values and constants.
 * Generated by org.javacc.parser.OtherFilesGen#start()
 */
public interface QueryExpressionParserConstants {

  /** End of File. */
  int EOF = 0;
  /** RegularExpression Id. */
  int K_AS = 5;
  /** RegularExpression Id. */
  int K_DISTINCT = 6;
  /** RegularExpression Id. */
  int K_PROJECT = 7;
  /** RegularExpression Id. */
  int K_SELECT = 8;
  /** RegularExpression Id. */
  int K_EQJOIN = 9;
  /** RegularExpression Id. */
  int K_GET = 10;
  /** RegularExpression Id. */
  int K_ORDER_BY = 11;
  /** RegularExpression Id. */
  int K_AGG_LIST = 12;
  /** RegularExpression Id. */
  int K_GROUP_BY = 13;
  /** RegularExpression Id. */
  int K_OP_AND = 14;
  /** RegularExpression Id. */
  int K_OP_OR = 15;
  /** RegularExpression Id. */
  int K_OP_NOT = 16;
  /** RegularExpression Id. */
  int K_OP_EQ = 17;
  /** RegularExpression Id. */
  int K_OP_LT = 18;
  /** RegularExpression Id. */
  int K_OP_LEQ = 19;
  /** RegularExpression Id. */
  int K_OP_GT = 20;
  /** RegularExpression Id. */
  int K_OP_GEQ = 21;
  /** RegularExpression Id. */
  int K_OP_NEQ = 22;
  /** RegularExpression Id. */
  int K_OP_LIKE = 23;
  /** RegularExpression Id. */
  int K_OP_IN = 24;
  /** RegularExpression Id. */
  int K_ATTR = 25;
  /** RegularExpression Id. */
  int K_INT = 26;
  /** RegularExpression Id. */
  int K_FLOAT = 27;
  /** RegularExpression Id. */
  int K_STR = 28;
  /** RegularExpression Id. */
  int K_DATE = 29;
  /** RegularExpression Id. */
  int K_SEL = 30;
  /** RegularExpression Id. */
  int K_STRING = 31;
  /** RegularExpression Id. */
  int K_ASC = 32;
  /** RegularExpression Id. */
  int K_DESC = 33;
  /** RegularExpression Id. */
  int O_EQ = 34;
  /** RegularExpression Id. */
  int O_NEQ = 35;
  /** RegularExpression Id. */
  int O_GT = 36;
  /** RegularExpression Id. */
  int O_GTE = 37;
  /** RegularExpression Id. */
  int O_LT = 38;
  /** RegularExpression Id. */
  int O_LTE = 39;
  /** RegularExpression Id. */
  int O_LPAREN = 40;
  /** RegularExpression Id. */
  int O_RPAREN = 41;
  /** RegularExpression Id. */
  int O_COMMA = 42;
  /** RegularExpression Id. */
  int O_SEMI = 43;
  /** RegularExpression Id. */
  int O_STAR = 44;
  /** RegularExpression Id. */
  int O_DOT = 45;
  /** RegularExpression Id. */
  int S_INTEGER = 46;
  /** RegularExpression Id. */
  int DIGIT = 47;
  /** RegularExpression Id. */
  int S_FLOAT = 48;
  /** RegularExpression Id. */
  int S_IDENTIFIER = 49;
  /** RegularExpression Id. */
  int LETTER = 50;
  /** RegularExpression Id. */
  int SPECIAL_CHAR = 51;
  /** RegularExpression Id. */
  int S_STRING = 52;
  /** RegularExpression Id. */
  int S_DATE = 53;
  /** RegularExpression Id. */
  int LINE_COMMENT = 54;
  /** RegularExpression Id. */
  int MULTI_LINE_COMMENT = 55;

  /** Lexical state. */
  int DEFAULT = 0;

  /** Literal token values. */
  String[] tokenImage = {
    "<EOF>",
    "\" \"",
    "\"\\t\"",
    "\"\\n\"",
    "\"\\r\"",
    "\"AS\"",
    "\"DISTINCT\"",
    "\"PROJECT\"",
    "\"SELECT\"",
    "\"EQJOIN\"",
    "\"GET\"",
    "\"ORDER_BY\"",
    "\"AGG_LIST\"",
    "\"GROUP_BY\"",
    "\"OP_AND\"",
    "\"OP_OR\"",
    "\"OP_NOT\"",
    "\"OP_EQ\"",
    "\"OP_LT\"",
    "\"OP_LE\"",
    "\"OP_GT\"",
    "\"OP_GE\"",
    "\"OP_NE\"",
    "\"OP_LIKE\"",
    "\"OP_IN\"",
    "\"ATTR\"",
    "\"INT\"",
    "\"FLOAT\"",
    "\"STR\"",
    "\"DATE\"",
    "\"SEL\"",
    "\"STRING\"",
    "\"ASC\"",
    "\"DESC\"",
    "\"=\"",
    "\"<>\"",
    "\">\"",
    "\">=\"",
    "\"<\"",
    "\"<=\"",
    "\"(\"",
    "\")\"",
    "\",\"",
    "\";\"",
    "\"*\"",
    "\".\"",
    "<S_INTEGER>",
    "<DIGIT>",
    "<S_FLOAT>",
    "<S_IDENTIFIER>",
    "<LETTER>",
    "<SPECIAL_CHAR>",
    "<S_STRING>",
    "<S_DATE>",
    "<LINE_COMMENT>",
    "<MULTI_LINE_COMMENT>",
  };

}
