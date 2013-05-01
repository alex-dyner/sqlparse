# -*- coding: utf-8 -*-

from sqlparse.sql import Statement, Token
from sqlparse import tokens as T

import re

class StatementFilter:
    "Filter that split stream at individual statements"

    def __init__(self):
        self._in_dbldollar = False
        self._in_locking = False
        self._in_cursor = False

        self.block_stack = []

    def _reset(self):
        "Set the filter attributes to its default values"
        self._in_dbldollar = False
        self._in_locking = False
        self._in_cursor = False

        self.block_stack = []

    def _change_splitlevel(self, ttype, value):
        "Get the new split level (increase, decrease or remain equal)"


        BLOCK_MATCH = {'BEGIN'  :r'END$',
                       'CASE'   :r'END$',
                       'IF'     :r'END\s+IF$',
                       'FOR'    :r'END\s+FOR$',
                       'WHILE'  :r'END\s+WHILE$',
                       'REPEAT' :r'END\s+REPEAT$',
                       'LOOP'   :r'END\s+LOOP$',
                       '('      :r'\)$'}
        # PostgreSQL
        if (ttype == T.Name.Builtin
            and value.startswith('$') and value.endswith('$')):
            if self._in_dbldollar:
                self._in_dbldollar = False
                return -1
            else:
                self._in_dbldollar = True
                return 1
        elif self._in_dbldollar:
            return 0

        # ANSI
        if ttype not in T.Keyword and not ttype is T.Punctuation:
            return 0

        unified = value.upper()

        # Teradata's LOCKING ROW|TABLE|VIEW|DATABASE FOR ACCESS|READ|WRITE|EXCLUSIVE
        if unified in ('LOCK','LOCKING'):
            self._in_locking = True
            return 0

        if unified == 'FOR' and self._in_locking:
            return 0

        if unified in ('ACCESS','READ','WRITE','EXCLUSIVE') and self._in_locking:
            self._in_locking = False
            return 0

        # Teradata's CURSOR
        if unified == 'CURSOR':
            self._in_cursor = True
            return 0

        if unified == 'FOR' and self._in_cursor:
            return 0


        # Begin/end of the block
        if unified in BLOCK_MATCH:
            self.block_stack.append(unified)
            return 1

        if len(self.block_stack) > 0 and re.match(BLOCK_MATCH[self.block_stack[-1]],unified):
            self.block_stack.pop()
            return -1

        # Default
        return 0

    def process(self, stack, stream):
        "Process the stream"
        consume_ws = False
        splitlevel = 0
        stmt = None
        stmt_tokens = []

        # Run over all stream tokens
        for ttype, value in stream:
            # Yield token if we finished a statement and there's no whitespaces
            if consume_ws and ttype not in (T.Whitespace, T.Comment.Single):
                stmt.tokens = stmt_tokens
                yield stmt

                # Reset filter and prepare to process next statement
                self._reset()
                consume_ws = False
                splitlevel = 0
                stmt = None

            # Create a new statement if we are not currently in one of them
            if stmt is None:
                stmt = Statement()
                stmt_tokens = []

            # Change current split level (increase, decrease or remain equal)
            splitlevel += self._change_splitlevel(ttype, value)

            # Append the token to the current statement
            stmt_tokens.append(Token(ttype, value))

            # Check if we get the end of a statement
            if splitlevel <= 0 and ttype is T.Punctuation and value == ';':
                consume_ws = True

        # Yield pending statement (if any)
        if stmt is not None:
            stmt.tokens = stmt_tokens
            yield stmt
