SEL * FROM DBC.databasesv as d
inner join dbc.tablesv as t
on d.databasename = t.databasename
inner join dbc.columnsv as c
on t.databasename = c.databasename
and t.tablename = c.tablename
WHERE d.databasename = 'dbc'