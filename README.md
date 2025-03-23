# Duckdb dbase extension

> [!WARNING]
> This extension is not very well tested. Use at your own risk.

This extension is based on dbase-rs, and allows reading of dbf files using duckdb.

Example:

```
make configure
make debug
duckdb -unsigned

D load  './build/debug/extension/read_dbf/read_dbf.duckdb_extension';
D create view stations as select * from read_dbf('./test/data/stations.dbf');
D select * from stations limit 1;
┌─────────────────┬────────────┬────────────┬─────────┐
│      name       │ marker-col │ marker-sym │  line   │
│     varchar     │  varchar   │  varchar   │ varchar │
├─────────────────┼────────────┼────────────┼─────────┤
│ Van Dorn Street │ #0000ff    │ rail-metro │ blue    │
└─────────────────┴────────────┴────────────┴─────────┘
```

To do:
- [ ] prevent panic when referenced .dbf file does not exist.
