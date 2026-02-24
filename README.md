Hello world
//elmelteti leiras + felhasznali doku + fejlesztesi doku (tesztelesi doksi + vhol template)

# Notes
- eventual consistency -> Leader only GET, no VerfiyLeader()
- strong consistency -> Leader only GET, VerifyLeader()
- dirty reads -> Leader, follower GET, no VerifyLeader()

# TODO
- [ ] fix cluster tests
- [ ] simplify http server handlers
- [ ] Transactions
- [ ] BatchingFSM
- [ ] lease