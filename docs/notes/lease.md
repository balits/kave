# 1. Bérlet fogalma
- miért, és hogyan
    1 - ideiglenes adatok, szolgáltatás regisztráció
    2 - elosztott zárak: kliens összeomlásával azonnal törlődnek a kulcsai
    3 - kliens feladata a KeepAlive
        szerver feladata a Checkpoint

# 2. Bérletkezelő
    1 - leaseMap
    2 - backend
    3 - leaseHeap (peek: min O(1), törlés: O(log N))
# 3. Elosztott kezelés: Lejarát Ciklus
probléma:
    ha minden gép (követők is) a saját helyi órája alapján törölne bérleteket, az órák csúszása (clock drift) vagy a hálózati késések miatt az állapotgépek (FSM) divergálnának (szétcsúsznának).
megoldás:
    csak a vezető dönti el, lejárt-e egy bérlet. ilyenkor a minimum kupacból kiszedi a bérleteket, akiknek lejárt a TTL-e, és egy LeaseExpire parancsot javasol a Raft klaszternek. Amint a naplóbejegyzés replikálódik, minden gép determinisztikusan törli a bérletet, és a hozzá tartozó kulcsokat.
# 3. Hibatűrés: KeepAlive, Checkpoint
két irányú szinkronizáció:
    1 - KeepAlive: kliens -> server
    2 - Checkpoint: szerver -> server
        10 perces bérlet után 9 perc után összeomlik a vezető, honnan tudjuk hogy csak 1 perc van hátra?
        -> időközönként beírjuk a naplóba a hátralévő TTL-t,
# 4. Restore
    Miután a Bérletkezelő megkapja az adatbázis korábbi pillanatképét, végigmegy a teljes \texttt{\/lease} vödrön, és feltöltjük a bérlet halmazt, illetve minimum kupacot. Mindezt kötegezve, vagy angolul \textit{batch-elve} hajtjuk végre, miszerint a teljes vödröt kisebb tartományokra bontjuk, és egy ciklus minden iterációján megnyitjuk a backend olvasó tranzakcióját. Ezt azért fontos megjegyezni, mivel nagy bérlet vödör esetén túl sokáig tartanánk a backend kulcsát, kötegezéssel adunk egy kis időt a lélegzetvételre, hogy más rutinok is használhassák a backend író tranzakcióját. Miután az összes bérletet elmentettük memóriába, hasonló kötegező módszerrel végigmegyünk a \textt{\/kv} vödrön és a hozzáadjuk a kulcsokhoz a szükséges bérleteket.