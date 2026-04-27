# 1. MVCC elméleti alapja
    A Kávé rendszer egy globális, 64 bites monoton növekvő egész számot használ, amely az adatbázis egy adott állapotát azonosít, melyet verziósorszámnak, vagy röviden csak sorszámnak hívunk az angol \textit{revision} szóbol és ez a globalás sorszám teszi lehetővé a tökéletes pillanatkép készítést. Mivel az adatbázist érintő minden írási művelet a Raft állapotgépén halad át, mely maga is tárol több monoton növekvő számlálót, illetve a Raft garanciái mellett minden sikeres írás elmentődik a naplóba egy véglegsítő indexszel (angolul \textit{commit index}). Első ránézésre kézenfekvő lenne ezt a commit indexszet használni, mint az adatbázis verziósorszáma, viszont a Raft a commit indexet használja más belső, nem felhasználó definiált parancsokt végrehajtások után is, mint például a klaszter tagjainak fríssítése után, így egy külön számlálót használunk, mely az \texttt{internal/mvcc} csomag beli \texttt{mvcc.KvStore} struktúrában található: ezt a változót csak az sikeres állapotátmenetek növelik.
    Ez a sorszámozás teszi lehetővé az időutazó olvasásokat, miszerint egy kliens könnyedén lekérdezheti egy kulcs múltbéli értéket a megfelelő sorszámmal, mindaddig, amíg a tömörítő ki nem törli azokat (lásd később \ref{?}). Emellett minden API lekérdezés és adatbázis művelet úgy hajtódik végre, mintha egy atomi tranzakció része lenne: vagy teljesen, egy új sorszámot hozva létre, vagy egyáltalán nem, így az adatbázis sohasem kerülhet inkonzisztens állapotba, a verziósorszám mindig konzisztens marad.

# 2. Fizikai és logikai tár

    1 - BoltDB
    kulcs értek bájtsorozatok. itt egy kulcs nem felhasználói kulcs, hanem az adattár verziósorszáma.
        - KvEntry + BoltDB ACID tranzakciók
        - négy vödöt + bigEndian(verziósorszám), hogy gyors legyen tartomány lekérdezés, hiszen a sorszám bájtsorozota rendezve lesz (?)

    A Go nyelv legismertebb beágyazott kulcs-érték könyvtára kétség kívül a BoltDB, ami egy egyszerűségre törekvő tartós adattár. A teljes adatbázis egy fájl- ban található, minden művelet ACID tranzakció alapú, és különböző adatok elkülönítésére külön vödröket lehet létrehozni illetve a könyvtár kizárólag bájtsorozat (byte slice) típusú kulcs érték párokon dolgozik, azaz ezen az alacsony szinten az adatoknak semmilyen szemantikus jelentése nincs.
    Ezen a Kávé program legalacsonyabb szintje egy teljesen átlátszatlan bájtsor tár: itt a BoltDB kulcsok nem felhasználói kulcsok, hanem az adattár verziósorszámai. Mivel minden, adatbázist módosító írási művelethez egyedi, monotonikusan növekvő verziószám tartozik, így a BoltDB kulcsok értékei maguk a kulcs-érték bejegyzések (\texttt{types.KvEntry}), melyek a módósítások végeredményei.
    \begin{verbatim}
    // KvEntry a valódi tipus amit az adatbázisban tárolunk
    type KvEntry struct {
        Key       []byte `json:"key"` // felhasználói kulcs
        Value     []byte `json:"value"` // felhasználói érték
        CreateRev int64  `json:"create_revision"` // létrehozás verziósorszáma
        ModRev    int64  `json:"mod_revision"` // legutóbbi módosítás verziósorszáma
        Version   int64  `json:"version"` // a kulcson való módosítások számlálója
        LeaseID   int64  `json:"lease_id,omitempty"` // a kulcshoz csatolt bérlet azonosítója, ha nincs csatolt bérlet, akkor 0
    }
    \end{verbatim}


    2 - logikai index:
        - fa, kulcs, generációk...
        - generációk miértje...
        - a B-fa nem az adatot tárolja, csak egy referenciát a fizikai tárba a verziósorszám alapján
    Ha csak a BoltDB-t használnánk, egy kulcs összes eddigi értékét megvizsgálni - amit tartomány vagy \textit{range} lekérdezésnek hívunk - nagyon lassú és költséges lenne: végig kellene menni az adatbázis összes $N$ kulcsán (verziósorszám), azokat dekódolnánk és kinyernénk a hozzátartozó értékeket (bejegyzések), összehasonlítánk a kulcsokat, illetve a legutóbbi módosítás sorszámát. Ezt az $(O(N))$ komplexitású lekérdezést egy memóriában tárolt logikai indexszel orvosoljuk, melyben minden kulcshoz eltároljuk a hozzá tarozó verziósorszámokat. Így egy optimális \textit{range} műveletet már triviálisan végre tudunk hajtani: az indexből kiolvassuk a kulcshoz tartozó $R$ darab sorszámot, a sorszámokat BoltDB kulcsként kódolva $R$ célzott, $O(1)$ komplexitású olvasást hajtunk végre.
    
    A logikai index egy B-fa, mely egy önkiegyensúlyozó, a bináris keresőfa egy általánosítása \cite{btree}. A fa elemei nem tárolnak felhasználói adatot, csak referenciáka a fizikai tárba. A fa elemeiként egy külön struktúrát, a kulcs indexet használjuk, mely az kulcshoz tartozó sorszámokat tárolja. A kulcs index magába foglalja az adott kulcsot, a legutolsó módósítási sorszámot, és az összes a kulcs számára releváns sorszámot generációkra bontva. Miért tárolunk generációkat? Egy kulcs kitörlése után az eddigi bejegyzései még mindig elérhetők a fizikai tárban, viszont ha újra beillesztenénk egy értéket ugyanazzal a kulcssal, honnan tudnánk megmondani?????. Egy generáció elmenti a verziószámát, a létrehozási sorszámot és az generáció összes sorszámát. Miért generációkra?  Ezt a problémát oldja meg a generáció lista: minden kulcs roszámát egy generáció listában tároljuk, amikor pedig kitörölnénk egy kulcsot, egy üres generációt adunk hozzá a kulcs indexhez. Ha újra szeretnénk éleszteni a kulcsot egy új beszúrással, egyszerűen az új üres generációba kezdjük írni a sorzámokat a következő kulcs törlésig, amikor újra egy üres generációt szúrúnk be.

    Mind a fizikai tár, mind a logikai index pufferelni az írási műveleteket (\textit{batching}). Ez alapvető építőelemei a tranzakcióknak, a pufferen végzett műveletek nem kerülnek ki a valódi adattárra, ha a tranzakció bármilyen lépése hibát okozna, eldobjuk puffert a módosításokkal együtt. Ha nem történt hiba, az eddig összegyült írásokat egyszerre, atomikusan hajtjuk végre, így redukáljuk a nagyon lassú \texttt{fsync} rendszerhívásokat, mely gyakori meghívása meglehetősén csökkentené az alkalmazás teljesítményét.

# 3. adatírás flowchart?
    1 - Put:
        1. Parancs javaslata az FSM-be
        2. KvStore új verziósorszámot hoz létre 
        3. KvEntry mentése a BoltDB backendbe 
        4. logikai index frissítése
    2 - Read:
        1. Ez nem egy Raft Parancs, így bármelyik gép végre tudja hajtani
        2. logikai indexből kiolvassuk az kért verziósorszámot
        3. BoltDB backendből kiolvasni a KvEntryt

# 4. Konzisztencia
    1 - fő és alsorzsám

# 5. Tömörítés
    ...már megírt rész
    ...talán több fókusz a keyIndex, treeIndex, KvStore compact() függvényeire?

# 6. def
..? Kávéban a tömörítés egy logikai és fizikai megsemmisítési folyamat, amely garantálja, hogy a rendszer ne tároljon elavult revíziókat a megadott küszöbértéken túl.