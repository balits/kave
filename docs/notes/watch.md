# 1. Polling és streaming: a Figyelő API motivációja
Amikor egy kliens nyomon szeretné követni egy vagy több kulcs értékét, vagy a rajta végrehajtott módosításokat, két általános módszert lehet alkalmazni. A \textit{polling} használatával a kliens adott időközönként lekérdezi egy vagy több kulcs értékét, és ez a bizonyos időköz csökkentésével a kliens gyorsabban értesül az eseményekről. Ez viszont azt is jelenti, hogy akár másodpercenként, vagy gyorsabban is küldhet lekérdezéseket a szerverhez, túlterhelve nem csak a szervert de a hálózatot is (nem is beszélve a kliens CPU ciklusokat égető végtelen while ciklusáról).

A Figyelő API ezzel szemben a \textit{Push Notification} mechanizmust használja, miszerint a szerver a kulcsok módosíta után egyből elküldi a megfelelő eseményeket a feliratkozott figyelőknek; így az API egy megvalósítása a Termelő-Fogyasztó mintának.
Mivel a szakdolgozat erejéig csak a HTTP szállító réteget implementáluk, ezért a Figyelő API WebSocketen keresztül érhető el: ez nemcsak ipari sztender módja valós idejú üzenetek olvasására, de ugyanazon a WebSocket kapcsolaton keresztül több figyelőt is tudunk létrehozni, vagy bezárni, hiszen a WebSocket maga egy kétirányú protokoll.
A Figyelő API garantálja, hogy helyes sorrendben, minden kulcs-érték esemény meg lesz hírdetve, amely végigment az állapotgépen.

# 2. Figyelők felépítése és életcikla
\subsubection{Figyelő}
A Figyelő API felépítését legjobban az alsóbb absztrakciós rétegről felfelé menet tudjuk rendesen elmagyarázni. A hierarchia legalján léteznek a Figyelők \textt{watch.watcher}. Ezek magukbafoglalják a figyelés paramétereit: a figyelt kulcs vagy kulcs tartományt, kezdeti verziósorszám-számlaló, melytől fogva szeretnénk értesülni az eseményekről és egy opcionális eseményfilter, amivel meg tudjuk adni, hogy milyen eseményről (\texttt{PUT} vagy \texttt{DELETE}) nem szeretne értesítést kapni. Ez a struktúra tartalmaz még egy Go channel példányt is, melyeken keresztül aszinkron módon tudunk eseményeket küldeni a figyelőnek. Fontos megjegyezni, hogy a figyelő csatornája pufferelt: az első $N$ esemény küldése nem blokkol, viszont, ha ennél többet küldenénk és a figyelő nem tudja őket időben feldolgozni, hibát dob, ennek később lesz fontos szerepe.

\begin{verbatim}
ide kéne a watch.watcher struct és a kv.KvEntry struct
\end{verbatim}

\subsubsction{Elosztó és szinkronizálás}
Az események központi "csomópontja" az Elosztó \texttt{watch.WatchHub}. Ez a struktúra iratkozik fel az állapotgépen végigment kulcs-érték módosításokra, és ő küldi ezeket tovább eseményekként az figyelőknek. De mi történik, ha egy figyelő még a 100-zal ezelőtti eseményeket próbálja feldolgozni és rázúdítjuk a következő száz eseményt? A figyelő csatornája nem bírná el a terhelést és blokkolási hibát adna, eldobva az eseményeket, azaz megtörnénk az API garanciáját. Ezen a szinten jelenik meg a figyelők csoportosítása attól függően, hogy egy figyelő naprakész (\texttt{synced}), vagy lemaradt (texttt{unsynced}). Amikor az állapotgép elküldi az új kulcs érték bejegyzéseket, az Elosztó átformálja őket \texttt{kv.KvEvenet} eseményekké és csak a naprakész figyelőket értesíti róluk. Ha egy figyelő értesítése bármilyen hibát dob, mely nem túlterhelési hiba, megválunk tőle. Amennyiben a figyelő túlterhelt, demótáljuk a lemaradt figyelők csoportjába


A lemaradt figyelők kezelését egy háttérfolyamat, az \texttt{watch.UnsyncedLoop} feladata. A háttérfolyamat segítségével izonyos időközönként megpróbáljuk felzárkóztatni a lemaradt figyelőket, attól függően, milyen verziósorszámok kellenek nekik. Amint a figyelő belső verziósorszám-számlálója elérte az \texttt{mvcc.KvStore} globális számlálóját, a figyelő teljesen naprakész és átkerül a \texttt{watch.WatchHub} felügyelete alá. Amennyiben egy figyelőt többszöri alkalommal sem tudunk felzárkóztatni, megválunk tőle.

# 4. Integráció a Raft klaszterbe
Mivel a \texttt{watch.WatchHub} a Raft állapotgépére van rákötve, és a Raft garantálja, hogy idővel minden gépen ugyanazok az állapotátmenetek mennek végbe, a klaszter bármely gépe szolgálhat figyelőket. 

# Ide nagyon illene egy folyamatábra (Flowchart), amely bemutatja egy kliens életútját:
\begin{verbatim}
- Kliens beküldi: Watch(key="foo", revision=10)
- Döntés: A jelenlegi revízió 20. Lemaradt? Igen.
- Irány az UnsyncedLoop: MVCC-ből kiolvasni a 10-20 közötti változásokat.
- Átkapcsolás a valós idejű Hub-ra.
- Új írás érkezik a FSM-be (revision 21), a Hub egyből küldi a kliensnek.
\end{verbatim}