# hálózati réteg + biztonság
## 1. http réteg
    1 - k8s által védett TLS
    2 - klaszter gépei között mTLS a HashiCorp Raft könyvtár StreamLayer inteface külön implementációjával (internal/mtls) 
    3 - ratelimiting (k8s + internal/transport)
## telemetria
    1 - Miért fontos? Egy modern elosztott rendszert lehetetlen menedzselni vakon. "Ami nincs mérve, az nem létezik."
    2 - Mit írj róla (Instrukciók):
        Sorold fel a főbb kategóriákat (Backend, KV, Lease, OT, Raft metrikák).
        Hozd fel példaként, hogy milyen kérdésekre adnak választ ezek a metrikák: "Mennyi a Raft commit latency?", "Hány aktív bérlet van éppen?", "Mekkora a memóriaindex mérete?". (Ha van a Kávéhoz egy szép Grafana dashboardod, egy képernyőképet mindenképp érdemes betenni a dokumentációba!).