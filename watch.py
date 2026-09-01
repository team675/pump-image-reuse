#!/usr/bin/env python3
"""Veilleur TEMPS REEL, concu pour tourner dans GitHub Actions.

Ecoute le flux des creations pump.fun et, des qu'un token nait avec une image
deja utilisee, pousse le journal sur la branche `data`. La page le lit via
raw.githubusercontent (mesure : contenu frais en ~3 s).

Actions est illimite sur un depot public, mais un job dure 6 h au maximum : le
workflow relance donc un job regulierement et celui-ci s'arrete de lui-meme.
La branche `data` ne contient qu'UN commit, reecrit a chaque fois (--amend +
push --force), pour que l'historique ne gonfle pas.
"""
import asyncio, json, os, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import pump_reuse_watch as P

DUREE = float(os.environ.get("DUREE", 2100))     # 35 min puis le workflow reprend
PUSH_MIN = float(os.environ.get("PUSH_MIN", 15))  # au plus un push toutes les 15 s
BRANCHE = "data"
FICHIER = "data.json"
GARDE = 300
P.API_MIN_GAP = 0.15
P.MAX_PAGES = 2

etat = {"entrees": [], "vus": 0, "trouves": 0, "depart": time.time(),
        "dernier_push": 0.0, "sale": False}


def sh(*args, **kw):
    return subprocess.run(args, capture_output=True, text=True, cwd=HERE, **kw)


def charger_journal():
    """Recupere le journal deja publie sur la branche data, s'il existe."""
    sh("git", "fetch", "--depth=1", "origin", BRANCHE)
    r = sh("git", "show", f"FETCH_HEAD:{FICHIER}")
    if r.returncode == 0:
        try:
            etat["entrees"] = (json.loads(r.stdout) or {}).get("entrees", [])[:GARDE]
        except Exception:
            pass
    print(f"journal repris : {len(etat['entrees'])} entrees", flush=True)


def publier(force=False):
    if not etat["sale"]:
        return
    if not force and time.time() - etat["dernier_push"] < PUSH_MIN:
        return
    charge = {"maj": time.time(), "mode": "temps_reel", "scannes": etat["vus"],
              "trouves": etat["trouves"], "entrees": etat["entrees"][:GARDE]}
    chemin = os.path.join(HERE, FICHIER)
    json.dump(charge, open(chemin, "w"))
    sh("git", "add", "-f", FICHIER)
    # un seul commit sur la branche, reecrit : l'historique reste minuscule
    sh("git", "commit", "-q", "--allow-empty", "-m", "journal temps reel")
    r = sh("git", "push", "--force", "origin", f"HEAD:{BRANCHE}")
    if r.returncode == 0:
        etat["dernier_push"] = time.time()
        etat["sale"] = False
    else:
        print(f"  push en echec : {r.stderr.strip()[:200]}", flush=True)


async def traiter(m, sem):
    async with sem:
        try:
            r = await asyncio.to_thread(
                P.report, m["mint"], m.get("symbol") or m.get("name"),
                None, m.get("traderPublicKey"), m.get("uri"))
        except Exception:
            return
        etat["vus"] += 1
        if not r.get("n_same_image"):
            return
        etat["trouves"] += 1
        naissance = time.time()
        src = r.get("img_twin_cree") or 0
        etat["entrees"].insert(0, {
            "mint": r["mint"], "symbol": r["symbol"], "cree": naissance,
            "image": r.get("image_uri"),
            "src_mint": r.get("img_twin"), "src_symbol": r.get("img_twin_symbol"),
            "src_cree": src, "src_ath": r.get("img_twin_ath") or 0,
            "src_image": r.get("img_twin_image"),
            "jours": round((naissance - src) / 86400, 1) if src else 0,
            "dist": r.get("img_dist"), "n_homonymes": r.get("n_twins"),
            "lanceur": r.get("launcher"), "vu": naissance,
        })
        del etat["entrees"][GARDE:]
        etat["sale"] = True
        print(f"  {r['symbol']} copie {r.get('img_twin_symbol')} "
              f"(il y a {etat['entrees'][0]['jours']}j) d={r.get('img_dist')}", flush=True)


async def principal():
    import websockets
    charger_journal()
    sem = asyncio.Semaphore(12)
    fin = time.time() + DUREE
    print(f"veille temps reel pour {DUREE / 60:.0f} min", flush=True)
    async for ws in websockets.connect(P.WSURL, ping_interval=20, open_timeout=30):
        try:
            await ws.send(json.dumps({"method": "subscribeNewToken"}))
            while time.time() < fin:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=20)
                except asyncio.TimeoutError:
                    publier()
                    continue
                m = json.loads(raw)
                if m.get("txType") == "create" and m.get("mint"):
                    asyncio.create_task(traiter(m, sem))
                publier()
            break
        except Exception as e:
            print(f"  ~ reconnexion ({e})", flush=True)
            publier()
            if time.time() >= fin:
                break
    await asyncio.sleep(3)      # laisser finir les taches en vol
    etat["sale"] = True
    publier(force=True)
    print(f"fin : {etat['vus']} tokens vus, {etat['trouves']} images reutilisees, "
          f"journal={len(etat['entrees'])}", flush=True)


if __name__ == "__main__":
    asyncio.run(principal())
